"""
SQL Generator Agent for text-to-SQL tree orchestration.

This agent generates SQL queries based on query node intents and their
linked schema information.
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_content_types import (
    QueryNode, NodeStatus, ExecutionResult
)
from utils import extract_sql_from_text, parse_xml_hybrid, clean_sql_content
from prompts import SQL_CONSTRAINTS
from autogen_core.tools import FunctionTool
from sql_generator_tools import create_sql_generator_tools


class SQLGeneratorAgent(BaseMemoryAgent):
    """
    Generates SQL queries for query nodes.
    
    This agent:
    1. Takes a query node with intent and schema mapping
    2. Generates appropriate SQL based on the requirements
    3. Handles different query types (simple, joins, aggregations)
    4. Considers parent-child relationships for subqueries
    """
    
    agent_name = "sql_generator"
    
    def _initialize_managers(self):
        """Initialize the managers needed for SQL generation"""
        self.schema_manager = DatabaseSchemaManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
    
    def _build_system_message(self) -> str:
        """Build the system message for SQL generation"""
        from prompts.prompt_loader import PromptLoader
        loader = PromptLoader()
        return loader.get_prompt("sql_generator", version="v1.2")
    
    def _create_agent(self):
        """Override to create the AutoGen AssistantAgent with SQL tools"""
        # Get SQL generator tools
        tool_configs = create_sql_generator_tools(self.memory, self.logger)
        tools = [FunctionTool(
            func=config["function"],
            description=config["description"],
            name=config["name"]
        ) for config in tool_configs]
        
        # Import here to avoid circular imports
        from autogen_agentchat.agents import AssistantAgent
        
        self.assistant = AssistantAgent(
            name=self.agent_name,
            system_message=self._build_system_message(),
            model_client=self.model_client,
            tools=tools
        )
        self.logger.debug(f"Created AssistantAgent: {self.agent_name} with {len(tools)} tools")
        self.logger.info(f"SQL Generator tools: {[t.name for t in tools]}")
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from current node and parent if needed - NODE-FOCUSED VERSION"""
        # 1. OPERATE ON CURRENT NODE - Get current node information
        current_node_id = await self.tree_manager.get_current_node_id()
        if not current_node_id:
            self.logger.error("No current node - SQLGenerator requires a current node")
            return {"error": "No current node available"}
        
        current_node = await self.tree_manager.get_node(current_node_id)
        if not current_node:
            self.logger.error(f"Current node {current_node_id} not found")
            return {"error": f"Current node {current_node_id} not found"}
        
        # 2. READ PAST EXECUTION INFORMATION from current node
        previous_sql_generation = None
        if hasattr(current_node, 'generation') and current_node.generation and current_node.generation.get("sql"):
            previous_sql_generation = current_node.generation
            self.logger.info("Found previous SQL generation in current node - this is a regeneration")
        
        evaluation_feedback = None
        if hasattr(current_node, 'evaluation') and current_node.evaluation:
            evaluation_feedback = current_node.evaluation
            self.logger.info("Found evaluation feedback in current node")
        
        # 3. GET QUERY AND EVIDENCE - from current node first, then parent
        query = current_node.intent if current_node.intent else None
        evidence = None
        
        if hasattr(current_node, 'evidence') and current_node.evidence:
            evidence = current_node.evidence
        
        # If missing query/evidence, check parent node
        parent_node = None
        if (not query or not evidence) and current_node.parentId:
            parent_node = await self.tree_manager.get_node(current_node.parentId)
            if parent_node:
                if not query and parent_node.intent:
                    query = parent_node.intent
                    self.logger.info("Got query from parent node")
                if not evidence and hasattr(parent_node, 'evidence') and parent_node.evidence:
                    evidence = parent_node.evidence
                    self.logger.info("Got evidence from parent node")
        
        # 4. GET SCHEMA LINKING RESULTS - CRITICAL for SQL generation
        schema_linking_context = None
        if hasattr(current_node, 'schema_linking') and current_node.schema_linking:
            schema_linking_context = current_node.schema_linking
            self.logger.info("Found schema linking in current node")
        elif parent_node and hasattr(parent_node, 'schema_linking') and parent_node.schema_linking:
            schema_linking_context = parent_node.schema_linking
            self.logger.info("Found schema linking in parent node")
        else:
            self.logger.warning("No schema linking found - SQLGenerator needs schema information")
        
        # 5. GET QUERY ANALYSIS INFORMATION - from current node or parent
        query_analysis_context = None
        if hasattr(current_node, 'queryAnalysis') and current_node.queryAnalysis:
            query_analysis_context = current_node.queryAnalysis
            self.logger.info("Found query analysis in current node")
        elif parent_node and hasattr(parent_node, 'queryAnalysis') and parent_node.queryAnalysis:
            query_analysis_context = parent_node.queryAnalysis
            self.logger.info("Found query analysis in parent node")
        
        # 6. GET CHILDREN INFORMATION if any (for parent nodes that need to combine results)
        children_info = []
        children = await self.tree_manager.get_children(current_node_id)
        if children:
            for child in children:
                child_dict = child.to_dict()
                children_info.append({
                    "nodeId": child_dict["nodeId"],
                    "intent": child_dict["intent"],
                    "status": child_dict["status"],
                    "sql": child_dict.get("sql"),
                    "result": child_dict.get("result")
                })
        
        # 7. INCREMENT GENERATION ATTEMPT COUNTER
        current_node.generation_attempts += 1
        await self.tree_manager.update_node(current_node.nodeId, {"generation_attempts": current_node.generation_attempts})
        self.logger.info(f"SQL generation attempt #{current_node.generation_attempts} for node {current_node_id}")
        
        # 8. GET DATABASE-SPECIFIC PATTERNS AND GUIDANCE
        database_name = await self._get_database_name()
        success_patterns = await self._get_success_patterns(database_name)
        failure_avoidance = await self._get_failure_avoidance_patterns(database_name)
        strategic_guidance = await self._get_strategic_guidance(current_node_id)
        
        # Build context
        context = {
            "current_node": json.dumps(current_node.to_dict(), indent=2),
            "database_name": database_name
        }
        
        # Add required information
        if query:
            context["query"] = query
        if evidence:
            context["evidence"] = evidence
        
        # Add past execution information
        if previous_sql_generation:
            context["previous_sql_generation"] = json.dumps(previous_sql_generation, indent=2)
        if evaluation_feedback:
            context["evaluation_feedback"] = json.dumps(evaluation_feedback, indent=2)
        
        # Add information from other agents
        if schema_linking_context:
            context["schema_linking_results"] = json.dumps(schema_linking_context, indent=2)
        if query_analysis_context:
            context["query_analysis_results"] = json.dumps(query_analysis_context, indent=2)
        
        # Add children information if available
        if children_info:
            context["children_nodes"] = json.dumps(children_info, indent=2)
        
        # Add learning and guidance
        if strategic_guidance:
            context["strategic_guidance"] = strategic_guidance
        if success_patterns:
            context["success_patterns"] = success_patterns
        if failure_avoidance:
            context["failure_avoidance"] = failure_avoidance
        
        # Get node operation history
        history = await self.history_manager.get_node_operations(current_node_id)
        if history:
            context["node_history"] = json.dumps([op.to_dict() for op in history], indent=2)
        
        self.logger.info(f"SQL generator operating on node: {current_node_id}")
        self.logger.info(f"Query: {query}")
        self.logger.info(f"Has schema linking: {schema_linking_context is not None}")
        self.logger.info(f"Generation attempt: #{current_node.generation_attempts}")
        
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the SQL generation results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
        
        # Find the last assistant message that contains final XML output (not tool calls)
        last_assistant_message = None
        has_tool_calls = False
        
        for message in reversed(result.messages):
            # Check if this message contains tool calls
            if hasattr(message, 'content') and isinstance(message.content, list):
                has_tool_calls = True
                continue
                
            # Check for assistant messages with string content (potential final response)
            is_assistant = False
            if hasattr(message, 'source') and message.source == self.agent_name:
                is_assistant = True
            elif hasattr(message, 'role') and message.role == 'assistant':
                is_assistant = True
            elif not hasattr(message, 'source') and isinstance(message.content, str):
                is_assistant = True
                
            if is_assistant and isinstance(message.content, str):
                # Check if this looks like a final response (contains XML)
                if '<sql_generation>' in message.content or 'SELECT' in message.content.upper():
                    last_assistant_message = message.content
                    break
                
        # If we found tool calls but no final response, this is an incomplete conversation
        if has_tool_calls and not last_assistant_message:
            self.logger.info("Conversation still in progress - tool calls detected but no final response yet")
            return
                
        if not last_assistant_message:
            self.logger.warning("No final assistant message found in result")
            return
            
        # Log the raw output for debugging
        self.logger.info(f"Raw LLM output: {last_assistant_message}")
        self.logger.info(f"Found final response in conversation with {len(result.messages)} total messages")
        
        try:
            # Parse the XML output
            generation_result = self._parse_generation_xml(last_assistant_message)
            
            if generation_result and generation_result.get("sql"):
                sql = generation_result["sql"]
                explanation = generation_result.get("explanation", "")
                considerations = generation_result.get("considerations", "")
                
                # Validate required fields
                if not sql or sql.strip() == "":
                    self.logger.error("Generated SQL is empty or invalid")
                    return
                
                # Basic SQL validation
                if not self._validate_sql_basic(sql):
                    self.logger.warning(f"Generated SQL may be invalid: {sql}")
                    # Continue anyway - let evaluator catch issues
                
                # Get current node ID from QueryTreeManager
                node_id = await self.tree_manager.get_current_node_id()
                
                if node_id:
                    # Store the generation result in the QueryTree node
                    await self.tree_manager.update_node(node_id, {"generation": generation_result})
                    
                    # Update the node with SQL 
                    await self.tree_manager.update_node_sql(node_id, sql)
                    
                    # CRITICAL: Perform node-specific validation (Fix for Examples 1, 3, 15, 23)
                    if generation_result.get('needs_validation'):
                        await self._perform_node_validation(node_id, sql, generation_result)
                    
                    # Store explanation and considerations in the node
                    await self.tree_manager.update_node_sql_context(
                        node_id=node_id,
                        explanation=explanation,
                        considerations=considerations,
                        query_type=generation_result.get("query_type", "simple")
                    )
                    
                    # Get node to check attempt count
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        # Log attempt count for tracking
                        self.logger.info(f"SQL generation completed for attempt #{node.generation_attempts} for node {node_id}")
                        
                        if node.generation_attempts >= 3:
                            self.logger.warning(f"Node {node_id} has reached maximum attempts ({node.generation_attempts}). Will be marked complete by TaskStatusChecker.")
                    
                    # Basic logging
                    self.logger.info("="*60)
                    self.logger.info("SQL Generation")
                    self.logger.info(f"Generated SQL:")
                    sql_lines = sql.split('\n') if '\n' in sql else [sql]
                    for line in sql_lines:
                        if line.strip():
                            self.logger.info(f"  {line}")
                    
                    if explanation:
                        self.logger.info(f"Explanation: {explanation}")
                    
                    if considerations:
                        self.logger.info(f"Considerations: {considerations}")
                    
                    self.logger.info("="*60)
                    self.logger.info(f"Updated node {node_id} with generated SQL")
                else:
                    self.logger.warning("No node_id found to update with generated SQL")
                
        except Exception as e:
            self.logger.error(f"Error parsing SQL generation results: {str(e)}", exc_info=True)
    
    
    def _parse_generation_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the SQL generation XML output - v1.2 format only"""
        try:
            # Parse v1.2 format with sql_generation tag
            result = parse_xml_hybrid(output, 'sql_generation')
            if result:
                converted = self._convert_v12_format(result)
                if converted:
                    return converted
            
            # Last resort: manual extraction
            return self._extract_sql_fallback(output)
            
        except Exception as e:
            self.logger.error(f"Error parsing SQL generation XML: {str(e)}", exc_info=True)
            # Try fallback extraction
            return self._extract_sql_fallback(output)
    
    def _convert_v12_format(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert v1.2 format to internal format"""
        try:
            # Extract from final_selection section
            final_selection = result.get('final_selection')
            if not final_selection:
                return None
                
            converted = {}
            
            # Extract SQL from final_selection
            if isinstance(final_selection, dict):
                converted['sql'] = final_selection.get('final_sql', '')
                converted['explanation'] = final_selection.get('selection_reason', '')
                
                # Get query type from strategy_planning
                strategy_planning = result.get('strategy_planning', {})
                if isinstance(strategy_planning, dict):
                    converted['query_type'] = strategy_planning.get('complexity_level', 'simple')
                else:
                    converted['query_type'] = 'simple'
                
                # Get considerations from context_analysis
                context_analysis = result.get('context_analysis', {})
                if isinstance(context_analysis, dict):
                    query_intent = context_analysis.get('query_intent', '')
                    converted['considerations'] = f"v1.2 format - Context: {query_intent}"
                else:
                    converted['considerations'] = 'Generated using v1.2 format'
            
            # Clean up SQL if needed
            if converted.get('sql'):
                converted['sql'] = clean_sql_content(converted['sql'])
                
                # CRITICAL: Validate SQL syntax (Fix for backtick/quote issues)
                syntax_valid, syntax_msg = self._validate_sql_syntax(converted['sql'])
                if not syntax_valid:
                    self.logger.error(f"SQL syntax validation failed: {syntax_msg}")
                    converted['syntax_validation_error'] = syntax_msg
                else:
                    self.logger.info("✓ SQL syntax validation passed")
                
                # CRITICAL: Validate single statement (Fix for Example 83)
                is_valid, validation_msg = self._validate_single_statement(converted['sql'])
                if not is_valid:
                    self.logger.error(f"Single statement validation failed: {validation_msg}")
                    converted['validation_error'] = validation_msg
                    # Don't fail completely, but mark for review
                else:
                    self.logger.info("✓ Single statement validation passed")
                
                # CRITICAL: Validate basic SQL properties (Fix for Examples 1, 3, 15, 23)
                # Note: Node-specific validation happens after parsing in the main parser callback
                converted['needs_validation'] = True
                
                # Mark for NULL filtering check if SQL contains joins or conditions
                if 'JOIN' in converted['sql'].upper() or 'WHERE' in converted['sql'].upper():
                    converted['needs_null_check'] = True
                
                # Mark for simplification check if SQL is complex
                if converted['sql'].count('SELECT') > 1 or 'SUBQUERY' in converted['sql'].upper():
                    converted['needs_simplification_check'] = True
            
            # Extract execution results if present
            execution_results = result.get('execution_results', {})
            if isinstance(execution_results, dict):
                if execution_results.get('execution_status') == 'success':
                    converted['execution_result'] = {
                        'status': 'success',
                        'row_count': execution_results.get('row_count', 0),
                        'columns': [],  # Not captured in XML, would need tool response
                        'data': []      # Not captured in XML, would need tool response
                    }
                elif execution_results.get('execution_status') == 'error':
                    converted['execution_result'] = {
                        'status': 'error',
                        'error': execution_results.get('corrections_applied', 'Execution failed'),
                        'row_count': 0,
                        'columns': [],
                        'data': []
                    }
            
            return converted
            
        except Exception as e:
            self.logger.warning(f"Error converting v1.2 format: {str(e)}")
            return None
    
    
    def _extract_sql_from_string(self, selection_str: str) -> str:
        """Extract SQL from string content using multiple patterns"""
        try:
            import re
            
            # Try multiple SQL extraction patterns
            patterns = [
                r'SELECT.*?(?=\n\s*$|\n\s*\]\]>|$)',  # Original pattern
                r'SELECT.*?(?:;|$)',  # SQL ending with semicolon or end
                r'WITH.*?(?:;|$)',     # CTE queries
                r'(?:SELECT|WITH).*',  # Any SQL starting with SELECT or WITH
            ]
            
            for pattern in patterns:
                sql_match = re.search(pattern, selection_str, re.DOTALL | re.IGNORECASE)
                if sql_match:
                    sql = sql_match.group(0).strip()
                    if sql and len(sql.strip()) > 5:  # Basic sanity check
                        return sql
            
            # If no pattern matches, return the string as-is (might be valid SQL)
            return selection_str.strip()
            
        except Exception as e:
            self.logger.warning(f"Error extracting SQL from string: {str(e)}")
            return selection_str
    
    def _extract_sql_fallback(self, output: str) -> Optional[Dict[str, Any]]:
        """Fallback SQL extraction when XML parsing fails completely"""
        try:
            sql = extract_sql_from_text(output)
            if sql and sql.strip():
                cleaned_sql = clean_sql_content(sql)
                
                # CRITICAL: Validate single statement (Fix for Example 83)
                is_valid, validation_msg = self._validate_single_statement(cleaned_sql)
                result = {
                    "query_type": "unknown",
                    "sql": cleaned_sql,
                    "explanation": "Extracted from response using fallback method",
                    "considerations": "XML parsing failed, used regex extraction"
                }
                
                if not is_valid:
                    self.logger.error(f"Single statement validation failed in fallback: {validation_msg}")
                    result['validation_error'] = validation_msg
                else:
                    self.logger.info("✓ Single statement validation passed in fallback")
                
                return result
            
            self.logger.warning("No SQL found in output using fallback extraction")
            return None
            
        except Exception as e:
            self.logger.error(f"Error in fallback SQL extraction: {str(e)}")
            return None
    
    def _validate_sql_basic(self, sql: str) -> bool:
        """Basic SQL validation to catch obvious issues"""
        try:
            sql = sql.strip()
            
            # Must contain SELECT (basic requirement)
            if not any(keyword in sql.upper() for keyword in ['SELECT', 'WITH']):
                return False
            
            # Basic length check
            if len(sql) < 10:
                return False
            
            # Check for basic SQL structure
            if 'SELECT' in sql.upper() and 'FROM' not in sql.upper():
                # SELECT without FROM is suspicious unless it's a simple calculation
                if not any(func in sql.upper() for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                    return False
            
            return True
            
        except Exception as e:
            self.logger.warning(f"Error in basic SQL validation: {str(e)}")
            return True  # Default to valid if validation fails
    
    def _validate_single_statement(self, sql: str) -> Tuple[bool, str]:
        """Validate that SQL contains only one statement (CRITICAL FIX for Example 83)"""
        try:
            sql = sql.strip()
            
            # Remove comments first to avoid false positives
            sql_no_comments = re.sub(r'--.*?(?=\n|$)', '', sql, flags=re.MULTILINE)
            sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_no_comments, flags=re.DOTALL)
            
            # Count main SELECT statements (not in subqueries)
            # This is a simplified check - more sophisticated parsing could be added
            
            # Split by semicolons and check each part
            statements = [stmt.strip() for stmt in sql_no_comments.split(';') if stmt.strip()]
            
            if len(statements) > 1:
                # Multiple statements detected
                return False, f"Multiple SQL statements detected ({len(statements)} statements). Only single statements allowed."
            
            # Count SELECT keywords at the root level (not in subqueries)
            # This is a simple heuristic - could be improved with proper SQL parsing
            root_selects = 0
            paren_depth = 0
            tokens = re.findall(r'\w+|\(|\)', sql_no_comments.upper())
            
            for token in tokens:
                if token == '(':
                    paren_depth += 1
                elif token == ')':
                    paren_depth -= 1
                elif token == 'SELECT' and paren_depth == 0:
                    root_selects += 1
            
            if root_selects > 1:
                return False, f"Multiple root-level SELECT statements detected ({root_selects}). Use subqueries or CTEs instead."
            
            # Check for common multi-statement patterns
            multi_statement_patterns = [
                r';\s*SELECT',  # Semicolon followed by SELECT
                r'SELECT.*?;\s*SELECT',  # Two SELECTs separated by semicolon
            ]
            
            for pattern in multi_statement_patterns:
                if re.search(pattern, sql_no_comments, re.IGNORECASE | re.DOTALL):
                    return False, f"Multi-statement pattern detected. Use single query with CTEs or subqueries."
            
            return True, "Single statement validation passed"
            
        except Exception as e:
            self.logger.error(f"Error in single statement validation: {str(e)}", exc_info=True)
            return True, "Validation error - defaulting to pass"
    
    def _validate_sql_syntax(self, sql: str) -> Tuple[bool, str]:
        """Validate SQL syntax for common errors (CRITICAL FIX for quote/backtick issues)"""
        try:
            # Check for backtick misuse in string literals
            backtick_literal_pattern = r'=\s*`([^`]+)`(?!\s*\.)'  # backticks around values not followed by dot
            if re.search(backtick_literal_pattern, sql):
                return False, "Backticks (`) used for string literals. Use single quotes (') instead."
            
            # Check for SQLite incompatible functions
            incompatible_functions = [
                (r'EXTRACT\s*\(', "EXTRACT function not supported in SQLite. Use strftime() instead."),
                (r'DATEDIFF\s*\(', "DATEDIFF function not supported in SQLite. Use date arithmetic instead."),
                (r'SUBSTRING\s*\(', "SUBSTRING function not standard in SQLite. Use substr() instead.")
            ]
            
            for pattern, message in incompatible_functions:
                if re.search(pattern, sql, re.IGNORECASE):
                    return False, message
            
            # Check for proper quote usage patterns
            quote_patterns = [
                (r"=\s*'[^']*'", "✓ Correct single quote usage for string literals"),
                (r'"\w+[^"]*"', "✓ Correct double quote usage for identifiers")
            ]
            
            return True, "SQL syntax validation passed"
            
        except Exception as e:
            self.logger.error(f"Error in SQL syntax validation: {str(e)}", exc_info=True)
            return True, "Validation error - defaulting to pass"
    
    async def _validate_column_selection(self, sql: str, query_intent: str) -> Tuple[bool, str]:
        """Validate column selection matches query intent (CRITICAL FIX for Examples 1, 3, 15, 23)"""
        try:
            # Extract SELECT clause
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if not select_match:
                return True, "No SELECT clause found to validate"
            
            select_clause = select_match.group(1).strip()
            
            # Count selected columns (simple heuristic)
            # Remove function calls first to avoid false counting
            clean_select = re.sub(r'\w+\([^)]*\)', 'FUNCTION', select_clause)
            
            # Count commas + 1 for column count (rough estimate)
            column_count = len([x for x in clean_select.split(',') if x.strip()]) if ',' in clean_select else 1
            
            # Analyze query intent for expected output
            intent_lower = query_intent.lower()
            
            # Single value expectations
            single_value_patterns = [
                r'what is the\s+(\w+)',  # "what is the phone number"
                r'list the\s+(\w+)',     # "list the rates" 
                r'how many',             # count queries
                r'which\s+(\w+)',        # "which school"
                r'give.*their\s+(\w+)',  # "give their NCES number"
            ]
            
            # Multiple column expectations  
            multi_column_patterns = [
                r'names? and.*address',   # name and address
                r'full address',          # full address (multiple fields)
                r'complete.*address',     # complete address
            ]
            
            # Check for single value expectations
            for pattern in single_value_patterns:
                if re.search(pattern, intent_lower):
                    if column_count > 1:
                        return False, f"Query asks for single value but SQL selects {column_count} columns. Intent: '{query_intent[:100]}...'"
            
            # Check for specific single field requests
            specific_field_patterns = [
                r'(?:mailing )?street address',  # only street, not full address
                r'phone number',                 # only phone
                r'zip code',                     # only zip
                r'(\w+) rate',                   # only the rate, not components
            ]
            
            for pattern in specific_field_patterns:
                if re.search(pattern, intent_lower):
                    if column_count > 1:
                        return False, f"Query asks for specific field '{pattern}' but SQL selects {column_count} columns"
            
            # Check for calculation-only requests
            if any(word in intent_lower for word in ['rate', 'percentage', 'average']) and 'name' not in intent_lower and 'school' not in intent_lower:
                # If asking for calculation only, shouldn't include entity names
                if any(word in select_clause.lower() for word in ['name', 'school']):
                    return False, "Query asks for calculation only but SQL includes entity names"
            
            return True, f"Column selection validation passed ({column_count} columns selected)"
            
        except Exception as e:
            self.logger.error(f"Error in column selection validation: {str(e)}", exc_info=True)
            return True, "Validation error - defaulting to pass"
    
    def _detect_null_filtering_needed(self, sql: str, query_intent: str) -> Tuple[bool, str]:
        """Detect if NULL filtering is needed for entity queries (CRITICAL FIX for Example 22)"""
        try:
            intent_lower = query_intent.lower()
            
            # Patterns that indicate entity queries needing NULL filtering
            entity_query_patterns = [
                r'which\s+(\w+)',         # "which school"
                r'what.*name',            # "what is the name"
                r'list.*names?',          # "list the names"
                r'give.*names?',          # "give me the names"
                r'show.*names?',          # "show the names"
            ]
            
            is_entity_query = any(re.search(pattern, intent_lower) for pattern in entity_query_patterns)
            
            if not is_entity_query:
                return False, "Not an entity query"
            
            # Check if SQL already has appropriate NULL filtering
            sql_upper = sql.upper()
            
            # Look for name-like columns in SELECT
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if not select_match:
                return False, "No SELECT clause found"
            
            select_clause = select_match.group(1)
            
            # Check if selecting name-like fields
            name_fields = ['name', 'sname', 'school']
            has_name_field = any(field in select_clause.lower() for field in name_fields)
            
            if not has_name_field:
                return False, "No name fields in SELECT"
            
            # Check if NULL filtering already exists for name fields
            has_null_filter = any(pattern in sql_upper for pattern in [
                'NAME IS NOT NULL',
                'SNAME IS NOT NULL', 
                'SCHOOL IS NOT NULL'
            ])
            
            if has_null_filter:
                return False, "NULL filtering already present"
            
            # Check for ORDER BY pattern that might need NULL filtering
            if 'ORDER BY' in sql_upper and 'LIMIT' in sql_upper:
                # This is likely a "highest/lowest" query that should filter NULLs
                return True, "Entity query with ORDER BY needs NULL filtering for name fields"
            
            # Check for MAX/MIN subquery patterns
            if re.search(r'=\s*\(\s*SELECT\s+(MAX|MIN)', sql, re.IGNORECASE):
                return True, "Entity query with MAX/MIN subquery needs NULL filtering for name fields"
            
            return False, "NULL filtering not needed"
            
        except Exception as e:
            self.logger.error(f"Error detecting NULL filtering need: {str(e)}", exc_info=True)
            return False, "Detection error"
    
    def _suggest_query_simplification(self, sql: str, query_intent: str) -> Tuple[bool, str]:
        """Suggest query simplification (CRITICAL FIX for Examples 15, 19)"""
        try:
            sql_upper = sql.upper()
            intent_lower = query_intent.lower()
            
            # Detect "highest/lowest" patterns that could use ORDER BY instead of subqueries
            if any(word in intent_lower for word in ['highest', 'lowest', 'maximum', 'minimum', 'most', 'least']):
                
                # Check for complex MAX/MIN subquery patterns
                if re.search(r'=\s*\(\s*SELECT\s+(MAX|MIN)', sql, re.IGNORECASE):
                    return True, "Consider using ORDER BY DESC/ASC LIMIT 1 instead of MAX/MIN subquery for better reliability"
                
                # Check for unnecessary GROUP BY with aggregation
                if 'GROUP BY' in sql_upper and ('ORDER BY' in sql_upper and 'LIMIT 1' in sql_upper):
                    if not any(word in intent_lower for word in ['group', 'each', 'per']):
                        return True, "Consider removing GROUP BY for single highest/lowest value queries"
            
            return False, "No simplification needed"
            
        except Exception as e:
            self.logger.error(f"Error in query simplification detection: {str(e)}", exc_info=True)
            return False, "Detection error"
    
    
    async def _get_schema_xml(self) -> str:
        """Get the database schema in XML format"""
        # This is a simplified version - in production, would format properly
        tables = await self.schema_manager.get_all_tables()
        if not tables:
            return "<schema>No tables found</schema>"
        
        schema_parts = []
        for table_name, table_info in tables.items():
            columns = []
            for col in table_info.get("columns", []):
                columns.append(f"  ({col['name']}, {col.get('description', col['type'])})")
            
            schema_parts.append(f"# Table: {table_name}\n[\n" + "\n".join(columns) + "\n]")
        
        return "\n".join(schema_parts)
    
    async def _get_strategic_guidance(self, current_node_id: str) -> Optional[str]:
        """Get strategic guidance from intelligent pattern repositories"""
        try:
            # Get database name for pattern repository lookup
            task_context = await self.memory.get("task_context")
            database_name = task_context.get("db_name", "unknown") if task_context else "unknown"
            
            # Get node-specific guidance from pattern agents
            node_guidance_key = f"node_{current_node_id}_strategic_guidance"
            node_guidance = await self.memory.get(node_guidance_key)
            
            guidance_parts = []
            
            # Add pattern-based guidance for SQL Generator
            if node_guidance and "sql_generator_guidance" in node_guidance:
                sql_guidance_list = node_guidance["sql_generator_guidance"]
                if sql_guidance_list:
                    guidance_parts.append("=== INTELLIGENT PATTERN-BASED GUIDANCE ===")
                    for guidance_item in sql_guidance_list:
                        # Each guidance_item contains both success patterns and failure avoidance
                        guidance_parts.append(guidance_item)
            
            # Get database-specific success patterns
            success_repo_key = f"success_patterns_{database_name}"
            success_repo = await self.memory.get(success_repo_key)
            if success_repo:
                sql_success_guidance = success_repo.get("agent_guidance", {}).get("sql_generator", [])
                if sql_success_guidance:
                    guidance_parts.append("\n=== DATABASE-SPECIFIC SUCCESS PATTERNS ===")
                    for success_item in sql_success_guidance[-3:]:  # Last 3 success patterns
                        guidance_parts.append(f"✓ PROVEN STRATEGY: {success_item}")
            
            # Get database-specific failure patterns to avoid
            failure_repo_key = f"failure_patterns_{database_name}"
            failure_repo = await self.memory.get(failure_repo_key)
            if failure_repo:
                sql_failure_guidance = failure_repo.get("corrective_guidance", {}).get("sql_generator", {})
                if sql_failure_guidance:
                    guidance_parts.append("\n=== CRITICAL PITFALLS TO AVOID ===")
                    
                    for guidance_type, guidance_list in sql_failure_guidance.items():
                        if guidance_list:
                            guidance_parts.append(f"❌ {guidance_type.replace('_', ' ').title()}:")
                            for guidance_item in guidance_list[-2:]:  # Last 2 guidance items per type
                                guidance_parts.append(f"   - {guidance_item}")
            
            # Add summary stats for context
            if success_repo or failure_repo:
                guidance_parts.append(f"\n=== LEARNING CONTEXT FOR {database_name.upper()} ===")
                if success_repo:
                    total_successes = success_repo.get("total_successes", 0)
                    guidance_parts.append(f"✓ Successful patterns learned from {total_successes} executions")
                if failure_repo:
                    total_failures = failure_repo.get("total_failures", 0)
                    guidance_parts.append(f"❌ Failure patterns learned from {total_failures} failed attempts")
            
            if guidance_parts:
                final_guidance = "\n".join(guidance_parts)
                self.logger.info(f"Pattern-based strategic guidance retrieved for SQL generation on {database_name}: {len(guidance_parts)} sections")
                return final_guidance
            
            self.logger.info(f"No pattern-based guidance available yet for {database_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error retrieving pattern-based strategic guidance: {str(e)}", exc_info=True)
            return None
    
    async def _get_database_name(self) -> str:
        """Get the current database name from task context"""
        try:
            task_context = await self.memory.get("task_context")
            return task_context.get("db_name", "unknown") if task_context else "unknown"
        except Exception as e:
            self.logger.error(f"Error getting database name: {str(e)}")
            return "unknown"
    
    async def _get_success_patterns(self, database_name: str) -> Optional[str]:
        """Get success patterns for the given database"""
        try:
            success_repo_key = f"success_patterns_{database_name}"
            success_repo = await self.memory.get(success_repo_key)
            if success_repo:
                patterns = success_repo.get("patterns", [])
                if patterns:
                    return "\n".join([f"✓ {pattern}" for pattern in patterns[-3:]])  # Last 3 patterns
            return None
        except Exception as e:
            self.logger.error(f"Error getting success patterns: {str(e)}")
            return None
    
    async def _get_failure_avoidance_patterns(self, database_name: str) -> Optional[str]:
        """Get failure avoidance patterns for the given database"""
        try:
            failure_repo_key = f"failure_patterns_{database_name}"
            failure_repo = await self.memory.get(failure_repo_key)
            if failure_repo:
                patterns = failure_repo.get("patterns", [])
                if patterns:
                    return "\n".join([f"❌ AVOID: {pattern}" for pattern in patterns[-3:]])  # Last 3 patterns
            return None
        except Exception as e:
            self.logger.error(f"Error getting failure avoidance patterns: {str(e)}")
            return None
    
    async def _perform_node_validation(self, node_id: str, sql: str, generation_result: Dict[str, Any]) -> None:
        """Perform comprehensive validation using node context"""
        try:
            # Get the node to access intent
            node = await self.tree_manager.get_node(node_id)
            if not node or not node.intent:
                self.logger.warning("Cannot validate: no node intent available")
                return
            
            # CRITICAL: Validate column selection (Fix for Examples 1, 3, 15, 23)
            if hasattr(self, '_validate_column_selection'):
                col_valid, col_msg = await self._validate_column_selection(sql, node.intent)
                if not col_valid:
                    self.logger.error(f"Column selection validation failed: {col_msg}")
                    # Update generation result with validation error
                    updated_result = generation_result.copy()
                    updated_result['column_validation_error'] = col_msg
                    await self.tree_manager.update_node(node_id, {"generation": updated_result})
                else:
                    self.logger.info(f"✓ Column selection validation passed: {col_msg}")
            
            # CRITICAL: Check NULL filtering needs (Fix for Example 22)
            if generation_result.get('needs_null_check') and hasattr(self, '_detect_null_filtering_needed'):
                needs_null_filter, null_msg = self._detect_null_filtering_needed(sql, node.intent)
                if needs_null_filter:
                    self.logger.warning(f"NULL filtering recommended: {null_msg}")
                    updated_result = generation_result.copy()
                    updated_result['null_filtering_suggestion'] = null_msg
                    await self.tree_manager.update_node(node_id, {"generation": updated_result})
            
            # CRITICAL: Check query simplification (Fix for Examples 15, 19)
            if generation_result.get('needs_simplification_check') and hasattr(self, '_suggest_query_simplification'):
                needs_simplification, simp_msg = self._suggest_query_simplification(sql, node.intent)
                if needs_simplification:
                    self.logger.warning(f"Query simplification suggested: {simp_msg}")
                    updated_result = generation_result.copy()
                    updated_result['simplification_suggestion'] = simp_msg
                    await self.tree_manager.update_node(node_id, {"generation": updated_result})
                    
        except Exception as e:
            self.logger.warning(f"Error in node validation: {str(e)}")
    
