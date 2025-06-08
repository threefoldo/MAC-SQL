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
        """Read context from memory before generating SQL"""
        # Get current_node_id from QueryTreeManager
        current_node_id = await self.tree_manager.get_current_node_id()
        if not current_node_id:
            return {"error": "No current_node_id found"}
        
        # Get the node from QueryTreeManager
        node = await self.tree_manager.get_node(current_node_id)
        if not node:
            return {"error": f"Node {current_node_id} not found"}
        
        # Convert entire node to dictionary for simple string representation
        node_dict = node.to_dict()
        
        # Get children information if any
        children = await self.tree_manager.get_children(current_node_id)
        children_info = []
        if children:
            for child in children:
                child_dict = child.to_dict()
                # Include only essential fields for children
                children_info.append({
                    "nodeId": child_dict["nodeId"],
                    "intent": child_dict["intent"],
                    "status": child_dict["status"],
                    "sql": child_dict.get("sql"),
                    "result": child_dict.get("result")
                })
        
        # Get node operation history
        history = await self.history_manager.get_node_operations(current_node_id)
        # Convert NodeOperation objects to dictionaries
        history_dicts = [op.to_dict() for op in history] if history else []
        
        # Get database-specific success patterns
        database_name = await self._get_database_name()
        success_patterns = await self._get_success_patterns(database_name)
        failure_avoidance = await self._get_failure_avoidance_patterns(database_name)
        
        # Get evidence from the node or root node
        evidence = node_dict.get("evidence")
        if not evidence and node.parentId:
            # Try to get evidence from root node if not in current node
            root_node = await self.tree_manager.get_root_node()
            if root_node:
                evidence = root_node.evidence
        
        # Increment generation attempt counter
        node.generation_attempts += 1
        await self.tree_manager.update_node(node.nodeId, {"generation_attempts": node.generation_attempts})
        self.logger.info(f"SQL generation attempt #{node.generation_attempts} for node {current_node_id}")
        
        # Get intelligent learning guidance from SQLEvaluator
        strategic_guidance = await self._get_strategic_guidance(current_node_id)
        
        # Build context with ALL node information - let the prompt guide how to use it
        context = {
            "current_node": json.dumps(node_dict, indent=2),
            "evidence": evidence if evidence else None,  # Make evidence explicit
            "children_nodes": json.dumps(children_info, indent=2) if children_info else None,
            "node_history": json.dumps(history_dicts, indent=2) if history_dicts else None,  # ALL history
            "strategic_guidance": strategic_guidance if strategic_guidance else None,  # INTELLIGENT LEARNING
            "success_patterns": success_patterns,  # DYNAMIC SUCCESS PATTERNS
            "failure_avoidance": failure_avoidance,  # DYNAMIC FAILURE AVOIDANCE
            "database_name": database_name
        }
        
        # Remove None values
        context = {k: v for k, v in context.items() if v is not None}
        
        self.logger.info(f"SQL generator context prepared for node: {current_node_id}")
        self.logger.info(f"Node detail: {node_dict}")
        self.logger.info(f"Full context: {context}")
        
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
                return {
                    "query_type": "unknown",
                    "sql": clean_sql_content(sql),
                    "explanation": "Extracted from response using fallback method",
                    "considerations": "XML parsing failed, used regex extraction"
                }
            
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
    
