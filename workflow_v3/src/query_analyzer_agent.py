"""
Query Analyzer Agent for text-to-SQL tree orchestration.

This agent analyzes user queries and creates structured intents in memory.
For complex queries, it decomposes them into simpler sub-queries and builds
a query tree structure.
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from task_context_manager import TaskContextManager
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_content_types import (
    QueryNode, NodeStatus
)
from prompts import SUBQ_PATTERN, SQL_CONSTRAINTS
from utils import parse_xml_hybrid


class QueryAnalyzerAgent(BaseMemoryAgent):
    """
    Analyzes user queries and creates structured query trees.
    
    This agent:
    1. Analyzes the user's natural language query
    2. Creates a structured intent as the root node
    3. Decomposes complex queries into simpler sub-queries
    4. Builds a query tree structure in memory
    """
    
    agent_name = "query_analyzer"
    
    def _initialize_managers(self):
        """Initialize the managers needed for query analysis"""
        self.task_manager = TaskContextManager(self.memory)
        self.schema_manager = DatabaseSchemaManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
    
    def _build_system_message(self) -> str:
        """Build the system message for query analysis"""
        from prompts.prompt_loader import PromptLoader
        loader = PromptLoader()
        return loader.get_prompt("query_analyzer", version="v1.2")
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from current node and parent if needed - NODE-FOCUSED VERSION"""
        context = {}
        
        # 1. OPERATE ON CURRENT NODE - Get current node information
        current_node_id = await self.tree_manager.get_current_node_id()
        if not current_node_id:
            self.logger.error("No current node - QueryAnalyzer requires a current node")
            return {"error": "No current node available"}
        
        current_node = await self.tree_manager.get_node(current_node_id)
        if not current_node:
            self.logger.error(f"Current node {current_node_id} not found")
            return {"error": f"Current node {current_node_id} not found"}
        
        # 2. READ PAST EXECUTION INFORMATION from current node
        if hasattr(current_node, 'queryAnalysis') and current_node.queryAnalysis:
            context["previous_query_analysis"] = current_node.queryAnalysis
            self.logger.info("Found previous query analysis in current node - this is a re-analysis")
        
        # 3. GET QUERY AND EVIDENCE - from current node first, then parent, then task context
        query = current_node.intent if current_node.intent else None
        evidence = None
        database_name = None
        
        # Try to get evidence from current node
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
        
        # Fallback to task context if still missing
        if not query or not database_name:
            task_context = await self.task_manager.get()
            if task_context:
                if not query:
                    query = task_context.originalQuery
                database_name = task_context.databaseName
                if not evidence and task_context.evidence:
                    evidence = task_context.evidence
            else:
                if not query:
                    query = task
                    self.logger.warning("Using task parameter as query fallback")
        
        context["query"] = query
        context["database_id"] = database_name
        if evidence:
            context["evidence"] = evidence
            # CRITICAL: Parse evidence for complex patterns
            evidence_analysis = self._parse_evidence_patterns(evidence)
            if evidence_analysis:
                context["evidence_analysis"] = evidence_analysis
                self.logger.info(f"Evidence analysis: {evidence_analysis}")
        
        # 4. GET SCHEMA INFORMATION - from current node, parent, or database (QueryAnalyzer can access database)
        schema_info = None
        
        # Check current node for schema information
        if hasattr(current_node, 'schema_linking') and current_node.schema_linking:
            schema_info = current_node.schema_linking
            self.logger.info("Found schema information in current node")
        # Check parent node for schema information
        elif parent_node and hasattr(parent_node, 'schema_linking') and parent_node.schema_linking:
            schema_info = parent_node.schema_linking
            self.logger.info("Found schema information in parent node")
        # QueryAnalyzer can read directly from database if no schema info in nodes
        else:
            schema_xml = await self._get_schema_xml()
            context["schema"] = schema_xml
            self.logger.info("Reading schema directly from database (QueryAnalyzer privilege)")
        
        if schema_info:
            context["existing_schema_analysis"] = schema_info
        
        # 5. CHECK FOR OTHER EXECUTION RESULTS in the tree for context
        try:
            tree_data = await self.tree_manager.get_tree()
            sql_results = []
            evaluation_results = []
            
            for node_id, node_data in tree_data.get("nodes", {}).items():
                # Collect SQL generation results
                if node_data.get("generation") and node_data["generation"].get("sql"):
                    sql_results.append({
                        "node_id": node_id,
                        "intent": node_data.get("intent"),
                        "sql": node_data["generation"]["sql"],
                        "explanation": node_data["generation"].get("explanation")
                    })
                elif node_data.get("sql"):
                    sql_results.append({
                        "node_id": node_id,
                        "intent": node_data.get("intent"),
                        "sql": node_data["sql"],
                        "explanation": node_data.get("sqlExplanation")
                    })
                
                # Collect evaluation results
                if node_data.get("evaluation"):
                    evaluation_results.append({
                        "node_id": node_id,
                        "intent": node_data.get("intent"),
                        "evaluation": node_data["evaluation"],
                        "sql": node_data.get("sql") or (node_data.get("generation", {}).get("sql") if node_data.get("generation") else None)
                    })
            
            if sql_results:
                context["existing_sql_results"] = sql_results
                self.logger.info(f"Found {len(sql_results)} SQL generation results in tree")
            if evaluation_results:
                context["existing_evaluation_feedback"] = evaluation_results
                self.logger.info(f"Found {len(evaluation_results)} evaluation results in tree")
        except Exception as e:
            self.logger.debug(f"Could not scan tree for execution results: {e}")
        
        self.logger.info(f"Query analyzer operating on node: {current_node_id}")
        self.logger.info(f"Query: {query}")
        self.logger.info(f"Database: {database_name}")
        
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the analysis results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        # Log the raw output for debugging
        self.logger.info(f"Raw LLM output: {last_message}")
        
        try:
            # Parse the XML output
            analysis = self._parse_analysis_xml(last_message)
            
            if analysis:
                # Get current node (root node created by orchestrator)
                current_node_id = await self.tree_manager.get_current_node_id()
                if not current_node_id:
                    self.logger.error("No current node found - orchestrator should have initialized the tree")
                    return
                
                # Store the entire analysis result in the QueryTree node
                await self.tree_manager.update_node(current_node_id, {"queryAnalysis": analysis})
                
                # Update the root node with the analyzed intent
                await self.tree_manager.update_node(current_node_id, {"intent": analysis.get("intent", "")})
                
                # Record the analysis in history
                node = await self.tree_manager.get_node(current_node_id)
                if node:
                    await self.history_manager.record_create(node)
                
                # If complex query, create sub-query nodes
                if analysis.get("complexity") == "complex" and "decomposition" in analysis:
                    await self._create_subquery_nodes(current_node_id, analysis["decomposition"])
                    
                    # For complex queries, set current node to first child
                    children = await self.tree_manager.get_children(current_node_id)
                    if children and len(children) > 0:
                        first_child_id = children[0].nodeId
                        await self.tree_manager.set_current_node_id(first_child_id)
                        self.logger.debug(f"Complex query - set first child {first_child_id} as current node")
                
                
                # User-friendly logging
                self.logger.info("="*60)
                self.logger.info("Query Analysis")
                self.logger.info(f"Query: {task}")
                self.logger.info(f"Intent: {analysis.get('intent', 'N/A')}")
                complexity = analysis.get('complexity', 'unknown')
                self.logger.info(f"Complexity: {complexity.upper()}")
                
                if complexity == 'complex' and 'decomposition' in analysis:
                    # Safely handle subqueries with consistent key handling
                    decomposition = analysis['decomposition']
                    subqueries = self._extract_subqueries_safely(decomposition)
                    
                    if subqueries:
                        self.logger.info(f"Decomposed into {len(subqueries)} sub-queries:")
                        for sq in subqueries:
                            intent = sq.get('intent', 'N/A') if isinstance(sq, dict) else 'N/A'
                            self.logger.info(f"  - {intent}")
                        
                        # Safely extract combination strategy
                        combination = decomposition.get('combination', decomposition.get('combination_strategy', {}))
                        if isinstance(combination, dict):
                            strategy = combination.get('strategy', combination.get('method', 'N/A'))
                        else:
                            strategy = 'N/A'
                        self.logger.info(f"Combination strategy: {strategy.upper()}")
                
                self.logger.info("="*60)
                self.logger.debug(f"Root node ID: {current_node_id}")
                
        except Exception as e:
            self.logger.error(f"Error parsing analysis results: {str(e)}", exc_info=True)
    
    async def _get_schema_xml(self) -> str:
        """Get database schema in XML format"""
        tables = await self.schema_manager.get_all_tables()
        
        if not tables:
            return "<database_schema>No schema loaded</database_schema>"
        
        xml_parts = ["<database_schema>"]
        
        for table_name, table in tables.items():
            xml_parts.append(f'  <table name="{table_name}">')
            
            for col_name, col_info in table.columns.items():
                xml_parts.append(f'    <column name="{col_name}">')
                xml_parts.append(f'      <type>{col_info.dataType}</type>')
                xml_parts.append(f'      <nullable>{col_info.nullable}</nullable>')
                
                if col_info.isPrimaryKey:
                    xml_parts.append('      <primary_key>true</primary_key>')
                
                if col_info.isForeignKey and col_info.references:
                    xml_parts.append(f'      <foreign_key>')
                    xml_parts.append(f'        <references_table>{col_info.references["table"]}</references_table>')
                    xml_parts.append(f'        <references_column>{col_info.references["column"]}</references_column>')
                    xml_parts.append(f'      </foreign_key>')
                
                xml_parts.append('    </column>')
            
            xml_parts.append('  </table>')
        
        xml_parts.append('</database_schema>')
        
        return '\n'.join(xml_parts)
    
    def _parse_analysis_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the analysis XML output using hybrid approach with robust fallback"""
        try:
            # Try v1.2 format first
            analysis = parse_xml_hybrid(output, 'query_analysis')
            if analysis:
                # Convert v1.2 nested structure to flat structure expected by rest of code
                converted = {}
                
                # Safe extraction from nested structures
                if isinstance(analysis.get('schema_integration'), dict):
                    schema_integration = analysis['schema_integration']
                    converted['corrected_output'] = schema_integration.get('corrected_output', '')
                    converted['corrected_constraints'] = schema_integration.get('corrected_constraints', '')
                
                if isinstance(analysis.get('dependency_analysis'), dict):
                    dependency_analysis = analysis['dependency_analysis']
                    converted['dependency_type'] = dependency_analysis.get('dependency_type', '')
                    converted['dependency_description'] = dependency_analysis.get('dependency_description', '')
                    
                if isinstance(analysis.get('decomposition_decision'), dict):
                    decomposition_decision = analysis['decomposition_decision']
                    converted['complexity'] = decomposition_decision.get('complexity', '')
                    converted['reasoning'] = decomposition_decision.get('reasoning', '')
                    converted['single_step_possible'] = decomposition_decision.get('single_step_possible', '')
                    converted['single_table_solution'] = decomposition_decision.get('single_table_solution', '')
                
                # Safe handling of tables
                if 'tables' in analysis:
                    converted['tables'] = analysis['tables']
                    
                # Safe handling of decomposition
                if 'decomposition' in analysis:
                    converted['decomposition'] = analysis['decomposition']
                
                # Merge other top-level fields safely
                for key, value in analysis.items():
                    if key not in converted and value is not None:
                        converted[key] = value
                        
                return converted
            
            # Try v1.1 format fallback
            analysis = parse_xml_hybrid(output, 'analysis')
        
            # If parsing failed, try manual extraction for critical fields
            if not analysis:
                analysis = self._extract_critical_fields(output)
        
        except Exception as e:
            self.logger.error(f"Error in primary XML parsing: {str(e)}", exc_info=True)
            # Fallback to manual extraction
            analysis = self._extract_critical_fields(output)
        
        # Handle fallback parsing for subqueries if needed
        if analysis and not analysis.get("decomposition", {}).get("subqueries") and SUBQ_PATTERN:
            try:
                analysis = self._extract_subqueries_fallback(output, analysis)
            except Exception as e:
                self.logger.warning(f"Failed to extract subqueries fallback: {str(e)}")
        
        return analysis if analysis else None
    
    def _extract_critical_fields(self, output: str) -> Dict[str, Any]:
        """Extract critical fields using regex fallback when XML parsing fails"""
        analysis = {}
        
        try:
            # Extract intent
            intent_match = re.search(r'<intent>(.*?)</intent>', output, re.DOTALL)
            if intent_match:
                analysis["intent"] = intent_match.group(1).strip()
            else:
                # Fallback: look for intent in malformed tag
                intent_match = re.search(r'<intent>(.*?)(?:<|$)', output, re.DOTALL)
                if intent_match:
                    analysis["intent"] = intent_match.group(1).strip()
            
            # Extract complexity
            complexity_match = re.search(r'<complexity>(.*?)</complexity>', output, re.DOTALL)
            if complexity_match:
                analysis["complexity"] = complexity_match.group(1).strip()
            else:
                # Fallback: look for complexity in malformed tag
                complexity_match = re.search(r'<complexity>(.*?)(?:<|$)', output, re.DOTALL)
                if complexity_match:
                    analysis["complexity"] = complexity_match.group(1).strip()
            
            # Extract tables if present (basic fallback)
            tables_match = re.search(r'<tables>(.*?)</tables>', output, re.DOTALL)
            if tables_match:
                # Simple extraction - just get table names
                table_names = re.findall(r'<table[^>]*name="([^"]*)"', tables_match.group(1))
                if table_names:
                    analysis["tables"] = [{"name": name} for name in table_names]
        
        except Exception as e:
            self.logger.warning(f"Error in critical field extraction: {str(e)}")
            
        return analysis
    
    def _extract_subqueries_fallback(self, output: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Extract subqueries using fallback pattern matching"""
        try:
            # Look for "Sub question N:" pattern in the output as fallback
            matches = re.finditer(SUBQ_PATTERN, output)
            subqueries = []
            for i, match in enumerate(matches, 1):
                # Extract text after the match until next sub question or end
                start = match.end()
                next_match = re.search(SUBQ_PATTERN, output[start:])
                end = start + next_match.start() if next_match else len(output)
                
                sub_text = output[start:end].strip()
                if sub_text:
                    # Try to extract intent from the sub-question text
                    intent_match = re.search(r'^([^.!?]+[.!?])', sub_text)
                    intent = intent_match.group(1) if intent_match else sub_text[:100]
                    
                    subqueries.append({
                        "id": str(i),
                        "intent": intent.strip(),
                        "description": sub_text[:200],
                        "tables": []  # Will be determined by schema linker
                    })
            
            if subqueries:
                if "decomposition" not in analysis:
                    analysis["decomposition"] = {}
                analysis["decomposition"]["subqueries"] = subqueries
        
        except Exception as e:
            self.logger.warning(f"Error extracting subqueries: {str(e)}")
        
        return analysis
    
    def _extract_subqueries_safely(self, decomposition: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Safely extract subqueries from decomposition data"""
        if not isinstance(decomposition, dict):
            return []
            
        # Handle different possible keys and structures
        subqueries = []
        
        # Try 'subqueries' first (list format)
        if 'subqueries' in decomposition:
            subqs = decomposition['subqueries']
            if isinstance(subqs, list):
                subqueries = subqs
            elif isinstance(subqs, dict):
                subqueries = [subqs]  # Single subquery as dict
        
        # Try 'subquery' fallback (can be list or single dict)
        elif 'subquery' in decomposition:
            subqs = decomposition['subquery']
            if isinstance(subqs, list):
                subqueries = subqs
            elif isinstance(subqs, dict):
                subqueries = [subqs]  # Single subquery as dict
        
        # Try 'step' (v1.2 format)
        elif 'step' in decomposition:
            steps = decomposition['step']
            if isinstance(steps, list):
                subqueries = steps
            elif isinstance(steps, dict):
                subqueries = [steps]
        
        # Ensure all items are dictionaries
        valid_subqueries = []
        for sq in subqueries:
            if isinstance(sq, dict):
                valid_subqueries.append(sq)
        
        return valid_subqueries
    
    async def _create_subquery_nodes(self, parent_id: str, decomposition: Dict[str, Any]) -> None:
        """Create sub-query nodes in the tree with inherited schema information"""
        # Safely extract subqueries
        subqueries = self._extract_subqueries_safely(decomposition)
        
        # Get parent node's schema linking information for inheritance
        parent_node = await self.tree_manager.get_node(parent_id)
        parent_schema_linking = None
        if parent_node and hasattr(parent_node, 'schema_linking') and parent_node.schema_linking:
            parent_schema_linking = parent_node.schema_linking
        elif parent_node:
            # Try to get schema_linking from the raw node data
            parent_data = await self.tree_manager.get_tree()
            if parent_data and "nodes" in parent_data and parent_id in parent_data["nodes"]:
                parent_schema_linking = parent_data["nodes"][parent_id].get("schema_linking")
        
        # Create nodes for each subquery
        created_nodes = []
        for sq in subqueries:
            # Generate node ID
            node_id = f"node_{datetime.now().timestamp()}_{sq['id']}"
            
            # Create the node with just the basic info
            # Other agent outputs will be populated later by respective agents
            node = QueryNode(
                nodeId=node_id,
                intent=sq["intent"],
                parentId=parent_id
            )
            
            # Add to tree
            await self.tree_manager.add_node(node, parent_id)
            
            # Store the subquery info directly in the node
            await self.tree_manager.update_node(node_id, {"subqueryInfo": sq})
            
            # Inherit schema linking information from parent if available
            if parent_schema_linking:
                await self.tree_manager.update_node(node_id, {"schema_linking": parent_schema_linking})
                self.logger.info(f"Inherited schema linking from parent {parent_id} to child {node_id}")
                
                # Log inherited schema information for debugging
                if isinstance(parent_schema_linking, dict) and "selected_tables" in parent_schema_linking:
                    selected_tables = parent_schema_linking["selected_tables"]
                    if isinstance(selected_tables, dict) and "table" in selected_tables:
                        tables = selected_tables["table"]
                        if isinstance(tables, list):
                            table_names = [t.get("name", "unknown") for t in tables if isinstance(t, dict)]
                            self.logger.info(f"  Inherited tables: {', '.join(table_names)}")
                        elif isinstance(tables, dict):
                            self.logger.info(f"  Inherited table: {tables.get('name', 'unknown')}")
            else:
                self.logger.warning(f"No schema linking found in parent {parent_id} to inherit")
            
            # Record in history
            node = await self.tree_manager.get_node(node_id)
            if node:
                await self.history_manager.record_create(node)
            
            created_nodes.append(node_id)
            self.logger.debug(f"Created subquery node: {node_id}")
        
        # Log summary of created nodes
        if created_nodes:
            self.logger.info(f"Created {len(created_nodes)} child nodes with inherited schema information")
        
    
    async def get_analysis_summary(self) -> Dict[str, Any]:
        """Get a summary of the current analysis"""
        analysis = await self.memory.get("query_analysis")
        tree_stats = await self.tree_manager.get_tree_stats()
        
        summary = {
            "has_analysis": analysis is not None,
            "complexity": analysis.get("complexity") if analysis else None,
            "tree_stats": tree_stats
        }
        
        if analysis and "decomposition" in analysis:
            summary["subquery_count"] = len(analysis["decomposition"].get("subqueries", []))
            summary["combination_strategy"] = analysis["decomposition"].get("combination", {}).get("strategy")
        
        return summary
    
    def _parse_evidence_patterns(self, evidence: str) -> Optional[Dict[str, Any]]:
        """Parse evidence for complex calculation patterns (CRITICAL FIX for Example 25)"""
        try:
            evidence_lower = evidence.lower()
            
            # Just provide the evidence to the LLM for analysis - no hardcoded pattern matching
            return {
                "evidence_provided": True,
                "evidence_text": evidence,
                "note": "Evidence complexity assessment should be done by LLM in prompt, not hardcoded patterns"
            }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error parsing evidence patterns: {str(e)}", exc_info=True)
            return None