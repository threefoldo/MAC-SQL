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
from utils import extract_sql_from_text, parse_xml_hybrid
from prompts import SQL_CONSTRAINTS


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
        return loader.get_prompt("sql_generator", version="v1.0")
    
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
        
        # Get SQL evaluation analysis if available
        analysis_key = f"node_{current_node_id}_analysis"
        evaluation_analysis = await self.memory.get(analysis_key)
        
        # Get refiner prompt if available
        refiner_key = f"node_{current_node_id}_refiner_prompt"
        refiner_prompt = await self.memory.get(refiner_key)
        
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
        
        # Build context with ALL node information - let the prompt guide how to use it
        context = {
            "current_node": json.dumps(node_dict, indent=2),
            "evidence": evidence if evidence else None,  # Make evidence explicit
            "children_nodes": json.dumps(children_info, indent=2) if children_info else None,
            "node_history": json.dumps(history_dicts, indent=2) if history_dicts else None,  # ALL history
            "sql_evaluation_analysis": json.dumps(evaluation_analysis, indent=2) if evaluation_analysis else None,
            "refiner_prompt": refiner_prompt if refiner_prompt else None
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
            
        last_message = result.messages[-1].content
        # Log the raw output for debugging
        self.logger.info(f"Raw LLM output: {last_message}")
        
        try:
            # Parse the XML output
            generation_result = self._parse_generation_xml(last_message)
            
            if generation_result and generation_result.get("sql"):
                sql = generation_result["sql"]
                explanation = generation_result.get("explanation", "")
                considerations = generation_result.get("considerations", "")
                
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
        """Parse the SQL generation XML output using hybrid approach"""
        # Use the hybrid parsing utility
        result = parse_xml_hybrid(output, 'generation')
        
        if result:
            # Clean up SQL (preserve line structure but remove extra whitespace)
            if result.get("sql"):
                # Split into lines, clean each line, then rejoin
                lines = result["sql"].split('\n')
                cleaned_lines = []
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('--'):  # Skip empty lines and comments
                        # Clean internal whitespace but preserve line structure
                        line = re.sub(r'\s+', ' ', line)
                        cleaned_lines.append(line)
                
                result["sql"] = ' '.join(cleaned_lines).strip()
            
            return result
        
        # Fallback: try to extract SQL directly
        sql = extract_sql_from_text(output)
        if sql:
            return {
                "query_type": "unknown",
                "sql": sql,
                "explanation": "Extracted from response",
                "considerations": ""
            }
        return None
    
    
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
    
