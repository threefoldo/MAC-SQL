"""
SQL Generator Agent for text-to-SQL workflow.

This agent generates SQL queries based on query node intents and their
linked schema information.
"""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_content_types import (
    QueryNode, QueryMapping, NodeStatus, ExecutionResult,
    CombineStrategy, CombineStrategyType
)
from utils import extract_sql_from_text


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
        return """You are an expert SQL query generator for SQLite databases. Your role is to generate accurate SQL queries based on the provided node information.

## Context Understanding
You will receive a JSON object containing:
- **current_node**: Complete information about the query node including intent, mapping, status, and any previous attempts
- **children_nodes** (optional): Information about child nodes if this is a complex query
- **node_history** (optional): Recent operations on this node including errors and revisions

## Key Information to Extract from Node
1. **intent**: The natural language query to convert to SQL
2. **mapping**: Schema information with tables, columns, and joins to use
3. **complexity**: Whether this is a simple or complex query
4. **sql**: Any previously generated SQL (if this is a revision)
5. **result**: Previous execution results including errors

## SQL Generation Guidelines

### For Simple Queries:
- Generate straightforward SELECT statements
- Use only the tables and columns specified in the mapping
- Apply filters, aggregations, and sorting as implied by the intent
- Keep the query as simple as possible while meeting requirements

### For Complex Queries with Children:
- Check if children have SQL already generated
- Combine child queries using appropriate methods:
  - CTEs (Common Table Expressions) for better readability
  - Subqueries when simpler
  - UNION/UNION ALL for combining similar results
  - JOINs when correlating different aspects

### Error Recovery:
If the node has previous errors in history or result:
- Analyze what went wrong (syntax, missing columns, incorrect joins)
- Generate a corrected version addressing the specific issue
- Mention what you fixed in the explanation

### SQLite-Specific Considerations:
- No FULL OUTER JOIN (use LEFT JOIN + UNION)
- Use CAST for type conversions
- datetime() function for date operations
- GROUP BY requires all non-aggregate SELECT columns
- LIMIT without ORDER BY may return inconsistent results

### Best Practices:
- Use meaningful table aliases from the mapping
- Qualify all columns with table aliases to avoid ambiguity
- Include only necessary columns (avoid SELECT *)
- Use proper JOIN conditions from the mapping
- Add comments for complex logic using /* */

## Output Format:

<sql_generation>
  <query_type>simple|join|aggregate|subquery|complex</query_type>
  <sql>
    -- Generated SQL query
    Your SQL query here
  </sql>
  <explanation>
    Brief explanation of how the query addresses the intent
  </explanation>
  <considerations>
    - Any assumptions made
    - Potential limitations
    - Error fixes applied (if revision)
  </considerations>
</sql_generation>

IMPORTANT: Generate SQL that exactly matches the intent using ONLY the schema elements provided in the mapping."""
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from memory before generating SQL"""
        # Get current_node_id from QueryTreeManager
        current_node_id = await self.tree_manager.get_current_node_id()
        if not current_node_id:
            return {"error": "No current_node_id found"}
        
        self.logger.debug(f"Using current_node_id from QueryTreeManager: {current_node_id}")
        
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
        
        # Build context with all node information as a single string
        context = {
            "current_node": json.dumps(node_dict, indent=2),
            "children_nodes": json.dumps(children_info, indent=2) if children_info else None,
            "node_history": json.dumps(history_dicts[-5:], indent=2) if history_dicts else None  # Last 5 operations
        }
        
        # Remove None values
        context = {k: v for k, v in context.items() if v is not None}
        
        self.logger.debug(f"SQL generator context prepared for node: {current_node_id}")
        
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the SQL generation results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        
        try:
            # Parse the XML output
            generation_result = self._parse_generation_xml(last_message)
            
            if generation_result and generation_result.get("sql"):
                sql = generation_result["sql"]
                
                # Get current node ID from QueryTreeManager
                node_id = await self.tree_manager.get_current_node_id()
                
                if node_id:
                    # Update the node with SQL
                    await self.tree_manager.update_node_sql(node_id, sql)
                    
                    # Record in history
                    await self.history_manager.record_generate_sql(
                        node_id=node_id,
                        sql=sql
                    )
                    
                    # User-friendly logging
                    self.logger.info("="*60)
                    self.logger.info("SQL Generation")
                    
                    # Get node for intent
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        self.logger.info(f"Query intent: {node.intent}")
                    
                    # Log query type
                    if generation_result.get("query_type"):
                        self.logger.info(f"Query type: {generation_result['query_type'].upper()}")
                    
                    # Log the generated SQL
                    self.logger.info("Generated SQL:")
                    # Format SQL for better readability
                    sql_lines = sql.split('\n') if '\n' in sql else [sql]
                    for line in sql_lines:
                        if line.strip():
                            self.logger.info(f"  {line}")
                    
                    # Log explanation if available
                    if generation_result.get("explanation"):
                        self.logger.info(f"Explanation: {generation_result['explanation']}")
                    
                    self.logger.info("="*60)
                    self.logger.debug(f"Updated node {node_id} with generated SQL")
                else:
                    # Store SQL in memory
                    await memory.set("generated_sql", sql)
                    self.logger.debug("Stored generated SQL in memory")
                
                # Store additional metadata
                await memory.set("last_sql_generation", generation_result)
                
        except Exception as e:
            self.logger.error(f"Error parsing SQL generation results: {str(e)}", exc_info=True)
    
    
    def _parse_generation_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the SQL generation XML output"""
        try:
            # Extract XML
            xml_match = re.search(r'<sql_generation>.*?</sql_generation>', output, re.DOTALL)
            if not xml_match:
                # Try code block
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
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
            else:
                xml_content = xml_match.group()
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            result = {
                "query_type": root.findtext("query_type", "simple").strip(),
                "sql": root.findtext("sql", "").strip(),
                "explanation": root.findtext("explanation", "").strip(),
                "considerations": root.findtext("considerations", "").strip()
            }
            
            # Clean up SQL (remove extra whitespace)
            if result["sql"]:
                result["sql"] = re.sub(r'\s+', ' ', result["sql"]).strip()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing SQL generation XML: {str(e)}", exc_info=True)
            
            # Try to extract SQL as fallback
            sql = extract_sql_from_text(output)
            if sql:
                return {
                    "query_type": "unknown",
                    "sql": sql,
                    "explanation": "Fallback extraction",
                    "considerations": ""
                }
            
            return None
    
    async def generate_sql(self, node_id: str) -> Optional[str]:
        """
        Generate SQL for a specific query node.
        
        Args:
            node_id: The ID of the node to generate SQL for
            
        Returns:
            The generated SQL or None if failed
        """
        self.logger.debug(f"Generating SQL for node: {node_id}")
        
        # Set the current node in QueryTreeManager
        await self.tree_manager.set_current_node_id(node_id)
        
        # Run the agent
        task = "Generate SQL for the current query node"
        result = await self.run(task)
        
        # Get the node to check if it has SQL
        node = await self.tree_manager.get_node(node_id)
        if node and node.sql:
            return node.sql
        
        return None
    
    async def generate_combined_sql(self, parent_node_id: str) -> Optional[str]:
        """
        Generate combined SQL for a parent node with children.
        
        Args:
            parent_node_id: The ID of the parent node
            
        Returns:
            The combined SQL or None if failed
        """
        # Get parent and children
        parent = await self.tree_manager.get_node(parent_node_id)
        children = await self.tree_manager.get_children(parent_node_id)
        
        if not parent or not children:
            return await self.generate_sql(parent_node_id)
        
        # Check if all children have SQL
        for child in children:
            if not child.sql:
                self.logger.warning(f"Child node {child.nodeId} missing SQL")
                return None
        
        # Generate combined SQL based on strategy
        self.logger.info("="*60)
        self.logger.info("Generating Combined SQL")
        self.logger.info(f"Combining results from {len(children)} sub-queries")
        self.logger.info("="*60)
        self.logger.debug(f"Generating combined SQL for parent {parent_node_id} with {len(children)} children")
        
        # Set the current node in QueryTreeManager
        await self.tree_manager.set_current_node_id(parent_node_id)
        
        # Run with special task
        task = "Generate combined SQL using child queries for the current parent node"
        result = await self.run(task)
        
        # Get the node to check if it has SQL
        node = await self.tree_manager.get_node(parent_node_id)
        if node and node.sql:
            return node.sql
        
        return None