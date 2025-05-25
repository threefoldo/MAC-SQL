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
        return """You are an expert SQL query generator for SQLite databases. Your job is to:

1. Generate correct SQL queries based on the provided intent and schema mapping
2. Use proper SQL syntax and best practices
3. Handle various query types: SELECT, JOINs, aggregations, subqueries
4. Consider performance and readability

Guidelines:
- Use table aliases when specified in the mapping
- Include only the necessary columns
- Use appropriate JOIN types (INNER, LEFT, etc.)
- Handle NULL values appropriately
- Use proper GROUP BY for aggregations
- Add ORDER BY when sorting is implied
- Use LIMIT for "top N" queries

For queries involving child nodes:
- Generate subqueries or CTEs as appropriate
- Consider the combination strategy (UNION, JOIN, etc.)

Output format:

<sql_generation>
  <query_type>simple|join|aggregate|subquery</query_type>
  <sql>
    Your generated SQL query here
  </sql>
  <explanation>
    Brief explanation of the query structure
  </explanation>
  <considerations>
    Any special considerations or assumptions made
  </considerations>
</sql_generation>

Return the SQL wrapped in the XML format above."""
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from memory before generating SQL"""
        context = {}
        
        # Extract node_id from task if provided
        node_id = None
        if "node:" in task:
            parts = task.split(" - ", 1)
            if parts[0].startswith("node:"):
                node_id = parts[0][5:]
        
        if not node_id:
            node_id = await memory.get("current_node_id")
        
        if node_id:
            # Get the node with its mapping
            node = await self.tree_manager.get_node(node_id)
            if node:
                context["node_id"] = node_id
                context["intent"] = node.intent
                
                # Add mapping information
                if node.mapping:
                    context["mapping"] = self._format_mapping_for_prompt(node.mapping)
                
                # Check for child nodes
                children = await self.tree_manager.get_children(node_id)
                if children:
                    context["has_children"] = True
                    context["child_count"] = len(children)
                    # Add child SQL if already generated
                    child_sql = []
                    for child in children:
                        if child.sql:
                            child_sql.append({
                                "intent": child.intent,
                                "sql": child.sql
                            })
                    if child_sql:
                        context["child_sql"] = child_sql
                
                # Add combination strategy if present
                if node.combineStrategy:
                    context["combination_strategy"] = node.combineStrategy.type.value
        else:
            # Use task as intent
            context["intent"] = task
            
            # Try to get schema context
            # Try selected schema first, fall back to full
            schema = await memory.get("selected_schema")
            if not schema:
                schema = await memory.get("full_database_schema")
            
            if schema:
                context["database_schema"] = schema
                
            # Add database ID if available
            db_id = await memory.get("database_id")
            if db_id:
                context["database_id"] = db_id
                
            # Add database path if available
            db_path = await memory.get("database_path")
            if db_path:
                context["database_path"] = db_path
        
        self.logger.debug(f"SQL generator context prepared for node: {node_id}")
        
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
                
                # Get node ID
                node_id = None
                if "node:" in task:
                    parts = task.split(" - ", 1)
                    if parts[0].startswith("node:"):
                        node_id = parts[0][5:]
                
                if not node_id:
                    node_id = await memory.get("current_node_id")
                
                if node_id:
                    # Update the node with SQL
                    await self.tree_manager.update_node_sql(node_id, sql)
                    
                    # Record in history
                    await self.history_manager.record_generate_sql(
                        node_id=node_id,
                        sql=sql
                    )
                    
                    self.logger.info(f"Updated node {node_id} with generated SQL")
                else:
                    # Store SQL in memory
                    await memory.set("generated_sql", sql)
                    self.logger.info("Stored generated SQL in memory")
                
                # Store additional metadata
                await memory.set("last_sql_generation", generation_result)
                
        except Exception as e:
            self.logger.error(f"Error parsing SQL generation results: {str(e)}", exc_info=True)
    
    def _format_mapping_for_prompt(self, mapping: QueryMapping) -> str:
        """Format the query mapping for the LLM prompt"""
        lines = ["Schema Mapping:"]
        
        # Tables
        if mapping.tables:
            lines.append("\nTables:")
            for table in mapping.tables:
                alias_part = f" (alias: {table.alias})" if table.alias else ""
                lines.append(f"  - {table.name}{alias_part}: {table.purpose}")
        
        # Columns
        if mapping.columns:
            lines.append("\nColumns:")
            for col in mapping.columns:
                lines.append(f"  - {col.table}.{col.column} (used for: {col.usedFor})")
        
        # Joins
        if mapping.joins:
            lines.append("\nJoins:")
            for join in mapping.joins:
                lines.append(f"  - {join.from_table} → {join.to}: {join.on}")
        
        return "\n".join(lines)
    
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
        self.logger.info(f"Generating SQL for node: {node_id}")
        
        # Store the node ID in memory
        await self.memory.set("current_node_id", node_id)
        
        # Run the agent
        task = f"node:{node_id} - Generate SQL for this query node"
        result = await self.run(task)
        
        # Get the generated SQL
        generation = await self.memory.get("last_sql_generation")
        if generation and generation.get("sql"):
            return generation["sql"]
        
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
        self.logger.info(f"Generating combined SQL for parent {parent_node_id} with {len(children)} children")
        
        # Store context for the generator
        await self.memory.set("current_node_id", parent_node_id)
        
        # Run with special task
        task = f"node:{parent_node_id} - Generate combined SQL using child queries"
        result = await self.run(task)
        
        # Get the result
        generation = await self.memory.get("last_sql_generation")
        if generation and generation.get("sql"):
            return generation["sql"]
        
        return None