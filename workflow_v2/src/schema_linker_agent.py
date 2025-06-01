"""
Schema Linker Agent for text-to-SQL tree orchestration.

This agent links relevant schema information to query nodes in the tree.
It analyzes the intent of a node and finds all relevant tables, columns,
and relationships needed to generate SQL for that node.
"""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
import json

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from task_context_manager import TaskContextManager
from memory_content_types import (
    QueryNode, TableSchema, ColumnInfo, NodeStatus, NodeOperationType
)
from prompts import SQL_CONSTRAINTS
from utils import parse_xml_hybrid, strip_quotes, ensure_list


class SchemaLinkerAgent(BaseMemoryAgent):
    """
    Links database schema elements to query nodes.
    
    This agent:
    1. Analyzes a query node's intent
    2. Identifies relevant tables and columns
    3. Determines join relationships
    4. Updates the node's mapping with schema information
    """
    
    agent_name = "schema_linker"
    
    def _initialize_managers(self):
        """Initialize the managers needed for schema linking"""
        self.task_manager = TaskContextManager(self.memory)
        self.schema_manager = DatabaseSchemaManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
    
    
    def _log_linking_summary(self, linking_result: Dict[str, Any]) -> None:
        """Log a concise summary of the linking result."""
        # Log tables if present
        if "selected_tables" in linking_result:
            tables = linking_result["selected_tables"].get("table", [])
            if not isinstance(tables, list):
                tables = [tables] if tables else []
            
            if tables:
                self.logger.info(f"Linked {len(tables)} table(s):")
                for table in tables:
                    if isinstance(table, dict):
                        self.logger.info(f"  - {table.get('name', 'unknown')}: {table.get('purpose', '')}")
        
        # Log joins if present
        if "joins" in linking_result:
            joins_data = linking_result["joins"]
            if isinstance(joins_data, dict):
                joins = joins_data.get("join", [])
                if not isinstance(joins, list):
                    joins = [joins] if joins else []
            else:
                joins = []
            
            if joins:
                self.logger.info(f"Joins: {len(joins)}")
                for join in joins:
                    if isinstance(join, dict):
                        self.logger.info(f"  - {join.get('from_table', '')} → {join.get('to_table', '')}")
    
    def _extract_text(self, element: Optional[ET.Element], tag: str, default: str = "") -> str:
        """Extract text from XML element safely."""
        if element is None:
            return default
        found = element.find(tag)
        if found is not None and found.text:
            return found.text.strip()
        return default
    
    def _build_system_message(self) -> str:
        """Build the system message for schema linking"""
        from prompts.prompt_loader import PromptLoader
        loader = PromptLoader()
        return loader.get_prompt("schema_linker", version="v1.1")
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from memory before schema linking"""
        # Get task context for original query and evidence
        task_context = await self.task_manager.get()
        if not task_context:
            self.logger.error("Task context not found")
            return {"error": "Task context not initialized"}
        
        # Get current_node_id from QueryTreeManager for node-specific operations
        current_node_id = await self.tree_manager.get_current_node_id()
        if not current_node_id:
            return {"error": "No current_node_id found"}
        
        # Get the node from QueryTreeManager
        node = await self.tree_manager.get_node(current_node_id)
        if not node:
            return {"error": f"Node {current_node_id} not found"}
        
        # Convert entire node to dictionary for simple string representation
        node_dict = node.to_dict()
        
        # Get parent and sibling info if node has parent
        parent_info = None
        siblings_info = []
        if node.parentId:
            parent_node = await self.tree_manager.get_node(node.parentId)
            if parent_node:
                parent_info = parent_node.to_dict()
                # Get siblings only if parent exists
                siblings = await self.tree_manager.get_children(node.parentId)
                siblings_info = [s.to_dict() for s in siblings if s.nodeId != current_node_id]
        
        # Get node operation history
        history = await self.history_manager.get_node_operations(current_node_id)
        # Convert NodeOperation objects to dictionaries
        history_dicts = [op.to_dict() for op in history] if history else []
        
        # Get SQL evaluation analysis from node if available
        evaluation_analysis = node.evaluation if node.evaluation else None
        
        # Create context from task information
        context = {
            "original_query": task_context.originalQuery,
            "database_name": task_context.databaseName,
            "evidence": task_context.evidence
        }
        
        # Add node-specific information
        context.update({
            "current_node": json.dumps(node_dict, indent=2),
            "parent_node": json.dumps(parent_info, indent=2) if parent_info else None,
            "sibling_nodes": json.dumps(siblings_info, indent=2) if siblings_info else None,
            "node_history": json.dumps(history_dicts, indent=2) if history_dicts else None,
            "sql_evaluation_analysis": json.dumps(evaluation_analysis, indent=2) if evaluation_analysis else None,
        })
        
        # Ensure we have the full schema (may be in context already or need to fetch)
        if "full_schema" not in context:
            context["full_schema"] = await self._get_full_schema_xml()
        
        # Remove None values
        context = {k: v for k, v in context.items() if v is not None}
        
        self.logger.info(f"Schema linking context prepared for node: {current_node_id}")
        self.logger.info(f"Node details: {node_dict}")
        self.logger.info(f"Full context: {context}")
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the schema linking results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        # Log the raw output for debugging
        self.logger.info(f"Raw LLM output: {last_message}")
        
        try:
            # Parse the XML output
            linking_result = self._parse_linking_xml(last_message)
            
            if linking_result:
                # Schema linking results are stored in the node, not in separate memory
                
                # Get the current node ID from QueryTreeManager
                node_id = await self.tree_manager.get_current_node_id()
                
                if node_id:
                    # Store the entire linking result in the node's schema_linking field
                    await self.tree_manager.update_node(node_id, {"schema_linking": linking_result})
                    self.logger.info(f"Stored schema linking result in query tree node {node_id}")
                    
                    # Also update the schema_linking memory as expected by tests/orchestrator
                    schema_context = await self.memory.get("schema_linking")
                    if schema_context:
                        schema_context["schema_analysis"] = linking_result
                        schema_context["last_update"] = datetime.now().isoformat()
                        await self.memory.set("schema_linking", schema_context)
                    
                    # Record in history - get the node and update its status
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        # Update node status to show schema linking is completed
                        node.status = NodeStatus.SCHEMA_LINKED if hasattr(NodeStatus, 'SCHEMA_LINKED') else NodeStatus.CREATED
                        await self.history_manager.record_node_operation(
                            node, 
                            NodeOperationType.CREATE
                        )
                    
                    # Enhanced user-friendly logging
                    self.logger.info("="*60)
                    self.logger.info("Schema Linking")
                    
                    # Get node for intent
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        self.logger.info(f"Query intent: {node.intent}")
                    
                    # Log column discovery process if available
                    if linking_result.get("column_discovery"):
                        self.logger.info("Column Discovery Process:")
                        column_discovery = linking_result["column_discovery"]
                        self.logger.info(f"  {column_discovery}")
                    
                    # Log single-table analysis
                    if linking_result.get("single_table_possible") is not None:
                        if linking_result.get("single_table_possible"):
                            best_table = linking_result.get("best_single_table", "")
                            self.logger.info(f"✓ Single-table solution POSSIBLE using table: {best_table}")
                            if linking_result.get("single_table_solution"):
                                self.logger.info("✓ Single-table solution SELECTED")
                        else:
                            self.logger.info("✗ Single-table solution NOT possible - multiple tables required")
                    
                    # Log summary based on linking_result
                    self._log_linking_summary(linking_result)
                    
                    self.logger.info("="*60)
                    self.logger.info(f"Updated node {node_id} with schema mapping")
                else:
                    self.logger.warning("No node_id found to update with schema mapping")
                
        except Exception as e:
            self.logger.error(f"Error parsing schema linking results: {str(e)}", exc_info=True)
    
    async def _get_full_schema_xml(self) -> str:
        """Get full database schema with sample data in XML format"""
        import html
        
        tables = await self.schema_manager.get_all_tables()
        
        if not tables:
            return "<database_schema>No schema loaded</database_schema>"
        
        # Log available tables for debugging
        self.logger.info(f"Available tables in schema: {list(tables.keys())}")
        
        xml_parts = ["<database_schema>"]
        
        # Include database description if available
        description = await self.schema_manager.get_database_description()
        if description:
            xml_parts.append(f"  <description><![CDATA[{description}]]></description>")
        
        xml_parts.append(f"  <total_tables>{len(tables)}</total_tables>")
        xml_parts.append("  <tables>")
        
        for table_name, table in tables.items():
            xml_parts.append(f'    <table name="{html.escape(table_name)}">')
            xml_parts.append(f'      <column_count>{len(table.columns)}</column_count>')
            xml_parts.append('      <columns>')
            
            # Add columns
            for col_name, col_info in table.columns.items():
                # Escape special characters in column names
                escaped_col_name = html.escape(col_name)
                xml_parts.append(f'        <column name="{escaped_col_name}">')
                xml_parts.append(f'          <type>{col_info.dataType}</type>')
                xml_parts.append(f'          <nullable>{col_info.nullable}</nullable>')
                
                if col_info.isPrimaryKey:
                    xml_parts.append('          <primary_key>true</primary_key>')
                
                if col_info.isForeignKey and col_info.references:
                    xml_parts.append(f'          <foreign_key>')
                    xml_parts.append(f'            <references_table>{html.escape(col_info.references["table"])}</references_table>')
                    xml_parts.append(f'            <references_column>{html.escape(col_info.references["column"])}</references_column>')
                    xml_parts.append(f'          </foreign_key>')
                
                # Add typical values if available
                if col_info.typicalValues:
                    xml_parts.append('          <typical_values>')
                    # Limit to first 10 values for readability
                    for value in col_info.typicalValues[:10]:
                        if value is not None:
                            # Use CDATA to handle special characters in values
                            xml_parts.append(f'            <value><![CDATA[{value}]]></value>')
                        else:
                            xml_parts.append('            <value null="true"/>')
                    if len(col_info.typicalValues) > 10:
                        xml_parts.append(f'            <!-- {len(col_info.typicalValues) - 10} more values -->')
                    xml_parts.append('          </typical_values>')
                
                xml_parts.append('        </column>')
            
            xml_parts.append('      </columns>')
            xml_parts.append('    </table>')
        
        xml_parts.append("  </tables>")
        xml_parts.append('</database_schema>')
        
        return '\n'.join(xml_parts)
    
    def _parse_linking_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the schema linking XML output using hybrid approach"""
        # Use the hybrid parsing utility
        parsed = parse_xml_hybrid(output, 'schema_linking')
        return parsed
    
