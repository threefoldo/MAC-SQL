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
        return f"""You are a schema linking expert for text-to-SQL conversion.

## CRITICAL RULES (Must Follow)
1. **USE EXACT NAMES**: Table and column names are CASE-SENSITIVE - use them exactly as shown in the provided schema
2. **NO INVENTION**: Only use tables/columns that exist in the schema - never guess or create names
3. **SCHEMA SOURCE**: The 'full_schema' field below is your ONLY source of truth
4. **SINGLE TABLE PREFERENCE**: Always try single-table solutions first before considering joins
5. **COLUMN DISCOVERY**: Show all potentially relevant columns with sample data before selecting the best ones

## Your Task
**PRIMARY GOAL**: Analyze the FULL user query and find ALL relevant schema elements that could be used to answer it.

Use a comprehensive approach to link schema elements to the complete query:

### Phase 1: Query Understanding and Discovery

**Step 1.1: Analyze the Complete Query**
- Read the full user query (not just a decomposed intent)
- Identify ALL data elements mentioned or implied
- Look for entities, attributes, conditions, aggregations, and relationships
- Consider what data would be needed to fully answer this query

**Step 1.2: Available Schema Elements**
Before making any selections, list out:
- All available table names from the schema
- For each table, list ALL column names with their typical values
- Identify foreign key relationships

**Step 1.3: Comprehensive Column Search**
For EVERY entity/condition/attribute mentioned in the query:
- Search ALL tables for columns that could match
- Check typical_values in EVERY column across ALL tables
- Look for exact matches in typical_values first
- Rank candidates by how well their typical_values match the query terms
- **IMPORTANT**: Consider the query may be complex and need multiple tables/columns
- If retry, identify specific issues to fix from evaluation feedback

**Step 1.4: Single-Table Preference Check**
- After finding all matching columns, check if they all exist in ONE table
- If yes, strongly prefer the single-table solution
- Only use multiple tables if columns are spread across tables

### Phase 2: Schema Linking with Candidate Analysis

**Step 2.1: Comprehensive Column Discovery**
For EVERY filter/condition term in the query:
- Show ALL columns from ALL tables that could match the term
- Include typical_values for each candidate column
- Score each candidate:
  - **HIGH confidence**: Exact match found in typical_values
  - **MEDIUM confidence**: Partial/fuzzy match in typical_values
  - **LOW confidence**: Column name matches but no value match

**Step 2.2: Value Matching and Selection**
- **Prefer Exact Matches**: Always choose columns where typical_values contain exact matches
- **Single Table Preference**: If multiple columns match, prefer those from the same table
- **Show Evidence**: Display the exact value from typical_values that matches
- **NO GUESSING**: Use only values that exist in typical_values
- **MULTIPLE SELECTION**: If multiple columns are relevant for a single query term (e.g., "charter-funded schools" might need both Charter School (Y/N) and Charter Funding Type), SELECT ALL of them

**Step 2.3: Optimal Table Selection**
After analyzing all candidates:
- **Single-Table First**: Can all required columns come from ONE table?
- **Minimize Joins**: Use the fewest tables possible
- **Check Coverage**: Ensure selected tables have all needed columns
- **Validate Values**: Confirm filter values exist in typical_values

**Step 2.4: Final Column Selection**
- Choose columns based on:
  1. Exact value matches in typical_values (highest priority)
  2. Single-table solutions (second priority)
  3. Column name relevance (third priority)
- **IMPORTANT**: Select ALL columns that are relevant to the query, not just the "best" one
- For complex conditions (e.g., "charter-funded schools"), this often means selecting multiple related columns
- Document why each column was selected

**Step 2.5: Handle Retry Issues**
If this is a retry with issues:
- **Zero Results**: Check if filter values match exactly with typical_values
- **Wrong Values**: Use the exact values from <typical_values>, not approximations
- **SQL Errors**: Fix table/column names using exact names from schema
- **Poor Quality**: Address specific evaluation feedback

## Output Format

**CRITICAL: Generate valid XML. Use CDATA for special characters or complex values.**

<schema_linking>
  <available_schema>
    <tables>
      <table name="table_name_here">
        <columns>
          <column name="column_name_here" type="data_type_here" sample_values="list_of_values_here"/>
        </columns>
      </table>
    </tables>
  </available_schema>
  
  <column_discovery>
    <query_term original="user_search_term_here">
      <all_candidates>
        <candidate table="table1_name" column="column1_name" confidence="high">
          <typical_values><![CDATA[['value1', 'value2', 'value3']]]></typical_values>
          <exact_match_value>exact_matching_value_if_found</exact_match_value>
          <reason>Exact match found in typical_values</reason>
        </candidate>
        <candidate table="table2_name" column="column2_name" confidence="medium">
          <typical_values><![CDATA[[100, 200, 300]]]></typical_values>
          <partial_match_value>partial_matching_value</partial_match_value>
          <reason>Partial match or column name similarity</reason>
        </candidate>
      </all_candidates>
      <selected_columns>
        <column table="table1_name" column="column1_name">
          <exact_value>exact_value_from_typical_values</exact_value>
          <reason>Selected because of exact value match and single-table solution possible</reason>
        </column>
        <column table="table2_name" column="column2_name">
          <exact_value>exact_value_from_typical_values</exact_value>
          <reason>Additional relevant column for the same query term</reason>
        </column>
      </selected_columns>
    </query_term>
  </column_discovery>
  
  <single_table_analysis>
    <possible>true or false</possible>
    <best_single_table>table_name_if_possible</best_single_table>
    <reason>why_single_table_works_or_not</reason>
  </single_table_analysis>
  
  <selected_tables>
    <table name="table_name_here" alias="alias_here">
      <purpose>why_this_table_is_needed</purpose>
      <single_table_solution>true or false</single_table_solution>
      <columns>
        <column name="column_name_here" used_for="select or filter or join or group or order or aggregate">
          <reason>why_this_column_is_needed</reason>
        </column>
      </columns>
    </table>
  </selected_tables>
  
  <joins>
    <join>
      <from_table>table1</from_table>
      <from_column>column1</from_column>
      <to_table>table2</to_table>
      <to_column>column2</to_column>
      <join_type>INNER or LEFT or RIGHT or FULL</join_type>
    </join>
  </joins>
</schema_linking>

## Context Analysis
Examine the provided context:
- **current_node**: Intent, existing mapping, sql, executionResult, status
- **node_history**: Previous operations and their outcomes  
- **sql_evaluation_analysis**: Quality assessment and improvement suggestions
- **full_schema**: Complete database schema with sample data

For retries, explain what changed and why the new approach should work."""
    
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
    
