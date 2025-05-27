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
from memory_content_types import (
    QueryNode, QueryMapping, TableMapping, ColumnMapping, JoinMapping,
    TableSchema, ColumnInfo, NodeStatus
)
from prompts import SQL_CONSTRAINTS


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
        self.schema_manager = DatabaseSchemaManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
    
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
Use a two-phase approach to link schema elements to the query:

### Phase 1: Discovery and Validation

**Step 1.1: Available Schema Elements**
Before making any selections, list out:
- All available table names from the schema
- For each table, list ALL column names with their typical values
- Identify foreign key relationships

**Step 1.2: Comprehensive Column Search**
For EVERY filter/condition term in the query:
- Search ALL tables for columns that could match
- Check typical_values in EVERY column across ALL tables
- Look for exact matches in typical_values first
- Rank candidates by how well their typical_values match the query terms

**Step 1.3: Query Analysis**
- Analyze the node's intent for required data elements
- Check if this is a retry (look for previous sql/executionResult/analysis)
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

## SQL Constraints to Consider
{SQL_CONSTRAINTS}

## Output Format

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
          <typical_values>actual_values_from_this_column</typical_values>
          <exact_match_value>exact_matching_value_if_found</exact_match_value>
          <reason>Exact match found in typical_values</reason>
        </candidate>
        <candidate table="table2_name" column="column2_name" confidence="medium">
          <typical_values>actual_values_from_this_column</typical_values>
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
  
  <sample_query_pattern>
    Brief SQL pattern showing how these elements would be used
  </sample_query_pattern>
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
        
        # Get parent node if exists
        parent_info = None
        if node.parentId:
            parent_node = await self.tree_manager.get_node(node.parentId)
            if parent_node:
                parent_info = parent_node.to_dict()
        
        # Get sibling nodes if exists
        siblings_info = []
        if node.parentId:
            siblings = await self.tree_manager.get_children(node.parentId)
            siblings_info = [s.to_dict() for s in siblings if s.nodeId != current_node_id]
        
        # Get node operation history
        history = await self.history_manager.get_node_operations(current_node_id)
        # Convert NodeOperation objects to dictionaries
        history_dicts = [op.to_dict() for op in history] if history else []
        
        # Get SQL evaluation analysis if available
        analysis_key = f"node_{current_node_id}_analysis"
        evaluation_analysis = await self.memory.get(analysis_key)
        
        # Build context with ALL node information - let the prompt guide how to use it
        context = {
            "current_node": json.dumps(node_dict, indent=2),
            "parent_node": json.dumps(parent_info, indent=2) if parent_info else None,
            "sibling_nodes": json.dumps(siblings_info, indent=2) if siblings_info else None,
            "node_history": json.dumps(history_dicts, indent=2) if history_dicts else None,  # ALL history
            "sql_evaluation_analysis": json.dumps(evaluation_analysis, indent=2) if evaluation_analysis else None,
            "full_schema": await self._get_full_schema_xml()
        }
        
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
        self.logger.info(f"Raw LLM output (first 500 chars): {last_message[:500]}")
        
        try:
            # Parse the XML output
            linking_result = self._parse_linking_xml(last_message)
            
            if linking_result:
                # Create mapping from the linking result
                mapping = await self._create_mapping_from_linking(linking_result)
                
                # Get the current node ID from QueryTreeManager
                node_id = await self.tree_manager.get_current_node_id()
                
                if node_id:
                    # Update the node with the mapping
                    await self.tree_manager.update_node_mapping(node_id, mapping)
                    
                    # Record in history
                    await self.history_manager.record_revise(
                        node_id=node_id,
                        new_mapping=mapping
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
                        for term, discovery in linking_result["column_discovery"].items():
                            self.logger.info(f"  Term: '{term}'")
                            if discovery.get("candidates"):
                                self.logger.info(f"    Candidates found across ALL tables: {len(discovery['candidates'])}")
                                # Group by confidence level
                                high_conf = [c for c in discovery["candidates"] if c.get("confidence") == "high"]
                                med_conf = [c for c in discovery["candidates"] if c.get("confidence") == "medium"]
                                low_conf = [c for c in discovery["candidates"] if c.get("confidence") == "low"]
                                
                                if high_conf:
                                    self.logger.info("    HIGH confidence matches (exact value match):")
                                    for candidate in high_conf[:3]:  # Show top 3
                                        exact_val = candidate.get("exact_match_value", "")
                                        self.logger.info(f"      ✓ {candidate['table']}.{candidate['column']} = '{exact_val}'")
                                
                                if med_conf:
                                    self.logger.info(f"    MEDIUM confidence matches: {len(med_conf)} found")
                                
                                if low_conf:
                                    self.logger.info(f"    LOW confidence matches: {len(low_conf)} found")
                                    
                            if discovery.get("selected"):
                                selected_list = discovery["selected"]
                                # Handle both list and single dict formats
                                if not isinstance(selected_list, list):
                                    selected_list = [selected_list] if selected_list else []
                                
                                if len(selected_list) > 1:
                                    self.logger.info(f"    → SELECTED {len(selected_list)} COLUMNS:")
                                    for sel in selected_list:
                                        if sel.get("exact_value"):
                                            # Strip quotes for display
                                            display_value = sel['exact_value']
                                            if display_value and len(display_value) >= 2:
                                                if (display_value.startswith("'") and display_value.endswith("'")) or \
                                                   (display_value.startswith('"') and display_value.endswith('"')):
                                                    display_value = display_value[1:-1]
                                            self.logger.info(f"        • {sel['table']}.{sel['column']} = '{display_value}'")
                                        else:
                                            self.logger.info(f"        • {sel['table']}.{sel['column']}")
                                elif selected_list:
                                    sel = selected_list[0]
                                    if sel.get("exact_value"):
                                        # Strip quotes for display
                                        display_value = sel['exact_value']
                                        if display_value and len(display_value) >= 2:
                                            if (display_value.startswith("'") and display_value.endswith("'")) or \
                                               (display_value.startswith('"') and display_value.endswith('"')):
                                                display_value = display_value[1:-1]
                                        self.logger.info(f"    → SELECTED: {sel['table']}.{sel['column']} = '{display_value}'")
                                    else:
                                        self.logger.info(f"    → SELECTED: {sel['table']}.{sel['column']}")
                    
                    # Log single-table analysis
                    if linking_result.get("single_table_possible") is not None:
                        if linking_result.get("single_table_possible"):
                            best_table = linking_result.get("best_single_table", "")
                            self.logger.info(f"✓ Single-table solution POSSIBLE using table: {best_table}")
                        else:
                            self.logger.info("✗ Single-table solution NOT possible - multiple tables required")
                    
                    # Log single table solution status
                    if linking_result.get("single_table_solution"):
                        self.logger.info("✓ Single-table solution SELECTED")
                    
                    # Log linked tables
                    if mapping.tables:
                        self.logger.info(f"Linked {len(mapping.tables)} table(s):")
                        for table in mapping.tables:
                            self.logger.info(f"  - {table.name}: {table.purpose}")
                    
                    # Log selected columns
                    if mapping.columns:
                        self.logger.info(f"Selected {len(mapping.columns)} column(s):")
                        # Group columns by table
                        cols_by_table = {}
                        for col in mapping.columns:
                            if col.table not in cols_by_table:
                                cols_by_table[col.table] = []
                            cols_by_table[col.table].append(col)
                        
                        for table, cols in cols_by_table.items():
                            self.logger.info(f"  From {table}:")
                            for col in cols:
                                self.logger.info(f"    - {col.column} (used for: {col.usedFor})")
                    
                    # Log joins
                    if mapping.joins:
                        self.logger.info(f"Identified {len(mapping.joins)} join(s):")
                        for join in mapping.joins:
                            self.logger.info(f"  - {join.from_table} -> {join.to}: {join.on}")
                    
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
        """Parse the schema linking XML output with enhanced structure"""
        try:
            # Extract XML from output
            xml_match = re.search(r'<schema_linking>.*?</schema_linking>', output, re.DOTALL)
            if not xml_match:
                # Try to find XML in code blocks
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
                    self.logger.error("No schema linking XML found in output")
                    return None
            else:
                xml_content = xml_match.group()
            
            # Try to parse XML
            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError as e:
                self.logger.warning(f"XML parsing failed: {str(e)}")
                self.logger.info("Attempting fallback parsing with regex...")
                # Fallback to regex-based parsing for key elements
                return self._fallback_parse_linking(xml_content)
            
            result = {
                "tables": [],
                "joins": [],
                "sample_query": root.findtext("sample_query_pattern", "").strip(),
                "column_discovery": {},
                "single_table_solution": False
            }
            
            # Parse column discovery section for enhanced logging
            discovery_elem = root.find("column_discovery")
            if discovery_elem is not None:
                for term_elem in discovery_elem.findall("query_term"):
                    original_term = term_elem.get("original", "")
                    candidates = []
                    
                    # Parse all_candidates for the new structure
                    all_candidates_elem = term_elem.find("all_candidates")
                    if all_candidates_elem is not None:
                        for candidate in all_candidates_elem.findall("candidate"):
                            candidates.append({
                                "table": candidate.get("table"),
                                "column": candidate.get("column"),
                                "typical_values": candidate.findtext("typical_values", "").strip(),
                                "exact_match_value": candidate.findtext("exact_match_value", "").strip(),
                                "confidence": candidate.get("confidence", "medium"),
                                "reason": candidate.findtext("reason", "").strip()
                            })
                    else:
                        # Fallback to old structure
                        candidates_elem = term_elem.find("candidates")
                        if candidates_elem is not None:
                            for candidate in candidates_elem.findall("candidate"):
                                candidates.append({
                                    "table": candidate.get("table"),
                                    "column": candidate.get("column"),
                                    "sample_values": candidate.get("sample_values", ""),
                                    "confidence": candidate.get("confidence", "medium"),
                                    "reason": candidate.findtext("reason", "").strip()
                                })
                    
                    # Handle both old single-selected and new multi-selected formats
                    selected_list = []
                    
                    # Try new format with selected_columns
                    selected_columns_elem = term_elem.find("selected_columns")
                    if selected_columns_elem is not None:
                        for col_elem in selected_columns_elem.findall("column"):
                            selected_list.append({
                                "table": col_elem.get("table"),
                                "column": col_elem.get("column"),
                                "exact_value": col_elem.findtext("exact_value", "").strip(),
                                "reason": col_elem.findtext("reason", "").strip()
                            })
                    else:
                        # Fallback to old single-selected format
                        selected_elem = term_elem.find("selected")
                        if selected_elem is not None:
                            selected_list.append({
                                "table": selected_elem.get("table"),
                                "column": selected_elem.get("column"),
                                "exact_value": selected_elem.findtext("exact_value", "").strip(),
                                "reason": selected_elem.findtext("reason", "").strip()
                            })
                    
                    result["column_discovery"][original_term] = {
                        "candidates": candidates,
                        "selected": selected_list  # Now a list instead of single item
                    }
            
            # Parse single-table analysis
            single_table_elem = root.find("single_table_analysis")
            if single_table_elem is not None:
                result["single_table_possible"] = single_table_elem.findtext("possible", "false").lower() == "true"
                result["best_single_table"] = single_table_elem.findtext("best_single_table", "").strip()
                if result["single_table_possible"]:
                    result["single_table_solution"] = True
            
            # Parse selected tables
            tables_elem = root.find("selected_tables")
            if tables_elem is not None:
                for table_elem in tables_elem.findall("table"):
                    # Check if this is marked as single table solution
                    single_table = table_elem.findtext("single_table_solution", "false").lower() == "true"
                    if single_table:
                        result["single_table_solution"] = True
                    
                    table_info = {
                        "name": table_elem.get("name"),
                        "alias": table_elem.get("alias", ""),
                        "purpose": table_elem.findtext("purpose", "").strip(),
                        "columns": []
                    }
                    
                    # Parse columns
                    columns_elem = table_elem.find("columns")
                    if columns_elem is not None:
                        for col_elem in columns_elem.findall("column"):
                            column_info = {
                                "name": col_elem.get("name"),
                                "used_for": col_elem.get("used_for", "select"),
                                "reason": col_elem.findtext("reason", "").strip()
                            }
                            table_info["columns"].append(column_info)
                    
                    result["tables"].append(table_info)
            
            # Parse joins
            joins_elem = root.find("joins")
            if joins_elem is not None:
                for join_elem in joins_elem.findall("join"):
                    join_info = {
                        "from_table": join_elem.findtext("from_table", "").strip(),
                        "from_column": join_elem.findtext("from_column", "").strip(),
                        "to_table": join_elem.findtext("to_table", "").strip(),
                        "to_column": join_elem.findtext("to_column", "").strip(),
                        "join_type": join_elem.findtext("join_type", "INNER").strip()
                    }
                    result["joins"].append(join_info)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing schema linking XML: {str(e)}", exc_info=True)
            return None
    
    async def _create_mapping_from_linking(self, linking_result: Dict[str, Any]) -> QueryMapping:
        """Create QueryMapping from schema linking result"""
        mapping = QueryMapping()
        
        # Build a lookup for exact values from column discovery
        exact_values_lookup = {}
        column_discovery = linking_result.get("column_discovery", {})
        for term, discovery in column_discovery.items():
            selected_list = discovery.get("selected", [])
            # Handle both list and single dict formats
            if not isinstance(selected_list, list):
                selected_list = [selected_list] if selected_list else []
            
            # Process all selected columns for this term
            for selected in selected_list:
                if selected and selected.get("exact_value"):
                    table = selected.get("table")
                    column = selected.get("column")
                    if table and column:
                        key = f"{table}.{column}"
                        exact_value = selected.get("exact_value")
                        # Strip surrounding quotes if present (typical values often include quotes)
                        if exact_value and len(exact_value) >= 2:
                            if (exact_value.startswith("'") and exact_value.endswith("'")) or \
                               (exact_value.startswith('"') and exact_value.endswith('"')):
                                exact_value = exact_value[1:-1]
                        exact_values_lookup[key] = exact_value
        
        # Add tables
        for table_info in linking_result.get("tables", []):
            table_mapping = TableMapping(
                name=table_info["name"],
                alias=table_info.get("alias", ""),
                purpose=table_info.get("purpose", "")
            )
            mapping.tables.append(table_mapping)
            
            # Add columns
            for col_info in table_info.get("columns", []):
                # Look up exact value for this column
                lookup_key = f"{table_info['name']}.{col_info['name']}"
                exact_value = exact_values_lookup.get(lookup_key)
                
                # Get data type from schema
                data_type = None
                column_info = await self.schema_manager.get_column(table_info["name"], col_info["name"])
                if column_info:
                    data_type = column_info.dataType
                
                column_mapping = ColumnMapping(
                    table=table_info["name"],
                    column=col_info["name"],
                    usedFor=col_info.get("used_for", "select"),
                    exactValue=exact_value,  # Include the exact value if found
                    dataType=data_type  # Include the data type
                )
                mapping.columns.append(column_mapping)
        
        # Add joins
        for join_info in linking_result.get("joins", []):
            # Create join condition string
            from_alias = ""
            to_alias = ""
            
            # Find aliases for tables
            for table in linking_result.get("tables", []):
                if table["name"] == join_info["from_table"] and table.get("alias"):
                    from_alias = table["alias"]
                if table["name"] == join_info["to_table"] and table.get("alias"):
                    to_alias = table["alias"]
            
            # Build join condition
            from_ref = f"{from_alias}.{join_info['from_column']}" if from_alias else f"{join_info['from_table']}.{join_info['from_column']}"
            to_ref = f"{to_alias}.{join_info['to_column']}" if to_alias else f"{join_info['to_table']}.{join_info['to_column']}"
            
            join_mapping = JoinMapping(
                from_table=join_info["from_table"],
                to=join_info["to_table"],
                on=f"{from_ref} = {to_ref}"
            )
            if mapping.joins is None:
                mapping.joins = []
            mapping.joins.append(join_mapping)
        
        return mapping
    
    async def link_schema(self, node_id: str) -> Optional[QueryMapping]:
        """
        Link schema to a specific query node.
        
        Args:
            node_id: The ID of the node to link schema to
            
        Returns:
            The created QueryMapping or None if failed
        """
        self.logger.debug(f"Linking schema for node: {node_id}")
        
        # Set the current node in QueryTreeManager
        await self.tree_manager.set_current_node_id(node_id)
        
        # Run the agent
        task = "Link schema for the current query node"
        result = await self.run(task)
        
        # Get the node to check if it has mapping
        node = await self.tree_manager.get_node(node_id)
        if node and node.mapping:
            return node.mapping
        
        return None
    
    def _fallback_parse_linking(self, xml_content: str) -> Optional[Dict[str, Any]]:
        """Fallback parser using regex when XML parsing fails"""
        self.logger.info("Using fallback regex-based parser")
        
        result = {
            "tables": [],
            "joins": [],
            "column_discovery": {},
            "single_table_solution": False
        }
        
        try:
            # Extract selected tables
            tables_match = re.search(r'<selected_tables>(.*?)</selected_tables>', xml_content, re.DOTALL)
            if tables_match:
                tables_content = tables_match.group(1)
                # Find each table
                for table_match in re.finditer(r'<table\s+name="([^"]+)"\s+alias="([^"]+)"[^>]*>(.*?)</table>', tables_content, re.DOTALL):
                    table_name = table_match.group(1)
                    alias = table_match.group(2)
                    table_content = table_match.group(3)
                    
                    table_info = {
                        "name": table_name,
                        "alias": alias,
                        "columns": []
                    }
                    
                    # Extract columns for this table
                    for col_match in re.finditer(r'<column\s+name="([^"]+)"\s+used_for="([^"]+)"[^>]*>', table_content):
                        col_name = col_match.group(1)
                        used_for = col_match.group(2)
                        table_info["columns"].append({
                            "name": col_name,
                            "used_for": used_for
                        })
                    
                    result["tables"].append(table_info)
            
            # Extract joins
            joins_match = re.search(r'<joins>(.*?)</joins>', xml_content, re.DOTALL)
            if joins_match:
                joins_content = joins_match.group(1)
                for join_match in re.finditer(r'<from_table>([^<]+)</from_table>.*?<from_column>([^<]+)</from_column>.*?<to_table>([^<]+)</to_table>.*?<to_column>([^<]+)</to_column>', joins_content, re.DOTALL):
                    result["joins"].append({
                        "from_table": join_match.group(1).strip(),
                        "from_column": join_match.group(2).strip(),
                        "to_table": join_match.group(3).strip(),
                        "to_column": join_match.group(4).strip(),
                        "join_type": "INNER"  # Default
                    })
            
            # Extract column discovery with exact values
            discovery_match = re.search(r'<column_discovery>(.*?)</column_discovery>', xml_content, re.DOTALL)
            if discovery_match:
                discovery_content = discovery_match.group(1)
                for term_match in re.finditer(r'<query_term\s+original="([^"]+)"[^>]*>(.*?)</query_term>', discovery_content, re.DOTALL):
                    term = term_match.group(1)
                    term_content = term_match.group(2)
                    
                    # Look for selected columns with exact values
                    selected_match = re.search(r'<selected_columns>(.*?)</selected_columns>', term_content, re.DOTALL)
                    if selected_match:
                        selected_content = selected_match.group(1)
                        selected_columns = []
                        
                        for col_match in re.finditer(r'<column\s+table="([^"]+)"\s+column="([^"]+)"[^>]*>.*?<exact_value>([^<]*)</exact_value>', selected_content, re.DOTALL):
                            selected_columns.append({
                                "table": col_match.group(1),
                                "column": col_match.group(2),
                                "exact_value": col_match.group(3).strip()
                            })
                        
                        if selected_columns:
                            result["column_discovery"][term] = {"selected": selected_columns}
            
            # Extract single table analysis
            single_table_match = re.search(r'<single_table_analysis>.*?<possible>([^<]+)</possible>', xml_content, re.DOTALL)
            if single_table_match:
                possible = single_table_match.group(1).strip().lower()
                result["single_table_solution"] = possible == "true"
            
            self.logger.info(f"Fallback parser extracted: {len(result['tables'])} tables, {len(result['joins'])} joins")
            return result
            
        except Exception as e:
            self.logger.error(f"Fallback parser also failed: {str(e)}", exc_info=True)
            return None