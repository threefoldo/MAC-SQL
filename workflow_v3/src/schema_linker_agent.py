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
        if "selected_tables" in linking_result and linking_result["selected_tables"]:
            selected_tables = linking_result["selected_tables"]
            if isinstance(selected_tables, dict) and "table" in selected_tables:
                tables = selected_tables["table"]
                if not isinstance(tables, list):
                    tables = [tables] if tables else []
                
                if tables:
                    self.logger.info(f"Linked {len(tables)} table(s):")
                    for table in tables:
                        if isinstance(table, dict):
                            self.logger.info(f"  - {table.get('name', 'unknown')}: {table.get('purpose', '')}")
        
        # Log joins if present
        if "joins" in linking_result and linking_result["joins"]:
            joins_data = linking_result["joins"]
            joins = []
            if isinstance(joins_data, dict) and "join" in joins_data:
                joins = joins_data["join"]
                if not isinstance(joins, list):
                    joins = [joins] if joins else []
            elif isinstance(joins_data, list):
                joins = joins_data
            
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
        """Read context from current node and parent if needed - NODE-FOCUSED VERSION"""
        # 1. OPERATE ON CURRENT NODE - Get current node information
        current_node_id = await self.tree_manager.get_current_node_id()
        if not current_node_id:
            self.logger.error("No current node - SchemaLinker requires a current node")
            return {"error": "No current node available"}
        
        current_node = await self.tree_manager.get_node(current_node_id)
        if not current_node:
            self.logger.error(f"Current node {current_node_id} not found")
            return {"error": f"Current node {current_node_id} not found"}
        
        # 2. READ PAST EXECUTION INFORMATION from current node
        previous_schema_analysis = None
        if hasattr(current_node, 'schema_linking') and current_node.schema_linking:
            previous_schema_analysis = current_node.schema_linking
            self.logger.info("Found previous schema linking in current node - this is a re-linking")
        
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
                self.logger.error("No task context available")
                return {"error": "Task context not initialized"}
        
        # 4. GET SCHEMA INFORMATION - from current node, parent, or database (SchemaLinker can access database)
        existing_schema_info = None
        
        # Check current node for schema information
        if hasattr(current_node, 'schema_linking') and current_node.schema_linking:
            existing_schema_info = current_node.schema_linking
            self.logger.info("Found existing schema information in current node")
        # Check parent node for schema information
        elif parent_node and hasattr(parent_node, 'schema_linking') and parent_node.schema_linking:
            existing_schema_info = parent_node.schema_linking
            self.logger.info("Found existing schema information in parent node")
        
        # SchemaLinker can always read directly from database
        full_schema = await self._get_full_schema_xml()
        
        # 5. GET QUERY ANALYSIS INFORMATION - from current node or parent
        query_analysis_context = None
        if hasattr(current_node, 'queryAnalysis') and current_node.queryAnalysis:
            query_analysis_context = current_node.queryAnalysis
            self.logger.info("Found query analysis in current node")
        elif parent_node and hasattr(parent_node, 'queryAnalysis') and parent_node.queryAnalysis:
            query_analysis_context = parent_node.queryAnalysis
            self.logger.info("Found query analysis in parent node")
        
        # 6. CHECK FOR OTHER EXECUTION RESULTS in the tree for context
        sql_generation_context = []
        evaluation_feedback = []
        try:
            tree_data = await self.tree_manager.get_tree()
            for node_id, node_data in tree_data.get("nodes", {}).items():
                # Collect SQL generation results with schema usage
                sql_found = False
                sql_info = {"node_id": node_id, "intent": node_data.get("intent")}
                
                if node_data.get("generation") and node_data["generation"].get("sql"):
                    sql_info.update({
                        "sql": node_data["generation"]["sql"],
                        "explanation": node_data["generation"].get("explanation")
                    })
                    sql_found = True
                elif node_data.get("sql"):
                    sql_info.update({
                        "sql": node_data["sql"],
                        "explanation": node_data.get("sqlExplanation")
                    })
                    sql_found = True
                
                if sql_found:
                    sql_info["schema_used"] = node_data.get("schema_linking")
                    sql_generation_context.append(sql_info)
                
                # Collect schema-related evaluation feedback
                if node_data.get("evaluation"):
                    eval_data = node_data["evaluation"]
                    if any(keyword in str(eval_data).lower() for keyword in ["table", "column", "join", "schema"]):
                        evaluation_feedback.append({
                            "node_id": node_id,
                            "intent": node_data.get("intent"),
                            "evaluation": eval_data,
                            "schema_issues": True
                        })
        except Exception as e:
            self.logger.debug(f"Could not scan tree for execution results: {e}")
        
        # 7. ANALYZE PATTERNS for specialized schema linking
        range_analysis = self._detect_range_patterns(query) if query else None
        if range_analysis:
            self.logger.warning(f"Range pattern detected: {range_analysis['warning']}")
        
        geo_analysis = self._detect_geographic_granularity(query) if query else None
        if geo_analysis:
            self.logger.info(f"Geographic pattern detected: {geo_analysis['description']} - {geo_analysis['suggested_filter']}")
        
        table_priorities = self._get_table_source_priorities()
        
        # Build context
        context = {
            "original_query": query,
            "database_name": database_name,
            "evidence": evidence,
            "full_schema": full_schema,
            "current_node": json.dumps(current_node.to_dict(), indent=2)
        }
        
        # Add optional context
        if existing_schema_info:
            context["previous_schema_linking"] = json.dumps(existing_schema_info, indent=2)
        if query_analysis_context:
            context["query_analysis_results"] = json.dumps(query_analysis_context, indent=2)
        if sql_generation_context:
            context["sql_generation_context"] = json.dumps(sql_generation_context, indent=2)
        if evaluation_feedback:
            context["evaluation_feedback"] = json.dumps(evaluation_feedback, indent=2)
        if range_analysis:
            context["range_pattern_analysis"] = json.dumps(range_analysis, indent=2)
        if geo_analysis:
            context["geographic_analysis"] = json.dumps(geo_analysis, indent=2)
        
        context["table_source_priorities"] = json.dumps(table_priorities, indent=2)
        
        # Get node operation history
        history = await self.history_manager.get_node_operations(current_node_id)
        if history:
            context["node_history"] = json.dumps([op.to_dict() for op in history], indent=2)
        
        # Get parent and sibling info if available
        if parent_node:
            context["parent_node"] = json.dumps(parent_node.to_dict(), indent=2)
            try:
                siblings = await self.tree_manager.get_children(current_node.parentId)
                siblings_info = [s.to_dict() for s in siblings if s.nodeId != current_node_id]
                if siblings_info:
                    context["sibling_nodes"] = json.dumps(siblings_info, indent=2)
            except Exception as e:
                self.logger.debug(f"Could not get sibling information: {e}")
        
        # Get evaluation analysis from current node
        if current_node.evaluation:
            context["sql_evaluation_analysis"] = json.dumps(current_node.evaluation, indent=2)
        
        self.logger.info(f"Schema linker operating on node: {current_node_id}")
        self.logger.info(f"Query: {query}")
        self.logger.info(f"Database: {database_name}")
        if previous_schema_analysis:
            self.logger.info("This is a re-linking - using previous analysis as context")
        
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
                # Validate column references before storing
                linking_result = await self._validate_column_references(linking_result)
                
                # Schema linking results are stored in the node, not in separate memory
                
                # Get the current node ID from QueryTreeManager
                node_id = await self.tree_manager.get_current_node_id()
                
                if node_id:
                    # Store the entire linking result in the node's schema_linking field ONLY
                    await self.tree_manager.update_node(node_id, {"schema_linking": linking_result})
                    self.logger.info(f"Stored schema linking result in query tree node {node_id}")
                    
                    # NO direct memory access - agents only save data in current node
                    
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
                    column_discovery = linking_result.get("column_discovery")
                    if column_discovery:
                        self.logger.info("Column Discovery Process:")
                        self.logger.info(f"  {column_discovery}")
                    
                    # Log single-table analysis
                    single_table_analysis = linking_result.get("single_table_analysis", {})
                    if isinstance(single_table_analysis, dict):
                        single_table_possible = single_table_analysis.get("possible")
                        if single_table_possible is not None:
                            if str(single_table_possible).lower() in ["true", "yes"]:
                                best_table = single_table_analysis.get("best_single_table", "")
                                self.logger.info(f"✓ Single-table solution POSSIBLE using table: {best_table}")
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
            xml_parts.append(f"  <description>{description}</description>")
        
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
                            # Use XML entity escaping for special characters in values
                            xml_parts.append(f'            <value>{html.escape(str(value))}</value>')
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
        """Parse the schema linking XML output using hybrid approach with robust fallback"""
        try:
            # Use the hybrid parsing utility
            parsed = parse_xml_hybrid(output, 'schema_linking')
            if parsed:
                # Validate and clean the parsed result
                return self._validate_and_clean_linking_result(parsed)
            
            # If XML parsing failed, try fallback extraction
            return self._extract_linking_fallback(output)
            
        except Exception as e:
            self.logger.error(f"Error parsing schema linking XML: {str(e)}", exc_info=True)
            # Try fallback extraction even on exceptions
            return self._extract_linking_fallback(output)
    
    def _validate_and_clean_linking_result(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean parsed schema linking result"""
        try:
            # Ensure essential sections exist
            if not parsed.get("selected_tables"):
                self.logger.warning("No selected_tables found in schema linking output")
                # Try to create minimal structure
                parsed["selected_tables"] = {}
            
            # Validate selected_tables structure
            selected_tables = parsed.get("selected_tables")
            if isinstance(selected_tables, dict) and "table" in selected_tables:
                # Ensure table data is in list format
                table_data = selected_tables["table"]
                if not isinstance(table_data, list):
                    if isinstance(table_data, dict):
                        selected_tables["table"] = [table_data]
                    else:
                        self.logger.warning(f"Unexpected table data format: {type(table_data)}")
                        selected_tables["table"] = []
            
            # Set default values for missing fields
            parsed.setdefault("joins", [])
            parsed.setdefault("column_discovery", "")
            parsed.setdefault("single_table_analysis", {})
            
            return parsed
            
        except Exception as e:
            self.logger.warning(f"Error validating linking result: {str(e)}")
            return parsed  # Return as-is if validation fails
    
    def _extract_linking_fallback(self, output: str) -> Optional[Dict[str, Any]]:
        """Fallback extraction when XML parsing fails completely"""
        try:
            # Try to extract minimal table information using regex
            table_matches = re.findall(r'<table name="([^"]+)"', output)
            if table_matches:
                fallback_result = {
                    "selected_tables": {
                        "table": [{"name": name} for name in table_matches]
                    },
                    "joins": [],
                    "column_discovery": "Extracted using fallback method",
                    "single_table_analysis": {}
                }
                self.logger.info(f"Fallback extraction found {len(table_matches)} tables")
                return fallback_result
            
            self.logger.warning("No usable schema linking information found in output")
            return None
            
        except Exception as e:
            self.logger.error(f"Error in fallback linking extraction: {str(e)}")
            return None
    
    async def _validate_column_references(self, linking_result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that all column references exist in the schema"""
        if not linking_result or "selected_tables" not in linking_result:
            return linking_result
        
        try:
            # Get all tables from schema
            all_tables = await self.schema_manager.get_all_tables()
            validation_errors = []
            
            # Validate selected tables
            selected_tables = linking_result.get("selected_tables", {})
            if isinstance(selected_tables, dict) and "table" in selected_tables:
                tables = selected_tables["table"]
                if not isinstance(tables, list):
                    tables = [tables] if tables else []
                
                for table_info in tables:
                    if not isinstance(table_info, dict):
                        continue
                        
                    table_name = table_info.get("name")
                    if not table_name:
                        continue
                    
                    # Check if table exists
                    if table_name not in all_tables:
                        validation_errors.append(f"Table '{table_name}' does not exist in schema")
                        continue
                    
                    # Validate columns in this table
                    schema_table = all_tables[table_name]
                    table_columns = table_info.get("columns", {})
                    
                    if isinstance(table_columns, dict) and "column" in table_columns:
                        columns = table_columns["column"]
                        if not isinstance(columns, list):
                            columns = [columns] if columns else []
                        
                        for col_info in columns:
                            if isinstance(col_info, dict):
                                col_name = col_info.get("name")
                                if col_name and col_name not in schema_table.columns:
                                    validation_errors.append(f"Column '{col_name}' does not exist in table '{table_name}'")
                                    self.logger.warning(f"Invalid column reference: {table_name}.{col_name}")
            
            # Log validation results
            if validation_errors:
                self.logger.error(f"Schema validation errors found: {validation_errors}")
                # Add validation errors to result for downstream agents
                linking_result["validation_errors"] = validation_errors
            else:
                self.logger.info("✓ All column references validated successfully")
                linking_result["validation_status"] = "passed"
            
            return linking_result
            
        except Exception as e:
            self.logger.error(f"Error validating column references: {str(e)}", exc_info=True)
            return linking_result
    
    def _detect_range_patterns(self, query_intent: str) -> Optional[Dict[str, Any]]:
        """Detect range patterns in query intent (CRITICAL FIX for Example 21)"""
        try:
            intent_lower = query_intent.lower()
            
            # Range patterns that should map to same column
            range_patterns = [
                # "more than X but less than Y" patterns
                r'more\s+than\s+(\d+).*?(?:but|and).*?less\s+than\s+(\d+)',
                r'greater\s+than\s+(\d+).*?(?:but|and).*?(?:less|under)\s+than\s+(\d+)',
                r'between\s+(\d+)\s+and\s+(\d+)',
                r'from\s+(\d+)\s+to\s+(\d+)',
                # "exceeding X" patterns
                r'exceeding\s+(\d+)',
                r'above\s+(\d+)',
            ]
            
            for pattern in range_patterns:
                match = re.search(pattern, intent_lower)
                if match:
                    # Extract the attribute being ranged
                    # Look for the subject before the range
                    before_range = intent_lower[:match.start()]
                    
                    # Common attributes that get ranged
                    range_subjects = {
                        'free meal': ['Free Meal Count', 'free meal count'],
                        'reduced price meal': ['Free Meal Count', 'free meal count'],  # Often same as free meal
                        'frpm': ['FRPM Count'],
                        'enrollment': ['Enrollment'],
                        'meal': ['Free Meal Count', 'FRPM Count'],
                        'student': ['Enrollment'],
                    }
                    
                    detected_subject = None
                    for subject, columns in range_subjects.items():
                        if subject in before_range:
                            detected_subject = subject
                            break
                    
                    if detected_subject:
                        return {
                            'has_range_pattern': True,
                            'range_subject': detected_subject,
                            'suggested_columns': range_subjects[detected_subject],
                            'pattern_matched': pattern,
                            'range_values': match.groups(),
                            'warning': f"Range pattern detected for '{detected_subject}' - ensure both conditions use same column"
                        }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error detecting range patterns: {str(e)}", exc_info=True)
            return None
    
    def _get_table_source_priorities(self) -> Dict[str, Dict[str, Any]]:
        """Get table source priority rules (CRITICAL FIX for Example 25)"""
        return {
            "funding_type": {
                "primary_sources": ["frpm"],
                "column_mappings": {
                    "frpm": "Charter Funding Type",
                    "schools": "FundingType"
                },
                "decision_rule": "prefer_frpm_for_funding_attributes",
                "context_hints": ["funding", "charter", "finance"]
            },
            "school_names": {
                "primary_sources": ["frpm", "schools"],
                "column_mappings": {
                    "frpm": "School Name",
                    "schools": "School",
                    "satscores": "sname"
                },
                "decision_rule": "prefer_canonical_school_table",
                "context_hints": ["name", "school"]
            },
            "enrollment_data": {
                "primary_sources": ["frpm"],
                "column_mappings": {
                    "frpm": "Enrollment (K-12)",
                    "satscores": "enroll12"
                },
                "decision_rule": "prefer_frpm_for_comprehensive_enrollment",
                "context_hints": ["enrollment", "students", "k-12"]
            }
        }
    
    def _detect_geographic_granularity(self, query_intent: str) -> Optional[Dict[str, Any]]:
        """Detect geographic granularity needs (CRITICAL FIX for Example 25)"""
        try:
            intent_lower = query_intent.lower()
            
            # Geographic patterns
            geographic_patterns = {
                "district_level": {
                    "patterns": [
                        r'schools? in (\w+)',
                        r'(\w+) schools?',
                        r'in (\w+) which',
                    ],
                    "filter_format": "District Name LIKE '{location}%'",
                    "description": "District-level filtering for school queries"
                },
                "county_level": {
                    "patterns": [
                        r'(\w+) county',
                        r'county of (\w+)',
                    ],
                    "filter_format": "County Name = '{location}'", 
                    "description": "County-level filtering"
                },
                "city_level": {
                    "patterns": [
                        r'city of (\w+)',
                        r'(\w+) city schools?',
                    ],
                    "filter_format": "City = '{location}'",
                    "description": "City-level filtering"
                }
            }
            
            for level, config in geographic_patterns.items():
                for pattern in config["patterns"]:
                    match = re.search(pattern, intent_lower)
                    if match:
                        location = match.group(1).title()  # Capitalize
                        return {
                            "geographic_level": level,
                            "location": location,
                            "suggested_filter": config["filter_format"].format(location=location),
                            "description": config["description"],
                            "pattern_matched": pattern
                        }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error detecting geographic granularity: {str(e)}", exc_info=True)
            return None
    
