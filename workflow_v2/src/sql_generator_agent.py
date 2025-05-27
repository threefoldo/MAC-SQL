"""
SQL Generator Agent for text-to-SQL tree orchestration.

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
from prompts import SQL_CONSTRAINTS, format_refiner_template


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
        return f"""You are an expert SQL query generator for SQLite databases.

## CRITICAL RULES (Must Follow)
1. **USE EXACT NAMES**: Use only table/column names from the provided mapping (CASE-SENSITIVE)
2. **NO GUESSING**: Never modify or invent table/column names
3. **FOLLOW CONSTRAINTS**: Apply all SQL generation constraints systematically
4. **PREFER SIMPLICITY**: Use single-table queries when possible, avoid unnecessary joins

## Your Task
Generate accurate SQL queries based on the provided node information.

### Step 1: Parse Context and Analyze Schema Linking
Extract from the "current_node" JSON:
- **intent**: The query to convert to SQL
- **mapping**: Tables, columns, and joins to use (preferred solution from schema linker)
  - Pay special attention to mapping.columns[].exactValue for filter values
  - **CRITICAL**: Check mapping.columns[].dataType to format values correctly
- **sql**: Previous SQL (if this is a retry)
- **executionResult**: Previous execution results and errors
- **evidence**: Domain-specific knowledge

**Data Type Formatting Guide**:
- INTEGER/INT/BIGINT: Use unquoted numbers (e.g., 1, 42, -10)
- REAL/FLOAT/DOUBLE: Use unquoted decimals (e.g., 3.14, -0.5)
- TEXT/VARCHAR/CHAR: Use single quotes (e.g., 'value', 'John')
- NULL values: Use unquoted NULL keyword

**Check Schema Linking Quality**:
- Look for single_table_solution indicators in mapping
- Verify that selected tables/columns match the intent
- If retry: check if previous failure was due to wrong table/column selection

### Step 2: Determine Scenario and Table Strategy
**New Generation**: No previous sql in the node
- Follow schema linker's table selection (single-table preferred)
- Generate fresh SQL based on intent and mapping

**Retry Generation**: Node has sql and executionResult
- **NEVER generate the same SQL that failed**
- Check if failure was due to wrong schema linking (table/column names)
- If schema issue: trust the updated mapping from schema linker
- Fix specific issues based on error type

**Single-Table Solution**: When mapping suggests single table
- Avoid joins unless absolutely necessary
- Use only the selected table and its columns
- Check that all needed data exists in the single table

### Step 3: Handle Retry Issues (Enhanced)
**CRITICAL for WHERE Clauses**:
- Check mapping.columns for exactValue field - this contains the exact filter value to use
- Use ONLY the exactValue from column mapping when available, never approximate
- If zero results, the issue is likely wrong filter values - check if exactValue is present in mapping

**Schema-Related Errors**:
- "no such table/column" → Use exact names from latest mapping, check schema linker fixes
- "wrong table joins" → Consider if single-table solution is possible
- Wrong filter values → Use exact values from schema linker's column discovery

**SQL Error (executionResult.error exists)**:
- "no such table/column" → Check exact names in mapping
- "ambiguous column" → Add table aliases  
- "syntax error" → Fix SQL syntax
- "division by zero" → Add NULLIF or CASE statements

**Zero Results (rowCount = 0)**:
- FIRST CHECK: Are you using exact values from schema linker for WHERE clauses?
- If schema linker didn't provide exact values, request re-linking
- Only try LIKE or case-insensitive if exact values don't work
- Check if JOINs exclude all records (consider single-table alternative)

**Poor Quality (from sql_evaluation_analysis)**:
- Address each listed issue
- Follow provided suggestions
- Consider if simpler table structure could solve the problem

### Step 4: Generate SQL with Table Preference
**Single-Table Queries** (preferred when possible):
- Direct SELECT from one table with proper WHERE/ORDER BY/GROUP BY
- Use the exact column names from schema linker mapping
- Verify all needed data exists in the single table

**Multi-Table Queries** (only when necessary):
- Use explicit JOIN syntax with table aliases
- Ensure all joins are actually needed for the query
- Follow the join patterns suggested by schema linker

**WHERE Clause Generation**:
- For each column in mapping.columns where usedFor="filter":
  - Check the column's dataType field to determine proper value formatting
  - If exactValue is provided: Format based on dataType
    - INTEGER/INT/BIGINT/SMALLINT: Use unquoted numbers (e.g., WHERE column = 1)
    - REAL/FLOAT/DOUBLE/NUMERIC/DECIMAL: Use unquoted numbers (e.g., WHERE column = 3.14)
    - TEXT/VARCHAR/CHAR: Use quoted strings (e.g., WHERE column = 'value')
    - Other types: Use quoted strings as default
  - If no exactValue: Fall back to LIKE patterns or case-insensitive matching
- **CRITICAL Data Type Rules**:
  - Always check mapping.columns[].dataType for the column's data type
  - For numeric types (INTEGER, REAL, etc.): NEVER use quotes around numbers
  - For text types: ALWAYS use quotes around values
  - If dataType is not provided, check evidence for hints
  - Common mistake: Using WHERE numeric_column = '1' instead of WHERE numeric_column = 1

**SQLite Specifics**: No FULL OUTER JOIN, use CAST(), proper GROUP BY

## SQL Generation Constraints
{SQL_CONSTRAINTS}

## Output Format

<sql_generation>
  <query_type>simple|join|aggregate|subquery|complex</query_type>
  <sql>
    -- Your SQL query here
    -- Example with correct data types:
    -- For INTEGER column: WHERE age = 25 (no quotes)
    -- For TEXT column: WHERE name = 'John' (with quotes)
    SELECT ... FROM ... WHERE ...
  </sql>
  <explanation>
    How the query addresses the intent
  </explanation>
  <considerations>
    - Assumptions made
    - Limitations
    - Changes from previous attempt (if retry)
    - Data type formatting applied (e.g., removed quotes from numeric values)
  </considerations>
</sql_generation>

## SQLite Best Practices
- Use table aliases and qualify all columns
- Include only necessary columns (avoid SELECT *)
- Use JOIN before aggregation functions
- Handle NULLs with IS NOT NULL when needed
- Use CAST for type conversions
- Add GROUP BY before ORDER BY for distinct values

For retries, explain what failed and what you changed."""
    
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
        
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the SQL generation results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        self.logger.info(f"Raw LLM output (first 500 chars): {last_message[:500]}")
        
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
                    
                    # Enhanced user-friendly logging
                    self.logger.info("="*60)
                    self.logger.info("SQL Generation")
                    
                    # Get node for intent and mapping details
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        self.logger.info(f"Query intent: {node.intent}")
                        
                        # Check for single table solution indicators
                        if node.mapping and node.mapping.tables:
                            table_count = len(node.mapping.tables)
                            if table_count == 1:
                                self.logger.info("✓ Single-table solution generated")
                            else:
                                self.logger.info(f"Multi-table solution ({table_count} tables)")
                            
                            # Show table utilization
                            self.logger.info("Table utilization:")
                            for table in node.mapping.tables:
                                table_cols = [c for c in node.mapping.columns if c.table == table.name]
                                self.logger.info(f"  - {table.name}: {len(table_cols)} columns used")
                    
                    # Log query type and complexity
                    if generation_result.get("query_type"):
                        query_type = generation_result['query_type'].upper()
                        self.logger.info(f"Query type: {query_type}")
                    
                    # Check for retry indicators
                    if node and node.sql:
                        self.logger.info("⚠️  Retry generation (previous SQL existed)")
                    
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
                    
                    # Log considerations for retries
                    if generation_result.get("considerations"):
                        self.logger.info(f"Considerations: {generation_result['considerations']}")
                    
                    self.logger.info("="*60)
                    self.logger.info(f"Updated node {node_id} with generated SQL")
                else:
                    self.logger.warning("No node_id found to update with generated SQL")
                
        except Exception as e:
            self.logger.error(f"Error parsing SQL generation results: {str(e)}", exc_info=True)
    
    
    def _parse_generation_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the SQL generation XML output"""
        try:
            # Extract XML - try multiple patterns
            xml_match = re.search(r'<sql_generation>.*?</sql_generation>', output, re.DOTALL)
            if not xml_match:
                # Try code block with xml
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
                    # Try code block with sql_generation
                    xml_match = re.search(r'```sql_generation\s*\n(.*?)\n```', output, re.DOTALL)
                    if xml_match:
                        xml_content = xml_match.group(1)
                    else:
                        # Try any code block that contains sql_generation tags
                        xml_match = re.search(r'```[a-zA-Z_]*\s*\n(.*?<sql_generation>.*?</sql_generation>.*?)\n```', output, re.DOTALL)
                        if xml_match:
                            xml_content = xml_match.group(1)
                        else:
                            # Try to extract individual XML tags separately
                            individual_result = self._extract_individual_tags(output)
                            if individual_result:
                                return individual_result
                            
                            # Final fallback: try to extract SQL directly
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
            
            # Clean up SQL (preserve line structure but remove extra whitespace)
            if result["sql"]:
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
    
    def _extract_individual_tags(self, output: str) -> Optional[Dict[str, Any]]:
        """Extract individual XML tags when full block parsing fails"""
        try:
            # Try to extract individual tags
            query_type_match = re.search(r'<query_type>\s*(.*?)\s*</query_type>', output, re.DOTALL)
            sql_match = re.search(r'<sql>\s*(.*?)\s*</sql>', output, re.DOTALL)
            explanation_match = re.search(r'<explanation>\s*(.*?)\s*</explanation>', output, re.DOTALL)
            considerations_match = re.search(r'<considerations>\s*(.*?)\s*</considerations>', output, re.DOTALL)
            
            # We need at least SQL to be successful
            if not sql_match:
                return None
            
            result = {
                "query_type": query_type_match.group(1).strip() if query_type_match else "unknown",
                "sql": sql_match.group(1).strip(),
                "explanation": explanation_match.group(1).strip() if explanation_match else "Individual tag extraction",
                "considerations": considerations_match.group(1).strip() if considerations_match else ""
            }
            
            # Clean up SQL (same logic as before)
            if result["sql"]:
                sql = result["sql"].strip()
                # Replace multiple whitespace with single space, but preserve line breaks if they exist
                sql = re.sub(r'[ \t]+', ' ', sql)  # Replace multiple spaces/tabs with single space
                sql = re.sub(r' *\n *', '\n', sql)  # Clean around line breaks
                result["sql"] = sql
            
            self.logger.info("Successfully extracted individual XML tags")
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting individual tags: {str(e)}", exc_info=True)
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
    
    async def refine_sql(self, node_id: str, error_message: str, exception_class: str = "SQLException") -> Optional[str]:
        """
        Refine SQL that encountered an error using the proven refiner template.
        
        Args:
            node_id: The ID of the node with failed SQL
            error_message: The error message from SQLite
            exception_class: The exception class name
            
        Returns:
            The refined SQL or None if failed
        """
        # Get the node
        node = await self.tree_manager.get_node(node_id)
        if not node or not node.sql:
            self.logger.error(f"No node or SQL found for refinement: {node_id}")
            return None
        
        # Get evidence from the node or root node
        evidence = node.evidence
        if not evidence and node.parentId:
            root_node = await self.tree_manager.get_root_node()
            if root_node:
                evidence = root_node.evidence
        
        # Get schema information
        schema_xml = await self._get_schema_xml()
        
        # Extract schema description and foreign keys (simplified for now)
        desc_str = schema_xml
        fk_str = ""  # Would need to extract from schema_xml
        
        # Format the refiner prompt
        refiner_prompt = format_refiner_template(
            query=node.intent,
            evidence=evidence or "",
            desc_str=desc_str,
            fk_str=fk_str,
            sql=node.sql,
            sqlite_error=error_message,
            exception_class=exception_class
        )
        
        # Log the refinement attempt
        self.logger.info("="*60)
        self.logger.info("SQL Refinement")
        self.logger.info(f"Refining SQL for node: {node_id}")
        self.logger.info(f"Error: {error_message}")
        self.logger.info("="*60)
        
        # Set the current node for context
        await self.tree_manager.set_current_node_id(node_id)
        
        # Store the refiner prompt in memory for the agent to use
        await self.memory.set(f"node_{node_id}_refiner_prompt", refiner_prompt)
        
        # Run the refinement
        task = "Refine the SQL query that encountered an error using the refiner prompt"
        result = await self.run(task)
        
        # Clean up
        await self.memory.delete(f"node_{node_id}_refiner_prompt")
        
        # Get the refined SQL
        node = await self.tree_manager.get_node(node_id)
        if node and node.sql:
            self.logger.info("SQL successfully refined")
            return node.sql
        
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