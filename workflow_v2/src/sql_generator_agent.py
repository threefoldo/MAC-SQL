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
        return f"""You are an expert SQL query generator for SQLite databases.

## CRITICAL RULES (Must Follow)
1. **USE EXACT NAMES ONLY**: Use ONLY table/column names from the provided schema mapping - copy them EXACTLY (CASE-SENSITIVE)
2. **NO GUESSING OR ASSUMPTIONS**: NEVER modify, invent, or assume table/column names - use ONLY what's provided in the mapping
3. **SCHEMA VALIDATION**: Before generating SQL, verify that ALL table/column names exist in the provided mapping
4. **NO FICTIONAL SCHEMAS**: NEVER use assumed names like "Schools", "Students", "TestScores" - use ONLY actual names from mapping
5. **FOLLOW CONSTRAINTS**: Apply all SQL generation constraints systematically
6. **PREFER SIMPLICITY**: Use single-table queries when possible, avoid unnecessary joins

## SCHEMA VALIDATION CHECKPOINT
Before writing any SQL:
- Check that schema mapping is provided and contains actual table/column names
- If mapping is empty or missing, REQUEST schema linking first - DO NOT proceed with assumptions
- Verify EVERY table and column name in your SQL exists in the mapping
- If you cannot find required schema elements, explain what's missing - DO NOT invent names

## Your Task
Generate accurate SQL queries based on the provided node information. You handle THREE scenarios automatically:

1. **New Generation**: No existing SQL - generate fresh SQL from intent and schema
2. **Refinement**: Existing SQL with issues - improve the SQL based on errors/feedback  
3. **Combination**: Multiple child node SQLs - combine them into a single query

The context will indicate which scenario applies.

### Step 1: Parse Context and Analyze Schema Linking
Extract from the "current_node" JSON:
- **intent**: The query to convert to SQL
- **mapping**: Tables, columns, and joins to use (preferred solution from schema linker)
  - Pay special attention to mapping.columns[].exactValue for filter values
  - **CRITICAL**: Check mapping.columns[].dataType to format values correctly
- **sql**: Previous SQL (if this is a retry)
- **executionResult**: Previous execution results and errors
- **evidence**: Domain-specific knowledge

**MANDATORY SCHEMA CHECK**:
- If mapping is empty (empty dict), missing, or contains no table information: STOP and request schema linking
- DO NOT proceed with assumed table/column names
- Example error response: "Schema mapping is empty. Cannot generate SQL without actual table/column names from schema linking."

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

<generation>
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
</generation>

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
                    
                    # Record in history with attempt tracking
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        await self.history_manager.record_generate_sql(node)
                        
                        # Track attempt count for max attempts logic
                        current_attempts = await self._get_node_attempt_count(node_id)
                        self.logger.info(f"SQL generation attempt #{current_attempts} for node {node_id}")
                        
                        if current_attempts >= 3:
                            self.logger.warning(f"Node {node_id} has reached maximum attempts ({current_attempts}). Will be marked complete by TaskStatusChecker.")
                    
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
    
    async def _get_node_attempt_count(self, node_id: str) -> int:
        """Get the attempt count for a node from its operation history."""
        try:
            operations = await self.history_manager.get_node_operations(node_id)
            
            # Count SQL generation attempts (specifically record_generate_sql calls)
            sql_generation_attempts = 0
            for op in operations:
                if hasattr(op, 'operationType') and op.operationType == 'GENERATE_SQL':
                    sql_generation_attempts += 1
            
            return sql_generation_attempts
        except Exception as e:
            self.logger.warning(f"Could not get attempt count for node {node_id}: {e}")
            return 0
    
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
    
