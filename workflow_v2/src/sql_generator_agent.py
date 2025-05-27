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
from prompts import SQL_CONSTRAINTS, MAX_ROUND, format_refiner_template


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
        return f"""You are an expert SQL query generator for SQLite databases. Your role is to generate accurate SQL queries based on the provided node information.

## SQL Generation Constraints
{SQL_CONSTRAINTS}

## Context Understanding
You will receive context information about the current node. The context will be provided in sections, each starting with "### Section Name". Look for these key sections:

1. **### Current Node**: Complete node information as a JSON object including:
   - intent: The natural language query to convert to SQL
   - mapping: Schema information with tables, columns, and joins to use
   - sql: Any previously generated SQL (if this is a revision)
   - executionResult: Previous execution results including data, rowCount, errors
   - evidence: Any domain knowledge/hints provided
   - status: Current processing status

2. **### Evidence** (when available): Domain-specific knowledge or hints that should guide SQL generation
   - Use this information to understand domain context
   - Apply any specific rules or constraints mentioned
   - Consider evidence when interpreting ambiguous requirements

3. **### Children Nodes** (optional): Information about child nodes if this is a complex query

4. **### Node History**: Complete history showing all operations on this node

5. **### Sql Evaluation Analysis** (optional): Detailed analysis of previous SQL attempt:
   - answers_intent: Whether the SQL answered the query correctly
   - result_quality: Quality assessment (excellent/good/acceptable/poor)
   - issues: Specific problems identified
   - suggestions: Recommendations for improvement

## IMPORTANT: Reading the Context
To generate SQL, you MUST first parse the ### Current Node section to extract:
1. The intent (what query to generate)
2. The mapping (which tables/columns to use)
3. Any previous SQL attempts and their results
4. The evidence (if provided in the node or separately)

The Current Node section contains a JSON object - parse it to understand the query requirements.

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

### CRITICAL for Retry Attempts (when node has sql and executionResult):
**YOU MUST NOT GENERATE THE SAME SQL THAT FAILED**

1. **If executionResult.error exists:**
   - Analyze the error message carefully
   - Common fixes:
     - "no such table/column": Check exact names in mapping
     - "ambiguous column": Add table aliases
     - "syntax error": Fix SQL syntax
     - "division by zero": Add NULLIF or CASE to handle zeros

2. **If executionResult.rowCount is 0 (NO RESULTS):**
   - The SQL syntax was correct but returned no data
   - DO NOT just adjust the SQL slightly - try a different approach:
     - Replace exact matches with LIKE '%value%'
     - Check if string values need different casing
     - Try removing some WHERE conditions to be less restrictive
     - Consider if JOINs are excluding all records
     - Look for typos in string literals

3. **If sql_evaluation_analysis shows poor/acceptable quality:**
   - Address each issue listed
   - Follow the suggestions provided
   - Ensure the new SQL better answers the intent

**In your explanation, clearly state:**
- What failed in the previous attempt
- What specific changes you made
- Why these changes should work

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
- Apply SQL constraints systematically:
  * Select only needed columns without unnecessary data
  * Avoid unnecessary tables in FROM/JOIN clauses
  * Use JOIN before MAX/MIN functions
  * Handle NULL values with IS NOT NULL when needed
  * Use GROUP BY before ORDER BY for distinct values
  * Use CAST for type conversions in SQLite

### CRITICAL RULES:
- Use EXACT table and column names from the mapping (CASE-SENSITIVE)
- Do NOT modify or guess table/column names
- If the mapping has incorrect names, the query will fail - do not try to fix them

## Special Case: SQL Refinement
If a 'refiner_prompt' is provided in the context, it means the previous SQL failed and needs correction.
In this case:
1. **Follow the refiner prompt exactly** - it contains the specific error and structured guidance
2. The refiner prompt already includes all necessary information (schema, error, constraints)
3. Generate the corrected SQL as requested in the refiner prompt
4. Still use the standard output format below

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
                    self.logger.info(f"Updated node {node_id} with generated SQL")
                else:
                    self.logger.warning("No node_id found to update with generated SQL")
                
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
    
    async def generate_sql(self, node_id: str, retry_count: int = 0) -> Optional[str]:
        """
        Generate SQL for a specific query node.
        
        Args:
            node_id: The ID of the node to generate SQL for
            retry_count: Current retry attempt number
            
        Returns:
            The generated SQL or None if failed
        """
        self.logger.debug(f"Generating SQL for node: {node_id} (attempt {retry_count + 1}/{MAX_ROUND})")
        
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