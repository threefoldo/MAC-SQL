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
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_types import (
    QueryNode, QueryMapping, NodeStatus, ExecutionResult,
    CombineStrategy, CombineStrategyType
)
from memory_agent_tool import MemoryAgentTool


class SQLGeneratorAgent:
    """
    Generates SQL queries for query nodes.
    
    This agent:
    1. Takes a query node with intent and schema mapping
    2. Generates appropriate SQL based on the requirements
    3. Handles different query types (simple, joins, aggregations)
    4. Considers parent-child relationships for subqueries
    """
    
    def __init__(self,
                 memory: KeyValueMemory,
                 model_name: str = "gpt-4o",
                 debug: bool = False):
        """
        Initialize the SQL generator agent.
        
        Args:
            memory: The KeyValueMemory instance
            model_name: The LLM model to use
            debug: Whether to enable debug logging
        """
        self.memory = memory
        self.model_name = model_name
        self.debug = debug
        
        # Initialize managers
        self.schema_manager = DatabaseSchemaManager(memory)
        self.tree_manager = QueryTreeManager(memory)
        self.history_manager = NodeHistoryManager(memory)
        
        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)
        if debug:
            self.logger.setLevel(logging.DEBUG)
        
        # Create the agent tool
        self._setup_agent()
    
    def _setup_agent(self):
        """Setup the SQL generator agent with memory callbacks."""
        
        # Agent signature
        signature = """
        Generate SQL query for a query node based on its intent and schema mapping.
        
        You will:
        1. Analyze the node's intent and mapped schema
        2. Generate syntactically correct SQL
        3. Consider child node results if combining queries
        4. Follow SQL best practices
        
        Input:
        - node_id: The query node ID
        - intent: The node's query intent
        - schema_mapping: Tables, columns, and joins for this query
        - child_results: Results from child nodes if applicable
        
        Output:
        XML format with generated SQL and explanation
        """
        
        # Instructions for the agent
        instructions = """
        You are an expert SQL generator for text-to-SQL conversion. Your job is to:
        
        1. Generate correct, efficient SQL based on the intent
        2. Use only the tables and columns from the schema mapping
        3. Apply appropriate JOINs, filters, and aggregations
        4. Handle NULL values and edge cases properly
        
        SQL Generation Rules:
        - Use proper JOIN syntax (prefer explicit JOINs over comma-separated)
        - Add appropriate WHERE clauses based on intent
        - Use GROUP BY for aggregations
        - Add ORDER BY and LIMIT when needed
        - Use table aliases if provided in mapping
        - Ensure all columns in SELECT are from mapped tables
        
        For queries with child nodes:
        - Use CTEs (WITH clauses) when combining results
        - Apply the specified combination strategy
        - Reference child results appropriately
        
        Output your SQL in this XML format:
        
        <sql_generation>
          <sql>
            <![CDATA[
            YOUR SQL QUERY HERE
            ]]>
          </sql>
          
          <explanation>
            Brief explanation of the SQL structure and logic
          </explanation>
          
          <query_type>select|aggregate|join|subquery|cte</query_type>
          
          <components>
            <tables>
              <table name="table_name" alias="alias">How it's used</table>
            </tables>
            <key_operations>
              <operation type="join|filter|group|order">Description</operation>
            </key_operations>
          </components>
        </sql_generation>
        """
        
        # Create the agent
        self.agent = MemoryAgentTool(
            name="sql_generator",
            signature=signature,
            instructions=instructions,
            model=self.model_name,
            memory=self.memory,
            pre_callback=self._pre_callback,
            post_callback=self._post_callback,
            debug=self.debug
        )
    
    async def _pre_callback(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pre-processing callback to prepare context for the agent.
        
        Args:
            inputs: The original inputs with node_id
            
        Returns:
            Enhanced inputs with node details and context
        """
        node_id = inputs.get("node_id")
        if not node_id:
            raise ValueError("node_id is required")
        
        # Get the node
        node = await self.tree_manager.get_node(node_id)
        if not node:
            raise ValueError(f"Node {node_id} not found")
        
        # Add node details
        inputs["intent"] = node.intent
        
        # Format schema mapping
        schema_mapping = await self._format_schema_mapping(node)
        inputs["schema_mapping"] = schema_mapping
        
        # Get child node results if this node has children
        if node.childIds:
            child_results = await self._get_child_results(node.childIds)
            inputs["child_results"] = child_results
            
            # Add combination strategy
            if node.combineStrategy:
                inputs["combination_strategy"] = self._format_combination_strategy(node.combineStrategy)
        
        # Get sample data for context
        sample_data = await self._get_relevant_sample_data(node)
        if sample_data:
            inputs["sample_data"] = sample_data
        
        self.logger.debug(f"Pre-callback: Prepared context for node {node_id}")
        
        return inputs
    
    async def _post_callback(self, output: str, original_inputs: Dict[str, Any]) -> str:
        """
        Post-processing callback to parse results and update node.
        
        Args:
            output: The agent's output
            original_inputs: The original inputs
            
        Returns:
            The processed output
        """
        try:
            node_id = original_inputs["node_id"]
            
            # Parse the SQL generation output
            generation = self._parse_generation_xml(output)
            
            if generation and generation.get("sql"):
                # Update the node with generated SQL
                await self.tree_manager.update_node_sql(node_id, generation["sql"])
                
                # Record SQL generation in history
                await self.history_manager.record_generate_sql(
                    node_id=node_id,
                    sql=generation["sql"]
                )
                
                # Store generation details
                await self.memory.set(f"sql_generation_{node_id}", generation)
                
                self.logger.info(f"SQL generated for node {node_id}: {generation.get('query_type', 'unknown')} query")
                
        except Exception as e:
            self.logger.error(f"Error in post-callback: {str(e)}", exc_info=True)
        
        return output
    
    async def _format_schema_mapping(self, node: QueryNode) -> str:
        """Format the node's schema mapping for the agent."""
        if not node.mapping or not node.mapping.tables:
            return "No schema mapping available"
        
        parts = ["<schema_mapping>"]
        
        # Tables
        parts.append("  <tables>")
        for table in node.mapping.tables:
            alias_str = f' alias="{table.alias}"' if table.alias else ""
            parts.append(f'    <table name="{table.name}"{alias_str}>')
            parts.append(f'      <purpose>{table.purpose}</purpose>')
            
            # Get columns for this table
            table_columns = [c for c in node.mapping.columns if c.table == table.name]
            if table_columns:
                parts.append('      <columns>')
                for col in table_columns:
                    parts.append(f'        <column name="{col.column}" used_for="{col.usedFor}" />')
                parts.append('      </columns>')
            
            parts.append('    </table>')
        parts.append("  </tables>")
        
        # Joins
        if node.mapping.joins:
            parts.append("  <joins>")
            for join in node.mapping.joins:
                parts.append(f'    <join>')
                parts.append(f'      <from>{join.from_table}</from>')
                parts.append(f'      <to>{join.to}</to>')
                parts.append(f'      <condition>{join.on}</condition>')
                parts.append(f'    </join>')
            parts.append("  </joins>")
        
        parts.append("</schema_mapping>")
        
        return '\n'.join(parts)
    
    async def _get_child_results(self, child_ids: List[str]) -> str:
        """Get results from child nodes if they have been executed."""
        parts = ["<child_results>"]
        
        for child_id in child_ids:
            child = await self.tree_manager.get_node(child_id)
            if child and child.sql:
                parts.append(f'  <child node_id="{child_id}">')
                parts.append(f'    <intent>{child.intent}</intent>')
                parts.append(f'    <sql><![CDATA[{child.sql}]]></sql>')
                
                if child.executionResult:
                    parts.append(f'    <row_count>{child.executionResult.rowCount}</row_count>')
                
                parts.append('  </child>')
        
        parts.append("</child_results>")
        
        return '\n'.join(parts)
    
    def _format_combination_strategy(self, strategy: CombineStrategy) -> str:
        """Format combination strategy for the agent."""
        parts = [f"Combination Type: {strategy.type.value}"]
        
        if strategy.type == CombineStrategyType.UNION:
            parts.append(f"Union Type: {strategy.unionType or 'UNION ALL'}")
        elif strategy.type == CombineStrategyType.JOIN:
            parts.append(f"Join Type: {strategy.joinType or 'INNER'}")
            if strategy.joinOn:
                parts.append(f"Join Columns: {', '.join(strategy.joinOn)}")
        elif strategy.type == CombineStrategyType.AGGREGATE:
            if strategy.aggregateFunction:
                parts.append(f"Aggregate Function: {strategy.aggregateFunction}")
            if strategy.groupBy:
                parts.append(f"Group By: {', '.join(strategy.groupBy)}")
        elif strategy.type == CombineStrategyType.FILTER:
            if strategy.filterCondition:
                parts.append(f"Filter: {strategy.filterCondition}")
        elif strategy.type == CombineStrategyType.CUSTOM:
            if strategy.template:
                parts.append(f"Template: {strategy.template}")
        
        return '\n'.join(parts)
    
    async def _get_relevant_sample_data(self, node: QueryNode) -> Optional[str]:
        """Get sample data for tables used in the query."""
        if not node.mapping or not node.mapping.tables:
            return None
        
        parts = ["<sample_data>"]
        
        for table in node.mapping.tables[:2]:  # Limit to first 2 tables
            sample_data = await self.schema_manager.get_sample_data(table.name)
            if sample_data:
                parts.append(f'  <table name="{table.name}">')
                for row in sample_data[:3]:  # Show up to 3 rows
                    parts.append('    <row>')
                    for col, val in row.items():
                        parts.append(f'      <{col}>{val}</{col}>')
                    parts.append('    </row>')
                parts.append('  </table>')
        
        parts.append("</sample_data>")
        
        return '\n'.join(parts) if len(parts) > 2 else None
    
    def _parse_generation_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the SQL generation XML output."""
        try:
            # Extract XML from output
            xml_match = re.search(r'<sql_generation>.*?</sql_generation>', output, re.DOTALL)
            if not xml_match:
                # Try to find XML in code blocks
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
                    self.logger.error("No sql_generation XML found in output")
                    return None
            else:
                xml_content = xml_match.group()
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            generation = {
                "sql": "",
                "explanation": "",
                "query_type": "",
                "components": {
                    "tables": [],
                    "operations": []
                }
            }
            
            # Extract SQL (handle CDATA)
            sql_elem = root.find("sql")
            if sql_elem is not None:
                sql_text = sql_elem.text or ""
                # Clean up SQL
                sql_text = sql_text.strip()
                if sql_text:
                    generation["sql"] = sql_text
            
            # Extract explanation
            generation["explanation"] = root.findtext("explanation", "").strip()
            
            # Extract query type
            generation["query_type"] = root.findtext("query_type", "").strip()
            
            # Extract components
            components_elem = root.find("components")
            if components_elem is not None:
                # Tables
                tables_elem = components_elem.find("tables")
                if tables_elem is not None:
                    for table_elem in tables_elem.findall("table"):
                        generation["components"]["tables"].append({
                            "name": table_elem.get("name"),
                            "alias": table_elem.get("alias"),
                            "usage": table_elem.text.strip() if table_elem.text else ""
                        })
                
                # Operations
                ops_elem = components_elem.find("key_operations")
                if ops_elem is not None:
                    for op_elem in ops_elem.findall("operation"):
                        generation["components"]["operations"].append({
                            "type": op_elem.get("type"),
                            "description": op_elem.text.strip() if op_elem.text else ""
                        })
            
            return generation
            
        except Exception as e:
            self.logger.error(f"Error parsing SQL generation XML: {str(e)}", exc_info=True)
            return None
    
    async def generate_sql(self, node_id: str) -> Dict[str, Any]:
        """
        Generate SQL for a query node.
        
        Args:
            node_id: The node ID to generate SQL for
            
        Returns:
            SQL generation result
        """
        self.logger.info(f"Generating SQL for node {node_id}")
        
        # Check if node has schema mapping
        node = await self.tree_manager.get_node(node_id)
        if not node:
            return {"error": "Node not found"}
        
        if not node.mapping or not node.mapping.tables:
            return {"error": "Node has no schema mapping. Run schema linking first."}
        
        # Run the agent
        result = await self.agent.run({"node_id": node_id})
        
        # Get the stored generation result
        generation = await self.memory.get(f"sql_generation_{node_id}")
        
        return generation if generation else {"error": "SQL generation failed"}
    
    async def generate_for_all_mapped_nodes(self) -> Dict[str, Any]:
        """Generate SQL for all nodes that have schema mappings but no SQL."""
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree:
            return {"error": "No query tree found"}
        
        results = {
            "generated": [],
            "failed": [],
            "skipped": []
        }
        
        for node_id, node_data in tree["nodes"].items():
            node = QueryNode.from_dict(node_data)
            
            # Skip if already has SQL
            if node.sql:
                results["skipped"].append(node_id)
                continue
            
            # Skip if no schema mapping
            if not node.mapping or not node.mapping.tables:
                continue
            
            try:
                generation = await self.generate_sql(node_id)
                if generation and not generation.get("error"):
                    results["generated"].append(node_id)
                else:
                    results["failed"].append(node_id)
            except Exception as e:
                self.logger.error(f"Failed to generate SQL for node {node_id}: {str(e)}")
                results["failed"].append(node_id)
        
        return results
    
    async def get_node_sql_summary(self, node_id: str) -> Dict[str, Any]:
        """Get a summary of SQL generation for a node."""
        node = await self.tree_manager.get_node(node_id)
        if not node:
            return {"error": "Node not found"}
        
        summary = {
            "node_id": node_id,
            "intent": node.intent,
            "has_sql": bool(node.sql),
            "status": node.status.value,
            "sql": node.sql if node.sql else None
        }
        
        # Get generation details if available
        generation = await self.memory.get(f"sql_generation_{node_id}")
        if generation:
            summary["query_type"] = generation.get("query_type", "")
            summary["explanation"] = generation.get("explanation", "")
            summary["tables_used"] = [t["name"] for t in generation.get("components", {}).get("tables", [])]
        
        return summary
    
    async def validate_sql(self, node_id: str) -> Dict[str, Any]:
        """Validate that generated SQL matches the node's schema mapping."""
        node = await self.tree_manager.get_node(node_id)
        if not node or not node.sql:
            return {"valid": False, "error": "Node not found or has no SQL"}
        
        validation = {
            "valid": True,
            "issues": []
        }
        
        # Check that all mapped tables are used
        if node.mapping and node.mapping.tables:
            sql_lower = node.sql.lower()
            for table in node.mapping.tables:
                if table.name.lower() not in sql_lower:
                    validation["issues"].append(f"Mapped table '{table.name}' not found in SQL")
        
        # Basic SQL syntax checks
        if not any(keyword in node.sql.upper() for keyword in ["SELECT", "WITH"]):
            validation["issues"].append("SQL does not start with SELECT or WITH")
            validation["valid"] = False
        
        if validation["issues"]:
            validation["valid"] = False
        
        return validation