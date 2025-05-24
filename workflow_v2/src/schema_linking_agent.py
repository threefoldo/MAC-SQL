"""
Schema Linking Agent for text-to-SQL workflow.

This agent links relevant schema information to query nodes in the tree.
It analyzes the intent of a node and finds all relevant tables, columns,
and relationships needed to generate SQL for that node.
"""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_types import (
    QueryNode, QueryMapping, TableMapping, ColumnMapping, JoinMapping,
    TableSchema, ColumnInfo, NodeStatus
)
from memory_agent_tool import MemoryAgentTool


class SchemaLinkingAgent:
    """
    Links database schema elements to query nodes.
    
    This agent:
    1. Analyzes a query node's intent
    2. Identifies relevant tables and columns
    3. Determines join relationships
    4. Updates the node's mapping with schema information
    """
    
    def __init__(self,
                 memory: KeyValueMemory,
                 model_name: str = "gpt-4o",
                 debug: bool = False):
        """
        Initialize the schema linking agent.
        
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
        """Setup the schema linking agent with memory callbacks."""
        
        # Agent signature
        signature = """
        Link database schema elements to a query node's intent.
        
        You will:
        1. Analyze the node's intent
        2. Select relevant tables and columns
        3. Identify necessary join relationships
        4. Consider sample data for better understanding
        
        Input:
        - node_id: The query node ID
        - intent: The node's query intent
        - full_schema: Complete database schema with sample data
        
        Output:
        XML format with selected schema elements and mappings
        """
        
        # Instructions for the agent
        instructions = """
        You are a schema linking expert for text-to-SQL conversion. Your job is to:
        
        1. Analyze the query intent to understand data requirements
        2. Select ONLY the necessary tables and columns
        3. Identify join relationships between selected tables
        4. Map schema elements to their purpose in the query
        
        Consider:
        - Be minimal: only include what's absolutely necessary
        - Think about implicit requirements (e.g., joins need key columns)
        - Use sample data to verify your selections make sense
        
        Output your schema linking in this XML format:
        
        <schema_linking>
          <selected_tables>
            <table name="table_name" alias="t1">
              <purpose>Why this table is needed</purpose>
              <columns>
                <column name="column_name" used_for="select|filter|join|group|order">
                  <reason>Why this column is needed</reason>
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
              <join_type>INNER|LEFT|RIGHT</join_type>
            </join>
          </joins>
          
          <sample_query_pattern>
            A template showing the query structure (not actual SQL)
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Create the agent
        self.agent = MemoryAgentTool(
            name="schema_linker",
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
            Enhanced inputs with node intent and full schema
        """
        node_id = inputs.get("node_id")
        if not node_id:
            raise ValueError("node_id is required")
        
        # Get the node
        node = await self.tree_manager.get_node(node_id)
        if not node:
            raise ValueError(f"Node {node_id} not found")
        
        # Add intent to inputs
        inputs["intent"] = node.intent
        
        # Get full schema with sample data
        schema_xml = await self._get_full_schema_xml()
        inputs["full_schema"] = schema_xml
        
        # Get parent node's mapping if exists (for context)
        if node.parentId:
            parent = await self.tree_manager.get_parent(node_id)
            if parent and parent.mapping.tables:
                parent_tables = [t.name for t in parent.mapping.tables]
                inputs["parent_context"] = f"Parent query uses tables: {', '.join(parent_tables)}"
        
        self.logger.debug(f"Pre-callback: Prepared context for node {node_id}")
        
        return inputs
    
    async def _post_callback(self, output: str, original_inputs: Dict[str, Any]) -> str:
        """
        Post-processing callback to parse results and update node mapping.
        
        Args:
            output: The agent's output
            original_inputs: The original inputs
            
        Returns:
            The processed output
        """
        try:
            node_id = original_inputs["node_id"]
            
            # Parse the schema linking output
            linking = self._parse_linking_xml(output)
            
            if linking:
                # Create QueryMapping from the linking
                mapping = await self._create_mapping_from_linking(linking)
                
                # Update the node's mapping
                await self.tree_manager.update_node_mapping(node_id, mapping)
                
                # Record in history
                await self.history_manager.add_operation({
                    "timestamp": datetime.now().isoformat(),
                    "nodeId": node_id,
                    "operation": "schema_linking",
                    "data": {
                        "tables_linked": len(mapping.tables),
                        "columns_linked": len(mapping.columns),
                        "joins_identified": len(mapping.joins) if mapping.joins else 0
                    }
                })
                
                # Store linking result for reference
                await self.memory.set(f"schema_linking_{node_id}", linking)
                
                self.logger.info(f"Schema linking completed for node {node_id}: "
                               f"{len(mapping.tables)} tables, {len(mapping.columns)} columns")
                
        except Exception as e:
            self.logger.error(f"Error in post-callback: {str(e)}", exc_info=True)
        
        return output
    
    async def _get_full_schema_xml(self) -> str:
        """Get complete database schema with sample data in XML format."""
        tables = await self.schema_manager.get_all_tables()
        
        if not tables:
            return "<database_schema>No schema loaded</database_schema>"
        
        xml_parts = ["<database_schema>"]
        
        for table_name, table in tables.items():
            xml_parts.append(f'  <table name="{table_name}">')
            
            # Add columns
            xml_parts.append('    <columns>')
            for col_name, col_info in table.columns.items():
                attrs = [f'name="{col_name}"', f'type="{col_info.dataType}"']
                
                if col_info.isPrimaryKey:
                    attrs.append('primary_key="true"')
                
                if col_info.isForeignKey and col_info.references:
                    ref_str = f"{col_info.references['table']}.{col_info.references['column']}"
                    attrs.append(f'foreign_key="{ref_str}"')
                
                if col_info.nullable:
                    attrs.append('nullable="true"')
                
                xml_parts.append(f'      <column {" ".join(attrs)} />')
            xml_parts.append('    </columns>')
            
            # Add sample data if available
            if table.sampleData:
                xml_parts.append('    <sample_data>')
                for row in table.sampleData[:5]:  # Limit to 5 rows
                    xml_parts.append('      <row>')
                    for col, value in row.items():
                        # Escape XML special characters
                        value_str = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                        xml_parts.append(f'        <{col}>{value_str}</{col}>')
                    xml_parts.append('      </row>')
                xml_parts.append('    </sample_data>')
            
            # Add metadata if available
            if table.metadata:
                xml_parts.append('    <metadata>')
                if 'rowCount' in table.metadata:
                    xml_parts.append(f'      <row_count>{table.metadata["rowCount"]}</row_count>')
                if 'indexes' in table.metadata:
                    xml_parts.append(f'      <indexes>{", ".join(table.metadata["indexes"])}</indexes>')
                xml_parts.append('    </metadata>')
            
            xml_parts.append('  </table>')
        
        # Add foreign key relationships summary
        xml_parts.append('  <relationships>')
        for table_name, table in tables.items():
            for col_name, col_info in table.columns.items():
                if col_info.isForeignKey and col_info.references:
                    xml_parts.append(f'    <foreign_key>')
                    xml_parts.append(f'      <from>{table_name}.{col_name}</from>')
                    xml_parts.append(f'      <to>{col_info.references["table"]}.{col_info.references["column"]}</to>')
                    xml_parts.append(f'    </foreign_key>')
        xml_parts.append('  </relationships>')
        
        xml_parts.append('</database_schema>')
        
        return '\n'.join(xml_parts)
    
    def _parse_linking_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the schema linking XML output."""
        try:
            # Extract XML from output
            xml_match = re.search(r'<schema_linking>.*?</schema_linking>', output, re.DOTALL)
            if not xml_match:
                # Try to find XML in code blocks
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
                    self.logger.error("No schema_linking XML found in output")
                    return None
            else:
                xml_content = xml_match.group()
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            linking = {
                "tables": [],
                "joins": [],
                "sample_query_pattern": ""
            }
            
            # Parse selected tables
            tables_elem = root.find("selected_tables")
            if tables_elem is not None:
                for table_elem in tables_elem.findall("table"):
                    table_info = {
                        "name": table_elem.get("name"),
                        "alias": table_elem.get("alias"),
                        "purpose": table_elem.findtext("purpose", "").strip(),
                        "columns": []
                    }
                    
                    # Parse columns
                    columns_elem = table_elem.find("columns")
                    if columns_elem is not None:
                        for col_elem in columns_elem.findall("column"):
                            col_info = {
                                "name": col_elem.get("name"),
                                "used_for": col_elem.get("used_for", "").strip(),
                                "reason": col_elem.findtext("reason", "").strip()
                            }
                            table_info["columns"].append(col_info)
                    
                    linking["tables"].append(table_info)
            
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
                    linking["joins"].append(join_info)
            
            # Parse sample query pattern
            linking["sample_query_pattern"] = root.findtext("sample_query_pattern", "").strip()
            
            return linking
            
        except Exception as e:
            self.logger.error(f"Error parsing schema linking XML: {str(e)}", exc_info=True)
            return None
    
    async def _create_mapping_from_linking(self, linking: Dict[str, Any]) -> QueryMapping:
        """Create a QueryMapping from the linking result."""
        mapping = QueryMapping()
        
        # Create table mappings
        for table_info in linking.get("tables", []):
            table_mapping = TableMapping(
                name=table_info["name"],
                alias=table_info.get("alias"),
                purpose=table_info.get("purpose", "")
            )
            mapping.tables.append(table_mapping)
            
            # Create column mappings
            for col_info in table_info.get("columns", []):
                column_mapping = ColumnMapping(
                    table=table_info["name"],
                    column=col_info["name"],
                    usedFor=col_info.get("used_for", "")
                )
                mapping.columns.append(column_mapping)
        
        # Create join mappings
        if linking.get("joins"):
            mapping.joins = []
            for join_info in linking["joins"]:
                join_mapping = JoinMapping(
                    from_table=join_info["from_table"],
                    to=join_info["to_table"],
                    on=f"{join_info['from_table']}.{join_info['from_column']} = {join_info['to_table']}.{join_info['to_column']}"
                )
                mapping.joins.append(join_mapping)
        
        return mapping
    
    async def link_schema(self, node_id: str) -> Dict[str, Any]:
        """
        Link schema elements to a query node.
        
        Args:
            node_id: The node ID to link schema for
            
        Returns:
            Schema linking result
        """
        self.logger.info(f"Linking schema for node {node_id}")
        
        # Run the agent
        result = await self.agent.run({"node_id": node_id})
        
        # Get the stored linking result
        linking = await self.memory.get(f"schema_linking_{node_id}")
        
        return linking if linking else {"error": "Schema linking failed"}
    
    async def link_all_unmapped_nodes(self) -> Dict[str, Any]:
        """Link schema for all nodes that don't have mappings yet."""
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree:
            return {"error": "No query tree found"}
        
        results = {
            "linked": [],
            "failed": [],
            "skipped": []
        }
        
        for node_id, node_data in tree["nodes"].items():
            node = QueryNode.from_dict(node_data)
            
            # Skip if already has mapping with tables
            if node.mapping and node.mapping.tables:
                results["skipped"].append(node_id)
                continue
            
            try:
                linking = await self.link_schema(node_id)
                if linking and not linking.get("error"):
                    results["linked"].append(node_id)
                else:
                    results["failed"].append(node_id)
            except Exception as e:
                self.logger.error(f"Failed to link schema for node {node_id}: {str(e)}")
                results["failed"].append(node_id)
        
        return results
    
    async def get_node_schema_summary(self, node_id: str) -> Dict[str, Any]:
        """Get a summary of schema elements linked to a node."""
        node = await self.tree_manager.get_node(node_id)
        if not node:
            return {"error": "Node not found"}
        
        summary = {
            "node_id": node_id,
            "intent": node.intent,
            "has_mapping": bool(node.mapping and node.mapping.tables),
            "tables": [],
            "total_columns": 0,
            "joins": []
        }
        
        if node.mapping:
            summary["tables"] = [t.name for t in node.mapping.tables]
            summary["total_columns"] = len(node.mapping.columns)
            
            if node.mapping.joins:
                summary["joins"] = [
                    f"{j.from_table} -> {j.to}" for j in node.mapping.joins
                ]
        
        # Get linking details if available
        linking = await self.memory.get(f"schema_linking_{node_id}")
        if linking:
            summary["sample_query_pattern"] = linking.get("sample_query_pattern", "")
        
        return summary