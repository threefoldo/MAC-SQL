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
from base_memory_agent import BaseMemoryAgent
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_content_types import (
    QueryNode, QueryMapping, TableMapping, ColumnMapping, JoinMapping,
    TableSchema, ColumnInfo, NodeStatus
)


class SchemaLinkingAgent(BaseMemoryAgent):
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
        return """You are a schema linking expert for text-to-SQL conversion. Your job is to:

1. Analyze the query intent to understand data requirements
2. Select ONLY the necessary tables and columns
3. Identify join relationships between selected tables
4. Map schema elements to their purpose in the query

Consider:
- Be minimal: only include what's absolutely necessary
- Think about implicit requirements (e.g., joins need key columns)
- Use sample data to verify your selections make sense
- Consider data types and constraints

Output your analysis in this XML format:

<schema_linking>
  <selected_tables>
    <table name="table_name" alias="t1">
      <purpose>Why this table is needed</purpose>
      <columns>
        <column name="column_name" used_for="select|filter|join|group|order|aggregate">
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
      <join_type>INNER|LEFT|RIGHT|FULL</join_type>
    </join>
  </joins>
  
  <sample_query_pattern>
    A sample SQL pattern showing how these elements would be used
  </sample_query_pattern>
</schema_linking>"""
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from memory before schema linking"""
        context = {}
        
        # Extract node_id from task if provided
        node_id = None
        if "node:" in task:
            # Task format: "node:node_id - actual task"
            parts = task.split(" - ", 1)
            if parts[0].startswith("node:"):
                node_id = parts[0][5:]
                task = parts[1] if len(parts) > 1 else task
        
        # If no node_id in task, check if there's a current node
        if not node_id:
            current_node_id = await memory.get("current_node_id")
            if current_node_id:
                node_id = current_node_id
        
        if node_id:
            # Get the node
            node = await self.tree_manager.get_node(node_id)
            if node:
                context["node_id"] = node_id
                context["intent"] = node.intent
            else:
                self.logger.warning(f"Node {node_id} not found")
                context["intent"] = task
        else:
            # No node context, use the task as intent
            context["intent"] = task
        
        # Get full schema with sample data
        schema_xml = await self._get_full_schema_xml()
        context["full_schema"] = schema_xml
        
        self.logger.debug(f"Schema linking context prepared for intent: {context.get('intent', '')[:100]}...")
        
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the schema linking results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        
        try:
            # Parse the XML output
            linking_result = self._parse_linking_xml(last_message)
            
            if linking_result:
                # Create mapping from the linking result
                mapping = await self._create_mapping_from_linking(linking_result)
                
                # Get the node ID to update
                node_id = None
                if "node:" in task:
                    parts = task.split(" - ", 1)
                    if parts[0].startswith("node:"):
                        node_id = parts[0][5:]
                
                if not node_id:
                    node_id = await memory.get("current_node_id")
                
                if node_id:
                    # Update the node with the mapping
                    await self.tree_manager.update_node_mapping(node_id, mapping)
                    
                    # Record in history
                    await self.history_manager.record_update(
                        node_id=node_id,
                        field="mapping",
                        old_value=None,
                        new_value=mapping
                    )
                    
                    self.logger.info(f"Updated node {node_id} with schema mapping")
                else:
                    # Store mapping in memory for other uses
                    await memory.set("last_schema_mapping", mapping.to_dict())
                    self.logger.info("Stored schema mapping in memory")
                
        except Exception as e:
            self.logger.error(f"Error parsing schema linking results: {str(e)}", exc_info=True)
    
    async def _get_full_schema_xml(self) -> str:
        """Get full database schema with sample data in XML format"""
        tables = await self.schema_manager.get_all_tables()
        
        if not tables:
            return "<database_schema>No schema loaded</database_schema>"
        
        xml_parts = ["<database_schema>"]
        
        for table_name, table in tables.items():
            xml_parts.append(f'  <table name="{table_name}">')
            
            # Add columns
            for col_name, col_info in table.columns.items():
                xml_parts.append(f'    <column name="{col_name}">')
                xml_parts.append(f'      <type>{col_info.dataType}</type>')
                xml_parts.append(f'      <nullable>{col_info.nullable}</nullable>')
                
                if col_info.isPrimaryKey:
                    xml_parts.append('      <primary_key>true</primary_key>')
                
                if col_info.isForeignKey and col_info.references:
                    xml_parts.append(f'      <foreign_key>')
                    xml_parts.append(f'        <references_table>{col_info.references["table"]}</references_table>')
                    xml_parts.append(f'        <references_column>{col_info.references["column"]}</references_column>')
                    xml_parts.append(f'      </foreign_key>')
                
                xml_parts.append('    </column>')
            
            # Add sample data if available
            if hasattr(table, 'sampleData') and table.sampleData:
                xml_parts.append('    <sample_data>')
                for i, row in enumerate(table.sampleData[:3]):  # Limit to 3 samples
                    xml_parts.append(f'      <row id="{i+1}">')
                    for col, value in row.items():
                        xml_parts.append(f'        <{col}>{value}</{col}>')
                    xml_parts.append('      </row>')
                xml_parts.append('    </sample_data>')
            
            xml_parts.append('  </table>')
        
        xml_parts.append('</database_schema>')
        
        return '\n'.join(xml_parts)
    
    def _parse_linking_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the schema linking XML output"""
        try:
            # Extract XML from output
            xml_content = MemoryCallbackHelpers.extract_xml_content(output, "schema_linking")
            if not xml_content:
                # Try to find XML in code blocks
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
                    self.logger.error("No schema linking XML found in output")
                    return None
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            result = {
                "tables": [],
                "joins": [],
                "sample_query": root.findtext("sample_query_pattern", "").strip()
            }
            
            # Parse selected tables
            tables_elem = root.find("selected_tables")
            if tables_elem is not None:
                for table_elem in tables_elem.findall("table"):
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
                column_mapping = ColumnMapping(
                    table=table_info["name"],
                    column=col_info["name"],
                    usedFor=col_info.get("used_for", "select")
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
                on=f"{from_ref} = {to_ref}",
                type=join_info.get("join_type", "INNER")
            )
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
        self.logger.info(f"Linking schema for node: {node_id}")
        
        # Store the node ID in memory for the callbacks
        await self.memory.set("current_node_id", node_id)
        
        # Run the agent with node context
        task = f"node:{node_id} - Link schema for this query node"
        result = await self.run(task)
        
        # Get the mapping from memory
        mapping_dict = await self.memory.get("last_schema_mapping")
        if mapping_dict:
            return QueryMapping.from_dict(mapping_dict)
        
        return None