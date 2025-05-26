"""
Query Analyzer Agent for text-to-SQL tree orchestration.

This agent analyzes user queries and creates structured intents in memory.
For complex queries, it decomposes them into simpler sub-queries and builds
a query tree structure.
"""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from task_context_manager import TaskContextManager
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_content_types import (
    QueryNode, QueryMapping, TableMapping, ColumnMapping,
    CombineStrategy, CombineStrategyType, NodeStatus
)


class QueryAnalyzerAgent(BaseMemoryAgent):
    """
    Analyzes user queries and creates structured query trees.
    
    This agent:
    1. Analyzes the user's natural language query
    2. Creates a structured intent as the root node
    3. Decomposes complex queries into simpler sub-queries
    4. Builds a query tree structure in memory
    """
    
    agent_name = "query_analyzer"
    
    def _initialize_managers(self):
        """Initialize the managers needed for query analysis"""
        self.task_manager = TaskContextManager(self.memory)
        self.schema_manager = DatabaseSchemaManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
    
    def _build_system_message(self) -> str:
        """Build the system message for query analysis"""
        return """You are a query analyzer for text-to-SQL conversion. Your job is to:

1. Analyze the user's query to understand their intent
2. Identify which tables and columns are needed
3. Determine query complexity:
   - Simple: Single table or straightforward join
   - Complex: Multiple aggregations, nested queries, complex conditions

4. For complex queries, decompose them into simpler sub-queries that can be:
   - Executed independently
   - Combined to produce the final result

Output your analysis in this XML format:

<analysis>
  <intent>Clear description of what the user wants</intent>
  <complexity>simple|complex</complexity>
  <tables>
    <table name="table_name" purpose="why this table is needed"/>
  </tables>
  <decomposition>
    <subquery id="1">
      <intent>What this subquery does</intent>
      <description>Detailed description</description>
      <tables>table1, table2</tables>
    </subquery>
    <subquery id="2">
      <intent>What this subquery does</intent>
      <description>Detailed description</description>
      <tables>table3</tables>
    </subquery>
    <combination>
      <strategy>union|join|aggregate|filter|custom</strategy>
      <description>How to combine the subquery results</description>
    </combination>
  </decomposition>
</analysis>

For simple queries, omit the decomposition section.

IMPORTANT: After your analysis, include this at the end of your response:
<node_info>
The query tree has been created. The root node ID will be logged and should be used for subsequent agent calls.
</node_info>"""
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from memory before analyzing the query"""
        context = {}
        
        # Add the query to context
        context["query"] = task
        
        # Get database schema
        schema_xml = await self._get_schema_xml()
        context["schema"] = schema_xml
        
        # Get any existing task context
        task_context = await self.task_manager.get()
        if task_context:
            context["database_id"] = task_context.databaseName
        
        self.logger.info(f"Query analyzer context prepared with schema length: {len(schema_xml)}")
        self.logger.info(f"query: {context['query']} database: {context['database_id']}")
        
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the analysis results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        
        try:
            # Parse the XML output
            analysis = self._parse_analysis_xml(last_message)
            
            if analysis:
                # Create the root node with the analyzed intent
                root_id = await self.tree_manager.initialize(analysis["intent"])
                
                # Record the creation in history
                await self.history_manager.record_create(
                    node_id=root_id,
                    intent=analysis["intent"]
                )
                
                # If complex query, create sub-query nodes
                if analysis.get("complexity") == "complex" and "decomposition" in analysis:
                    await self._create_subquery_nodes(root_id, analysis["decomposition"])
                
                # Store the analysis result with node ID
                analysis["root_node_id"] = root_id
                await memory.set("query_analysis", analysis)
                
                # Set current node based on complexity
                if analysis.get("complexity") == "simple":
                    # Simple query: current node is the root
                    await self.tree_manager.set_current_node_id(root_id)
                    self.logger.debug(f"Simple query - set root {root_id} as current node")
                else:
                    # Complex query: current node is the first child
                    tree = await self.tree_manager.get_tree()
                    if tree and "nodes" in tree and root_id in tree["nodes"]:
                        root_data = tree["nodes"][root_id]
                        # Get children IDs after they've been created
                        children = await self.tree_manager.get_children(root_id)
                        if children and len(children) > 0:
                            first_child_id = children[0].nodeId
                            await self.tree_manager.set_current_node_id(first_child_id)
                            self.logger.debug(f"Complex query - set first child {first_child_id} as current node")
                        else:
                            # Fallback to root if no children
                            await self.tree_manager.set_current_node_id(root_id)
                            self.logger.debug(f"Complex query but no children found - set root as current")
                
                # User-friendly logging
                self.logger.info("="*60)
                self.logger.info("Query Analysis")
                self.logger.info(f"Query: {task}")
                self.logger.info(f"Intent: {analysis.get('intent')}")
                self.logger.info(f"Complexity: {analysis.get('complexity').upper()}")
                
                if analysis.get('complexity') == 'complex' and 'decomposition' in analysis:
                    self.logger.info(f"Decomposed into {len(analysis['decomposition'].get('subqueries', []))} sub-queries:")
                    for sq in analysis['decomposition'].get('subqueries', []):
                        self.logger.info(f"  - {sq['intent']}")
                    self.logger.info(f"Combination strategy: {analysis['decomposition'].get('combination', {}).get('strategy', 'N/A').upper()}")
                
                self.logger.info("="*60)
                self.logger.debug(f"Root node ID: {root_id}")
                
        except Exception as e:
            self.logger.error(f"Error parsing analysis results: {str(e)}", exc_info=True)
    
    async def _get_schema_xml(self) -> str:
        """Get database schema in XML format"""
        tables = await self.schema_manager.get_all_tables()
        
        if not tables:
            return "<database_schema>No schema loaded</database_schema>"
        
        xml_parts = ["<database_schema>"]
        
        for table_name, table in tables.items():
            xml_parts.append(f'  <table name="{table_name}">')
            
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
            
            xml_parts.append('  </table>')
        
        xml_parts.append('</database_schema>')
        
        return '\n'.join(xml_parts)
    
    def _parse_analysis_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the analysis XML output"""
        try:
            # Extract XML from output
            xml_match = re.search(r'<analysis>.*?</analysis>', output, re.DOTALL)
            if not xml_match:
                # Try to find XML in code blocks
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
                    self.logger.error("No analysis XML found in output")
                    return None
            else:
                xml_content = xml_match.group()
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            analysis = {
                "intent": root.findtext("intent", "").strip(),
                "complexity": root.findtext("complexity", "simple").strip(),
                "tables": []
            }
            
            # Parse tables
            tables_elem = root.find("tables")
            if tables_elem is not None:
                for table_elem in tables_elem.findall("table"):
                    analysis["tables"].append({
                        "name": table_elem.get("name"),
                        "purpose": table_elem.get("purpose", "")
                    })
            
            # Parse decomposition if exists
            decomp_elem = root.find("decomposition")
            if decomp_elem is not None:
                decomposition = {
                    "subqueries": [],
                    "combination": {}
                }
                
                # Parse subqueries
                for sq_elem in decomp_elem.findall("subquery"):
                    subquery = {
                        "id": sq_elem.get("id"),
                        "intent": sq_elem.findtext("intent", "").strip(),
                        "description": sq_elem.findtext("description", "").strip(),
                        "tables": sq_elem.findtext("tables", "").strip().split(", ")
                    }
                    decomposition["subqueries"].append(subquery)
                
                # Parse combination strategy
                comb_elem = decomp_elem.find("combination")
                if comb_elem is not None:
                    decomposition["combination"] = {
                        "strategy": comb_elem.findtext("strategy", "").strip(),
                        "description": comb_elem.findtext("description", "").strip()
                    }
                
                analysis["decomposition"] = decomposition
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error parsing analysis XML: {str(e)}", exc_info=True)
            return None
    
    async def _create_subquery_nodes(self, parent_id: str, decomposition: Dict[str, Any]) -> None:
        """Create sub-query nodes in the tree"""
        subqueries = decomposition.get("subqueries", [])
        combination = decomposition.get("combination", {})
        
        # Create nodes for each subquery
        created_nodes = []
        for sq in subqueries:
            # Generate node ID
            node_id = f"node_{datetime.now().timestamp()}_{sq['id']}"
            
            # Create mapping (basic for now, can be enhanced)
            mapping = QueryMapping()
            for table_name in sq.get("tables", []):
                if table_name.strip():
                    mapping.tables.append(TableMapping(
                        name=table_name.strip(),
                        purpose=f"Used in subquery {sq['id']}"
                    ))
            
            # Create the node
            node = QueryNode(
                nodeId=node_id,
                intent=sq["intent"],
                mapping=mapping,
                parentId=parent_id
            )
            
            # Add to tree
            await self.tree_manager.add_node(node, parent_id)
            
            # Record in history
            await self.history_manager.record_create(
                node_id=node_id,
                intent=sq["intent"],
                mapping=mapping
            )
            
            created_nodes.append(node_id)
            self.logger.debug(f"Created subquery node: {node_id}")
        
        # Update parent node with combination strategy
        if combination and created_nodes:
            strategy_type = self._parse_strategy_type(combination.get("strategy", "custom"))
            combine_strategy = CombineStrategy(
                type=strategy_type
            )
            
            # Add strategy-specific details
            if strategy_type == CombineStrategyType.JOIN:
                combine_strategy.joinType = "INNER"  # Default, can be enhanced
            elif strategy_type == CombineStrategyType.UNION:
                combine_strategy.unionType = "UNION ALL"  # Default
            
            await self.tree_manager.update_node_combine_strategy(parent_id, combine_strategy)
    
    def _parse_strategy_type(self, strategy: str) -> CombineStrategyType:
        """Parse strategy string to enum"""
        strategy_map = {
            "union": CombineStrategyType.UNION,
            "join": CombineStrategyType.JOIN,
            "aggregate": CombineStrategyType.AGGREGATE,
            "filter": CombineStrategyType.FILTER,
            "custom": CombineStrategyType.CUSTOM
        }
        return strategy_map.get(strategy.lower(), CombineStrategyType.CUSTOM)
    
    async def get_analysis_summary(self) -> Dict[str, Any]:
        """Get a summary of the current analysis"""
        analysis = await self.memory.get("query_analysis")
        tree_stats = await self.tree_manager.get_tree_stats()
        
        summary = {
            "has_analysis": analysis is not None,
            "complexity": analysis.get("complexity") if analysis else None,
            "tree_stats": tree_stats
        }
        
        if analysis and "decomposition" in analysis:
            summary["subquery_count"] = len(analysis["decomposition"].get("subqueries", []))
            summary["combination_strategy"] = analysis["decomposition"].get("combination", {}).get("strategy")
        
        return summary