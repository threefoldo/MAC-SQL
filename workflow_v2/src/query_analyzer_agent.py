"""
Query Analyzer Agent for text-to-SQL tree orchestration.

This agent analyzes user queries and creates structured intents in memory.
For complex queries, it decomposes them into simpler sub-queries and builds
a query tree structure.
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from task_context_manager import TaskContextManager
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_content_types import (
    QueryNode, NodeStatus
)
from prompts import SUBQ_PATTERN, SQL_CONSTRAINTS
from utils import parse_xml_hybrid


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
        return f"""You are a query analyzer for text-to-SQL conversion. Your job is to:

1. Analyze the user's query to understand their intent
2. Identify which tables and columns are likely needed
3. Determine query complexity
4. For complex queries, decompose them into simpler sub-queries

## Schema-Informed Analysis

If schema analysis is provided (from SchemaLinker), use it to inform your decisions:
- **Table Selection**: Prefer tables already identified as relevant by schema analysis
- **Column Awareness**: Consider which columns were identified as important
- **Relationship Understanding**: Use identified foreign key relationships for decomposition
- **Confidence Boost**: Schema analysis increases confidence in table/column choices

If no schema analysis is available, perform standard analysis based on query text and evidence.

## Complexity Determination

**Simple Queries** (direct SQL generation):
- Single table queries (SELECT, COUNT, etc.)
- Basic joins between 2-3 tables
- Simple aggregations (SUM, AVG on one group)
- Straightforward WHERE conditions
- Basic sorting and limiting

**Complex Queries** (require decomposition):
- Comparisons against aggregated values (e.g., "above average")
- Multiple levels of aggregation
- Queries requiring intermediate results
- Complex business logic with multiple steps
- Questions with "and" connecting different analytical tasks
- Nested subqueries or CTEs needed
- Set operations (UNION, INTERSECT, EXCEPT)

## Table Identification Strategy

When analyzing which tables are needed:
1. **First priority**: Use schema analysis results if available
2. Look for entity mentions in the query (e.g., "students" → student table)
3. Use evidence to understand domain-specific terminology and mappings
4. Consider foreign key relationships for joins
5. Include tables needed for filtering even if not in SELECT

## Decomposition Guidelines

When decomposing complex queries:
1. **Break into logical steps**: Each sub-query should answer one clear question
2. **Ensure independence**: Sub-queries should be executable on their own
3. **Plan the combination**: Think about how results connect
4. **Order matters**: Earlier sub-queries may provide values for later ones
5. **Use schema insights**: If schema analysis identified key relationships, use them for decomposition

### Combination Strategies:
- **join**: When sub-queries share common columns to join on
- **union**: When combining similar results from different sources
- **aggregate**: When combining results needs SUM, COUNT, etc.
- **filter**: When one sub-query filters results of another
- **custom**: For complex logic not fitting above patterns

## SQL Generation Constraints to Consider
{SQL_CONSTRAINTS}

## Evidence Handling

Use the provided evidence exactly as given to:
- Understand domain-specific terminology and mappings
- Apply any constraints or business rules mentioned
- Determine correct table/column references
- Interpret data values and calculations

## Output Format

<analysis>
  <intent>Clear, concise description of what the user wants to find</intent>
  <complexity>simple|complex</complexity>
  <tables>
    <table name="table_name" purpose="why this table is needed"/>
  </tables>
  <decomposition>
    <subquery id="1">
      <intent>What this subquery finds</intent>
      <description>Detailed description including expected output</description>
      <tables>table1, table2</tables>
    </subquery>
    <subquery id="2">
      <intent>What this subquery finds</intent>
      <description>Detailed description including how it uses subquery 1</description>
      <tables>table3</tables>
    </subquery>
    <combination>
      <strategy>union|join|aggregate|filter|custom</strategy>
      <description>Specific description of how to combine results</description>
    </combination>
  </decomposition>
</analysis>

For simple queries, omit the decomposition section entirely.

## Examples

**Simple Query Example:**
Query: "Show all student names from the math class"
Analysis: Simple - single table with filter

**Complex Query Example:**
Query: "List schools with test scores above the district average"
Analysis: Complex - requires calculating average first, then comparing each school

IMPORTANT: Your analysis will be stored in the query tree node and used by SQLGenerator to create appropriate SQL queries."""
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from memory before analyzing the query"""
        context = {}
        
        # Get task context which contains the actual query and evidence
        task_context = await self.task_manager.get()
        if task_context:
            # Get query from task context (the actual question)
            context["query"] = task_context.originalQuery
            context["database_id"] = task_context.databaseName
            
            # Get evidence if available
            if task_context.evidence:
                context["evidence"] = task_context.evidence
        else:
            # Fallback to using task parameter if no task context
            context["query"] = task
            self.logger.warning("No task context found, using task parameter as query")
        
        # Get database schema
        schema_xml = await self._get_schema_xml()
        context["schema"] = schema_xml
        
        # Schema analysis is not needed here as query analysis happens before schema linking
        # The query analyzer should work independently without schema-specific context
        self.logger.debug("Query analysis will be schema-agnostic")
        
        self.logger.info(f"Query analyzer context prepared with schema length: {len(schema_xml)}")
        self.logger.info(f"query: {context['query']} database: {context.get('database_id', 'N/A')}")
        if "evidence" in context:
            self.logger.info(f"evidence: {context['evidence']}")
        
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the analysis results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        # Log the raw output for debugging
        self.logger.info(f"Raw LLM output: {last_message}")
        
        try:
            # Parse the XML output
            analysis = self._parse_analysis_xml(last_message)
            
            if analysis:
                # Get current node (root node created by orchestrator)
                current_node_id = await self.tree_manager.get_current_node_id()
                if not current_node_id:
                    self.logger.error("No current node found - orchestrator should have initialized the tree")
                    return
                
                # Store the entire analysis result in the QueryTree node
                await self.tree_manager.update_node(current_node_id, {"queryAnalysis": analysis})
                
                # Update the root node with the analyzed intent
                await self.tree_manager.update_node(current_node_id, {"intent": analysis.get("intent", "")})
                
                # Record the analysis in history
                await self.history_manager.record_create(
                    node_id=current_node_id,
                    intent=analysis.get("intent", "")
                )
                
                # If complex query, create sub-query nodes
                if analysis.get("complexity") == "complex" and "decomposition" in analysis:
                    await self._create_subquery_nodes(current_node_id, analysis["decomposition"])
                    
                    # For complex queries, set current node to first child
                    children = await self.tree_manager.get_children(current_node_id)
                    if children and len(children) > 0:
                        first_child_id = children[0].nodeId
                        await self.tree_manager.set_current_node_id(first_child_id)
                        self.logger.debug(f"Complex query - set first child {first_child_id} as current node")
                
                # Store the analysis result in memory as well (for backwards compatibility)
                analysis["root_node_id"] = current_node_id
                # Analysis is stored in the query tree nodes, not in memory directly
                
                # User-friendly logging
                self.logger.info("="*60)
                self.logger.info("Query Analysis")
                self.logger.info(f"Query: {task}")
                self.logger.info(f"Intent: {analysis.get('intent', 'N/A')}")
                complexity = analysis.get('complexity', 'unknown')
                self.logger.info(f"Complexity: {complexity.upper()}")
                
                if complexity == 'complex' and 'decomposition' in analysis:
                    # Handle both 'subqueries' and 'subquery' keys from XML parsing
                    subqueries = analysis['decomposition'].get('subqueries', analysis['decomposition'].get('subquery', []))
                    self.logger.info(f"Decomposed into {len(subqueries)} sub-queries:")
                    for sq in subqueries:
                        intent = sq.get('intent', 'N/A') if isinstance(sq, dict) else 'N/A'
                        self.logger.info(f"  - {intent}")
                    combination = analysis['decomposition'].get('combination', {})
                    strategy = combination.get('strategy', 'N/A') if isinstance(combination, dict) else 'N/A'
                    self.logger.info(f"Combination strategy: {strategy.upper()}")
                
                self.logger.info("="*60)
                self.logger.debug(f"Root node ID: {current_node_id}")
                
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
        """Parse the analysis XML output using hybrid approach"""
        # Use the hybrid parsing utility
        analysis = parse_xml_hybrid(output, 'analysis')
        
        # Handle fallback parsing for subqueries if needed
        if analysis and not analysis.get("decomposition", {}).get("subqueries") and SUBQ_PATTERN:
            # Look for "Sub question N:" pattern in the output as fallback
            matches = re.finditer(SUBQ_PATTERN, output)
            subqueries = []
            for i, match in enumerate(matches, 1):
                # Extract text after the match until next sub question or end
                start = match.end()
                next_match = re.search(SUBQ_PATTERN, output[start:])
                end = start + next_match.start() if next_match else len(output)
                
                sub_text = output[start:end].strip()
                if sub_text:
                    # Try to extract intent from the sub-question text
                    intent_match = re.search(r'^([^.!?]+[.!?])', sub_text)
                    intent = intent_match.group(1) if intent_match else sub_text[:100]
                    
                    subqueries.append({
                        "id": str(i),
                        "intent": intent.strip(),
                        "description": sub_text[:200],
                        "tables": []  # Will be determined by schema linker
                    })
            
            if subqueries:
                if "decomposition" not in analysis:
                    analysis["decomposition"] = {}
                analysis["decomposition"]["subqueries"] = subqueries
        
        return analysis
    
    async def _create_subquery_nodes(self, parent_id: str, decomposition: Dict[str, Any]) -> None:
        """Create sub-query nodes in the tree"""
        # Handle both 'subqueries' and 'subquery' keys from XML parsing
        subqueries = decomposition.get("subqueries", decomposition.get("subquery", []))
        
        # Create nodes for each subquery
        created_nodes = []
        for sq in subqueries:
            # Generate node ID
            node_id = f"node_{datetime.now().timestamp()}_{sq['id']}"
            
            # Create the node with just the basic info
            # Other agent outputs will be populated later by respective agents
            node = QueryNode(
                nodeId=node_id,
                intent=sq["intent"],
                parentId=parent_id
            )
            
            # Add to tree
            await self.tree_manager.add_node(node, parent_id)
            
            # Store the subquery info directly in the node
            await self.tree_manager.update_node(node_id, {"subqueryInfo": sq})
            
            # Record in history
            await self.history_manager.record_create(
                node_id=node_id,
                intent=sq["intent"]
            )
            
            created_nodes.append(node_id)
            self.logger.debug(f"Created subquery node: {node_id}")
        
    
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