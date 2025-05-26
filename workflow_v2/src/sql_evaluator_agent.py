"""
SQL Evaluator Agent for text-to-SQL tree orchestration.

This agent evaluates SQL query results and provides analysis of the execution.
Note: The actual SQL execution is handled by SQLExecutor, this agent analyzes
the results and provides insights.
"""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from sql_executor import SQLExecutor
from memory_content_types import (
    QueryNode, NodeStatus, ExecutionResult
)


class SQLEvaluatorAgent(BaseMemoryAgent):
    """
    Evaluates SQL execution results.
    
    This agent:
    1. Takes SQL query and its execution results
    2. Analyzes if the results answer the original intent
    3. Identifies potential issues or improvements
    4. Provides structured feedback on the results
    """
    
    agent_name = "sql_evaluator"
    
    def _initialize_managers(self):
        """Initialize the managers needed for SQL execution analysis"""
        self.tree_manager = QueryTreeManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
    
    def _build_system_message(self) -> str:
        """Build the system message for SQL result analysis"""
        return """You are an expert at analyzing SQL query execution results for a single node.

Your job is to:
1. Evaluate if the SQL results correctly answer the node's query intent
2. Check data quality and reasonableness of results
3. Identify any potential issues or unexpected outcomes
4. Determine the quality of the results

Quality Assessment:
- EXCELLENT: Perfect results, exactly answers the intent, no issues
- GOOD: Correct results with minor concerns that don't affect validity
- ACCEPTABLE: Results partially answer the intent or have moderate issues
- POOR: Results fail to answer the intent or have major issues

Consider:
- Does the result set size make sense?
- Are there any NULL values that might indicate issues?
- Does the data look reasonable and complete?
- Are there any performance concerns?
- Could the query be improved?

Output your analysis in XML format:

<evaluation>
  <answers_intent>yes|no|partially</answers_intent>
  <result_quality>excellent|good|acceptable|poor</result_quality>
  <result_summary>Brief description of what the results show</result_summary>
  <issues>
    <issue>
      <type>data_quality|performance|logic|other</type>
      <description>Description of the issue</description>
      <severity>high|medium|low</severity>
    </issue>
  </issues>
  <suggestions>
    <suggestion>Any suggestions for improvement</suggestion>
  </suggestions>
  <confidence_score>0.0-1.0</confidence_score>
</evaluation>

Focus ONLY on evaluating the current node's SQL results. Do not concern yourself with other nodes or tree completion."""
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from memory before analyzing results"""
        context = {}
        
        # Extract node_id from task
        node_id = await self.tree_manager.get_current_node_id()
        if not node_id:
            self.logger.error("No node_id found in task or QueryTreeManager. Task must be in format 'node:{node_id} - ...' or current_node_id must be set in QueryTreeManager")
            return context
        self.logger.info(f"Using current node: {node_id}")
        
        # Get the node
        node = await self.tree_manager.get_node(node_id)
        if not node:
            self.logger.error(f"Node {node_id} not found")
            return context
            
        context["node_id"] = node_id
        context["intent"] = node.intent
        context["sql"] = node.sql
        
        # Get execution result if available
        if node.executionResult:
            context["execution_result"] = self._format_execution_result(node.executionResult)
        else:
            # Execute SQL if we have it
            if node.sql:
                # Get task context to get database name
                task_context = await memory.get("taskContext")
                if not task_context:
                    self.logger.error("No task context found in memory")
                    return context
                
                db_name = task_context.get("databaseName")
                if not db_name:
                    self.logger.error("No database name in task context")
                    return context
                
                # Get data path and dataset name from database schema
                db_schema = await memory.get("databaseSchema")
                data_path = None
                dataset_name = "bird"  # Default to bird
                
                if db_schema and "metadata" in db_schema:
                    metadata = db_schema["metadata"]
                    data_path = metadata.get("data_path")
                    dataset_name = metadata.get("dataset_name", "bird")
                
                if not data_path:
                    # Try to infer from common patterns
                    self.logger.warning("No data_path in database schema metadata, using default")
                    data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
                
                self.logger.debug(f"Executing SQL for node {node_id} on database {db_name}")
                self.logger.debug(f"Using data_path: {data_path}, dataset_name: {dataset_name}")
                
                executor = SQLExecutor(data_path, dataset_name)
                
                try:
                    result_dict = executor.execute_sql(node.sql, db_name)
                    
                    if result_dict.get("error"):
                        execution_result = {
                            "status": "error",
                            "error": result_dict["error"],
                            "row_count": 0,
                            "columns": [],
                            "data": []
                        }
                    else:
                        execution_result = {
                            "status": "success",
                            "columns": result_dict.get("column_names", []),
                            "data": result_dict.get("data", []),
                            "row_count": len(result_dict.get("data", [])),
                            "execution_time": result_dict.get("execution_time")
                        }
                    
                    context["execution_result"] = execution_result
                    
                    # Update node with execution result
                    exec_result_obj = ExecutionResult(
                        data=execution_result["data"],
                        rowCount=execution_result["row_count"],
                        error=execution_result.get("error")
                    )
                    success = execution_result["status"] == "success"
                    await self.tree_manager.update_node_result(node_id, exec_result_obj, success)
                    
                except Exception as e:
                    self.logger.error(f"Error executing SQL: {str(e)}", exc_info=True)
                    execution_result = {
                        "status": "error",
                        "error": str(e),
                        "row_count": 0,
                        "columns": [],
                        "data": []
                    }
                    context["execution_result"] = execution_result
        
        self.logger.debug(f"SQL evaluator context prepared with result status: {context.get('execution_result', {}).get('status', 'unknown')}")
        
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the analysis results and update memory"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        self.logger.info(f"Raw LLM output (first 500 chars): {last_message[:500]}")
        
        try:
            # Extract XML from response
            evaluation_result = self._parse_evaluation_xml(last_message)
            
            if evaluation_result:
                # Store analysis
                await memory.set("execution_analysis", evaluation_result)
                
                # Get node ID
                node_id = None
                if "node:" in task:
                    parts = task.split(" - ", 1)
                    if parts[0].startswith("node:"):
                        node_id = parts[0][5:]
                
                if not node_id:
                    node_id = await self.tree_manager.get_current_node_id()
                
                if node_id:
                    # Store analysis for the node
                    await memory.set(f"node_{node_id}_analysis", evaluation_result)
                    
                    # Note: Node status is already updated when execution result is stored
                    # The status (EXECUTED_SUCCESS or EXECUTED_FAILED) is set based on whether
                    # the SQL execution had errors, not based on the quality of results
                    
                    # User-friendly logging
                    self.logger.info("="*60)
                    self.logger.info("SQL Execution & Evaluation")
                    
                    # Get node for intent
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        self.logger.info(f"Query intent: {node.intent}")
                    
                    # Log execution status
                    exec_result = await memory.get("execution_result")
                    if exec_result:
                        if exec_result.get("status") == "success":
                            self.logger.info(f"✓ SQL executed successfully")
                            self.logger.info(f"  Returned {exec_result.get('row_count', 0)} row(s)")
                            # Show sample of results
                            if exec_result.get('data') and len(exec_result['data']) > 0:
                                self.logger.info("  Sample results:")
                                # Show first 3 rows
                                for i, row in enumerate(exec_result['data'][:3]):
                                    if i == 0 and exec_result.get('columns'):
                                        # Show column headers
                                        self.logger.info(f"    {' | '.join(str(col) for col in exec_result['columns'])}")
                                    self.logger.info(f"    {' | '.join(str(val) for val in row)}")
                                if len(exec_result['data']) > 3:
                                    self.logger.info(f"    ... and {len(exec_result['data']) - 3} more row(s)")
                        else:
                            self.logger.info(f"✗ SQL execution failed: {exec_result.get('error', 'Unknown error')}")
                    
                    # Log evaluation results
                    self.logger.info(f"Evaluation results:")
                    self.logger.info(f"  - Answers intent: {evaluation_result.get('answers_intent', 'N/A').upper()}")
                    self.logger.info(f"  - Result quality: {evaluation_result.get('result_quality', 'N/A').upper()}")
                    self.logger.info(f"  - Confidence: {evaluation_result.get('confidence_score', 'N/A')}")
                    
                    if evaluation_result.get('result_summary'):
                        self.logger.info(f"  - Summary: {evaluation_result['result_summary']}")
                    
                    # Log issues if any
                    issues = evaluation_result.get('issues', [])
                    if issues:
                        self.logger.info(f"  Issues found:")
                        for issue in issues:
                            self.logger.info(f"    - [{issue.get('severity', 'N/A').upper()}] {issue.get('description', 'N/A')}")
                    
                    self.logger.info("="*60)
                    self.logger.info(f"Stored analysis for node {node_id}")
                    
                    # Log quality for debugging
                    quality = evaluation_result.get("result_quality", "").lower()
                    if quality in ["acceptable", "poor"]:
                        self.logger.info("="*60)
                        self.logger.info(f"⚠️  NODE NEEDS IMPROVEMENT - Quality: {quality.upper()}")
                        self.logger.info("This node should be retried with fixes")
                        self.logger.info("="*60)
                
                self.logger.info(f"Analysis complete - Answers intent: {evaluation_result.get('answers_intent')}, Quality: {evaluation_result.get('result_quality')}")
                
        except Exception as e:
            self.logger.error(f"Error parsing evaluation results: {str(e)}", exc_info=True)
            # Store raw analysis
            await memory.set("execution_analysis_raw", last_message)
    
    # Node progression is now handled by TaskStatusChecker
    # The following method has been removed as it's no longer needed
    
    async def _handle_node_progression_REMOVED(self, memory: KeyValueMemory, current_node_id: str) -> None:
        """
        Handle node progression after a successful evaluation.
        
        Rules:
        1. If current node has siblings, move to next sibling
        2. If no more siblings, move to parent
        3. If all children of parent are good, parent becomes current
        4. If at root and all descendants are good, tree processing is complete
        """
        try:
            tree = await self.tree_manager.get_tree()
            if not tree or "nodes" not in tree:
                return
            
            nodes = tree["nodes"]
            current_node = nodes.get(current_node_id)
            if not current_node:
                return
            
            # Find parent and siblings
            parent_id = current_node.get("parentId")
            if not parent_id:
                # This is the root node - check if all children are good
                if await self._all_children_good(nodes, current_node_id):
                    self.logger.info("="*60)
                    self.logger.info("✅ TREE COMPLETE: Root node and all descendants have good SQL!")
                    self.logger.info("All queries in the tree have been successfully executed.")
                    self.logger.info("="*60)
                    await memory.set("tree_complete", True)
                else:
                    self.logger.info("="*60) 
                    self.logger.info("⚠️  Root node processed but some children still need work")
                    self.logger.info("Coordinator should continue processing remaining nodes")
                    self.logger.info("="*60)
                return
            
            parent_node = nodes.get(parent_id)
            if not parent_node or "childIds" not in parent_node:
                return
            
            siblings = parent_node["childIds"]
            current_index = siblings.index(current_node_id) if current_node_id in siblings else -1
            
            # Check for next sibling
            if current_index >= 0 and current_index < len(siblings) - 1:
                next_node_id = siblings[current_index + 1]
                await self.tree_manager.set_current_node_id(next_node_id)
                self.logger.debug(f"Moving to next sibling: {next_node_id}")
                
                # Get info about the next node
                next_node = nodes.get(next_node_id)
                if next_node:
                    self.logger.info("="*60)
                    self.logger.info("📍 NODE PROGRESSION: Moving to next sibling")
                    self.logger.info(f"Next node: {next_node.get('intent', 'Unknown intent')}")
                    self.logger.info("Tree processing continues - coordinator should process this node")
                    self.logger.info("="*60)
            else:
                # No more siblings - check if all siblings are good
                if await self._all_children_good(nodes, parent_id):
                    # All children are good - move to parent
                    await self.tree_manager.set_current_node_id(parent_id)
                    self.logger.debug(f"All children of {parent_id} are good - moving to parent")
                    
                    # Get info about parent node
                    parent_node_info = nodes.get(parent_id)
                    if parent_node_info:
                        self.logger.info("="*60)
                        self.logger.info("📍 NODE PROGRESSION: All children complete - moving to parent")
                        self.logger.info(f"Parent node: {parent_node_info.get('intent', 'Unknown intent')}")
                        self.logger.info("Parent node should now combine results from children")
                        self.logger.info("Tree processing continues - coordinator should process parent node")
                        self.logger.info("="*60)
                    
                    # Check if parent is root and tree processing is complete
                    if not nodes[parent_id].get("parentId"):
                        self.logger.info("="*60)
                        self.logger.info("✅ TREE COMPLETE: All sub-queries executed and parent query ready!")
                        self.logger.info("All nodes in the query tree have good SQL results.")
                        self.logger.info("Parent node can now combine results from all children.")
                        self.logger.info("="*60)
                        await memory.set("tree_complete", True)
                else:
                    self.logger.debug(f"Not all siblings are good yet - staying on {current_node_id}")
                    
        except Exception as e:
            self.logger.error(f"Error in node progression: {str(e)}", exc_info=True)
    
    async def _all_children_good_REMOVED(self, nodes: Dict[str, Any], parent_id: str) -> bool:
        """Check if all children of a node have good quality evaluation."""
        parent_node = nodes.get(parent_id)
        if not parent_node:
            return False
        
        children_ids = parent_node.get("childIds", [])
        if not children_ids:
            # No children - check the node itself
            analysis_key = f"node_{parent_id}_analysis"
            analysis = await self.memory.get(analysis_key)
            if analysis:
                return analysis.get("result_quality") in ["excellent", "good"]
            return False
        
        # Check all children
        for child_id in children_ids:
            analysis_key = f"node_{child_id}_analysis"
            analysis = await self.memory.get(analysis_key)
            if not analysis or analysis.get("result_quality") not in ["excellent", "good"]:
                return False
        
        return True
    
    def _extract_code_block(self, content: str, language: str = "") -> Optional[str]:
        """
        Extract code from markdown code blocks.
        
        Args:
            content: The content to search
            language: Optional language specifier (e.g., "sql", "json")
            
        Returns:
            The code content or None
        """
        if language:
            pattern = f'```{language}\\s*\\n(.*?)\\n```'
        else:
            pattern = r'```(?:\w+)?\\s*\\n(.*?)\\n```'
        
        match = re.search(pattern, content, re.DOTALL)
        return match.group(1).strip() if match else None
    
    def _format_execution_result(self, exec_result: ExecutionResult) -> Dict[str, Any]:
        """Format ExecutionResult for the prompt"""
        result = {
            "status": "success" if not exec_result.error else "error",
            "row_count": exec_result.rowCount,
            "data": exec_result.data
        }
        
        # Add sample data if we have full data
        if isinstance(exec_result.data, list) and exec_result.data:
            result["sample_data"] = exec_result.data[:10]  # First 10 rows
            if exec_result.rowCount > 10:
                result["note"] = f"Showing first 10 of {exec_result.rowCount} rows"
        
        # Add error if present
        if exec_result.error:
            result["error"] = exec_result.error
        
        return result
    
    async def analyze_execution(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Analyze SQL execution results for a node.
        
        Args:
            node_id: The ID of the node to analyze
            
        Returns:
            The analysis results or None if failed
        """
        self.logger.info(f"Analyzing execution for node: {node_id}")
        
        # Set current node ID in QueryTreeManager
        await self.tree_manager.set_current_node_id(node_id)
        
        # Run the agent
        task = f"node:{node_id} - Analyze SQL execution results"
        result = await self.run(task)
        
        # Get the analysis
        analysis = await self.memory.get("execution_analysis")
        return analysis
    
    
    def _parse_evaluation_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the evaluation XML output"""
        try:
            # Extract XML from output
            xml_match = re.search(r'<evaluation>.*?</evaluation>', output, re.DOTALL)
            if not xml_match:
                # Try to find XML in code blocks
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
                    self.logger.error("No evaluation XML found in output")
                    return None
            else:
                xml_content = xml_match.group()
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            result = {
                "answers_intent": root.findtext("answers_intent", "").strip(),
                "result_quality": root.findtext("result_quality", "").strip(),
                "result_summary": root.findtext("result_summary", "").strip(),
                "confidence_score": float(root.findtext("confidence_score", "0.5"))
            }
            
            # Parse issues
            issues = []
            issues_elem = root.find("issues")
            if issues_elem is not None:
                for issue_elem in issues_elem.findall("issue"):
                    issue = {
                        "type": issue_elem.findtext("type", "").strip(),
                        "description": issue_elem.findtext("description", "").strip(),
                        "severity": issue_elem.findtext("severity", "").strip()
                    }
                    if issue["type"] or issue["description"]:  # Only add if not empty
                        issues.append(issue)
            result["issues"] = issues
            
            # Parse suggestions
            suggestions = []
            suggestions_elem = root.find("suggestions")
            if suggestions_elem is not None:
                for suggestion_elem in suggestions_elem.findall("suggestion"):
                    suggestion_text = suggestion_elem.text
                    if suggestion_text and suggestion_text.strip():
                        suggestions.append(suggestion_text.strip())
            result["suggestions"] = suggestions
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing evaluation XML: {str(e)}", exc_info=True)
            return None