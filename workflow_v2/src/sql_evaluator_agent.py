"""
SQL Evaluator Agent for text-to-SQL workflow.

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
        return """You are an expert at analyzing SQL query execution results. Your job is to:

1. Evaluate if the SQL results correctly answer the original query intent
2. Check data quality and reasonableness of results
3. Identify any potential issues or unexpected outcomes
4. Suggest improvements if needed

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

Be constructive and specific in your analysis."""
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read context from memory before analyzing results"""
        context = {}
        
        # Extract node_id from task
        node_id = None
        if "node:" in task:
            parts = task.split(" - ", 1)
            if parts[0].startswith("node:"):
                node_id = parts[0][5:]
        
        if not node_id:
            self.logger.error("No node_id found in task. Task must be in format 'node:{node_id} - ...'")
            return context
        
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
                
                self.logger.info(f"Executing SQL for node {node_id} on database {db_name}")
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
                    node_id = await memory.get("current_node_id")
                
                if node_id:
                    # Store analysis for the node
                    await memory.set(f"node_{node_id}_analysis", evaluation_result)
                    
                    # Note: Node status is already updated when execution result is stored
                    # The status (EXECUTED_SUCCESS or EXECUTED_FAILED) is set based on whether
                    # the SQL execution had errors, not based on the quality of results
                    
                    self.logger.info(f"Stored analysis for node {node_id} - Answers intent: {evaluation_result.get('answers_intent')}, Quality: {evaluation_result.get('result_quality')}")
                
                self.logger.info(f"Analysis complete - Answers intent: {evaluation_result.get('answers_intent')}, Quality: {evaluation_result.get('result_quality')}")
                
        except Exception as e:
            self.logger.error(f"Error parsing evaluation results: {str(e)}", exc_info=True)
            # Store raw analysis
            await memory.set("execution_analysis_raw", last_message)
    
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
        
        # Store node ID in memory
        await self.memory.set("current_node_id", node_id)
        
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