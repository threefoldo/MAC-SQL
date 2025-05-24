"""
SQL Evaluator Agent for text-to-SQL workflow.

This agent evaluates SQL query results and provides analysis of the execution.
Note: The actual SQL execution is handled by SQLExecutor, this agent analyzes
the results and provides insights.
"""

import logging
import re
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent, MemoryCallbackHelpers
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

Output your analysis in JSON format:

```json
{
  "answers_intent": true/false,
  "result_quality": "excellent|good|acceptable|poor",
  "result_summary": "Brief description of what the results show",
  "issues": [
    {
      "type": "data_quality|performance|logic|other",
      "description": "Description of the issue",
      "severity": "high|medium|low"
    }
  ],
  "suggestions": [
    "Any suggestions for improvement"
  ],
  "confidence_score": 0.0-1.0
}
```

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
            node_id = await memory.get("current_node_id")
        
        if node_id:
            # Get the node
            node = await self.tree_manager.get_node(node_id)
            if node:
                context["node_id"] = node_id
                context["intent"] = node.intent
                context["sql"] = node.sql
                
                # Get execution result if available
                if node.executionResult:
                    context["execution_result"] = self._format_execution_result(node.executionResult)
                else:
                    # Try to get from memory
                    exec_result = await memory.get("execution_result")
                    if exec_result:
                        context["execution_result"] = exec_result
        else:
            # Try to get SQL and results from memory
            sql = await memory.get("generated_sql")
            if sql:
                context["sql"] = sql
            
            exec_result = await memory.get("execution_result")
            if exec_result:
                context["execution_result"] = exec_result
            
            # Try to get intent
            query = await memory.get("user_query")
            if query:
                context["intent"] = query
        
        # If we have SQL but no execution result, execute it
        if context.get("sql") and not context.get("execution_result"):
            db_path = await memory.get("database_path")
            if db_path:
                self.logger.info("Executing SQL to get results for analysis")
                executor = SQLExecutor(db_path)
                
                try:
                    results, columns = executor.execute_sql(context["sql"], get_columns=True)
                    
                    execution_result = {
                        "status": "success",
                        "columns": columns,
                        "data": results,
                        "row_count": len(results),
                        "execution_time": None
                    }
                    
                    context["execution_result"] = execution_result
                    # Store for future use
                    await memory.set("execution_result", execution_result)
                    
                except Exception as e:
                    execution_result = {
                        "status": "error",
                        "error": str(e),
                        "row_count": 0
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
            # Extract JSON from response
            json_str = MemoryCallbackHelpers.extract_code_block(last_message, "json")
            if not json_str:
                # Try to find JSON without code block
                json_match = re.search(r'\{.*\}', last_message, re.DOTALL)
                if json_match:
                    json_str = json_match.group()
            
            if json_str:
                analysis = json.loads(json_str)
                
                # Store analysis
                await memory.set("execution_analysis", analysis)
                
                # Get node ID
                node_id = None
                if "node:" in task:
                    parts = task.split(" - ", 1)
                    if parts[0].startswith("node:"):
                        node_id = parts[0][5:]
                
                if not node_id:
                    node_id = await memory.get("current_node_id")
                
                if node_id:
                    # Update node status based on analysis
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        # Determine node status based on analysis
                        if analysis.get("answers_intent") and analysis.get("result_quality") in ["excellent", "good"]:
                            new_status = NodeStatus.COMPLETED
                        elif analysis.get("issues") and any(issue["severity"] == "high" for issue in analysis["issues"]):
                            new_status = NodeStatus.FAILED
                        else:
                            new_status = NodeStatus.COMPLETED
                        
                        # Update node status
                        await self.tree_manager.update_node_status(node_id, new_status)
                        
                        # Store analysis separately
                        await memory.set(f"node_{node_id}_analysis", analysis)
                        
                        self.logger.info(f"Updated node {node_id} status to {new_status.value} based on analysis")
                
                self.logger.info(f"Analysis complete - Answers intent: {analysis.get('answers_intent')}, Quality: {analysis.get('result_quality')}")
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON analysis: {str(e)}")
            # Store raw analysis
            await memory.set("execution_analysis_raw", last_message)
        except Exception as e:
            self.logger.error(f"Error in parser callback: {str(e)}", exc_info=True)
    
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
    
    async def execute_and_analyze(self, sql: str, intent: str, db_path: str) -> Dict[str, Any]:
        """
        Execute SQL and analyze results (convenience method).
        
        Args:
            sql: The SQL query to execute
            intent: The original query intent
            db_path: Path to the database
            
        Returns:
            Combined execution and analysis results
        """
        # Execute SQL
        executor = SQLExecutor(db_path)
        
        try:
            results, columns = executor.execute_sql(sql, get_columns=True)
            
            execution_result = {
                "status": "success",
                "columns": columns,
                "data": results,
                "row_count": len(results),
                "sql": sql
            }
            
        except Exception as e:
            execution_result = {
                "status": "error",
                "error": str(e),
                "sql": sql,
                "row_count": 0
            }
        
        # Store in memory for analysis
        await self.memory.set("generated_sql", sql)
        await self.memory.set("execution_result", execution_result)
        await self.memory.set("user_query", intent)
        
        # Run analysis
        result = await self.run(f"Analyze SQL execution results for: {intent}")
        
        # Get analysis
        analysis = await self.memory.get("execution_analysis")
        
        # Combine results
        return {
            "execution": execution_result,
            "analysis": analysis
        }