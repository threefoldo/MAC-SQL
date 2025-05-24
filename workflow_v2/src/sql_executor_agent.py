"""
SQL Executor Agent for text-to-SQL workflow.

This agent executes generated SQL queries, evaluates results, and provides
improvement suggestions.
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
from sql_executor import SQLExecutor
from memory_content_types import (
    QueryNode, ExecutionResult, NodeStatus
)
from memory_agent_tool import MemoryAgentTool


class SQLExecutorAgent:
    """
    Executes SQL queries and evaluates results.
    
    This agent:
    1. Executes SQL from query nodes
    2. Analyzes execution results
    3. Identifies potential issues
    4. Provides improvement suggestions
    """
    
    def __init__(self,
                 memory: KeyValueMemory,
                 sql_executor: SQLExecutor,
                 model_name: str = "gpt-4o",
                 debug: bool = False):
        """
        Initialize the SQL executor agent.
        
        Args:
            memory: The KeyValueMemory instance
            sql_executor: The SQL executor instance
            model_name: The LLM model to use
            debug: Whether to enable debug logging
        """
        self.memory = memory
        self.sql_executor = sql_executor
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
        """Setup the SQL executor agent with memory callbacks."""
        
        # Agent signature
        signature = """
        Evaluate SQL execution results and provide improvement suggestions.
        
        You will:
        1. Analyze execution results (success/failure)
        2. Check if results match the query intent
        3. Identify performance issues
        4. Suggest improvements if needed
        
        Input:
        - node_id: The query node ID
        - intent: The original query intent
        - sql: The executed SQL
        - execution_result: Result from SQL execution
        - execution_time: Time taken to execute
        
        Output:
        XML format with evaluation and suggestions
        """
        
        # Instructions for the agent
        instructions = """
        You are an SQL execution evaluator and optimizer. Your job is to:
        
        1. Evaluate execution results:
           - Check if execution was successful
           - Verify results match the intent
           - Assess data quality and completeness
           - Identify any anomalies
        
        2. Analyze performance:
           - Execution time relative to data size
           - Potential bottlenecks
           - Missing indexes
           - Inefficient query patterns
        
        3. Provide improvement suggestions:
           - Query optimization techniques
           - Index recommendations
           - Alternative query approaches
           - Data validation issues
        
        Output your evaluation in this XML format:
        
        <execution_evaluation>
          <status>success|failure|partial</status>
          
          <result_analysis>
            <matches_intent>true|false</matches_intent>
            <explanation>Why the result does/doesn't match intent</explanation>
            <data_quality>good|acceptable|poor</data_quality>
            <anomalies>Any unusual patterns or issues found</anomalies>
          </result_analysis>
          
          <performance_analysis>
            <execution_time_assessment>fast|acceptable|slow</execution_time_assessment>
            <bottlenecks>Identified performance issues</bottlenecks>
          </performance_analysis>
          
          <improvements>
            <suggestion priority="high|medium|low">
              <type>optimization|index|rewrite|validation</type>
              <description>Detailed suggestion</description>
              <example>Example implementation if applicable</example>
            </suggestion>
          </improvements>
          
          <final_verdict>
            <usable>true|false</usable>
            <confidence>high|medium|low</confidence>
            <summary>Brief summary of the evaluation</summary>
          </final_verdict>
        </execution_evaluation>
        """
        
        # Create the agent
        self.agent = MemoryAgentTool(
            name="sql_executor_evaluator",
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
            Enhanced inputs with execution context
        """
        node_id = inputs.get("node_id")
        if not node_id:
            raise ValueError("node_id is required")
        
        # Get the node
        node = await self.tree_manager.get_node(node_id)
        if not node:
            raise ValueError(f"Node {node_id} not found")
        
        if not node.sql:
            raise ValueError(f"Node {node_id} has no SQL to execute")
        
        # Execute the SQL
        try:
            start_time = datetime.now()
            result = await self._execute_sql(node.sql)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Update node with execution result
            exec_result = ExecutionResult(
                data=result.get("data", []),
                rowCount=result.get("row_count", 0),
                error=result.get("error")
            )
            
            success = not bool(result.get("error"))
            await self.tree_manager.update_node_result(node_id, exec_result, success)
            
            # Record execution in history
            await self.history_manager.record_execute(
                node_id=node_id,
                sql=node.sql,
                result=result.get("data") if success else None,
                error=result.get("error")
            )
            
            # Prepare inputs for evaluation
            inputs["intent"] = node.intent
            inputs["sql"] = node.sql
            inputs["execution_result"] = result
            inputs["execution_time"] = execution_time
            
            # Add schema context
            if node.mapping and node.mapping.tables:
                table_info = []
                for table in node.mapping.tables:
                    metadata = await self.schema_manager.get_table_metadata(table.name)
                    if metadata:
                        table_info.append({
                            "name": table.name,
                            "row_count": metadata.get("rowCount", "unknown"),
                            "indexes": metadata.get("indexes", [])
                        })
                inputs["table_metadata"] = table_info
            
        except Exception as e:
            self.logger.error(f"Error executing SQL: {str(e)}", exc_info=True)
            inputs["execution_error"] = str(e)
            
            # Update node with error
            exec_result = ExecutionResult(
                data=[],
                rowCount=0,
                error=str(e)
            )
            await self.tree_manager.update_node_result(node_id, exec_result, False)
        
        self.logger.debug(f"Pre-callback: Executed SQL for node {node_id}")
        
        return inputs
    
    async def _post_callback(self, output: str, original_inputs: Dict[str, Any]) -> str:
        """
        Post-processing callback to parse evaluation and store suggestions.
        
        Args:
            output: The agent's output
            original_inputs: The original inputs
            
        Returns:
            The processed output
        """
        try:
            node_id = original_inputs["node_id"]
            
            # Parse the evaluation output
            evaluation = self._parse_evaluation_xml(output)
            
            if evaluation:
                # Store evaluation results
                await self.memory.set(f"sql_evaluation_{node_id}", evaluation)
                
                # Log high-priority improvements
                high_priority = [s for s in evaluation.get("improvements", []) 
                               if s.get("priority") == "high"]
                if high_priority:
                    self.logger.warning(f"High-priority improvements for node {node_id}: "
                                      f"{len(high_priority)} suggestions")
                
                self.logger.info(f"SQL evaluation completed for node {node_id}: "
                               f"Status={evaluation.get('status')}, "
                               f"Usable={evaluation.get('final_verdict', {}).get('usable')}")
                
        except Exception as e:
            self.logger.error(f"Error in post-callback: {str(e)}", exc_info=True)
        
        return output
    
    async def _execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL and return results."""
        try:
            # Execute the SQL
            rows = self.sql_executor.execute(sql)
            
            return {
                "success": True,
                "data": rows,
                "row_count": len(rows),
                "columns": list(rows[0].keys()) if rows else []
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "row_count": 0
            }
    
    def _parse_evaluation_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the execution evaluation XML output."""
        try:
            # Extract XML from output
            xml_match = re.search(r'<execution_evaluation>.*?</execution_evaluation>', output, re.DOTALL)
            if not xml_match:
                # Try to find XML in code blocks
                xml_match = re.search(r'```xml\s*\n(.*?)\n```', output, re.DOTALL)
                if xml_match:
                    xml_content = xml_match.group(1)
                else:
                    self.logger.error("No execution_evaluation XML found in output")
                    return None
            else:
                xml_content = xml_match.group()
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            evaluation = {
                "status": root.findtext("status", "").strip(),
                "result_analysis": {},
                "performance_analysis": {},
                "improvements": [],
                "final_verdict": {}
            }
            
            # Parse result analysis
            result_elem = root.find("result_analysis")
            if result_elem is not None:
                evaluation["result_analysis"] = {
                    "matches_intent": result_elem.findtext("matches_intent", "").strip() == "true",
                    "explanation": result_elem.findtext("explanation", "").strip(),
                    "data_quality": result_elem.findtext("data_quality", "").strip(),
                    "anomalies": result_elem.findtext("anomalies", "").strip()
                }
            
            # Parse performance analysis
            perf_elem = root.find("performance_analysis")
            if perf_elem is not None:
                evaluation["performance_analysis"] = {
                    "execution_time_assessment": perf_elem.findtext("execution_time_assessment", "").strip(),
                    "bottlenecks": perf_elem.findtext("bottlenecks", "").strip()
                }
            
            # Parse improvements
            improvements_elem = root.find("improvements")
            if improvements_elem is not None:
                for suggestion_elem in improvements_elem.findall("suggestion"):
                    suggestion = {
                        "priority": suggestion_elem.get("priority", "medium"),
                        "type": suggestion_elem.findtext("type", "").strip(),
                        "description": suggestion_elem.findtext("description", "").strip(),
                        "example": suggestion_elem.findtext("example", "").strip()
                    }
                    evaluation["improvements"].append(suggestion)
            
            # Parse final verdict
            verdict_elem = root.find("final_verdict")
            if verdict_elem is not None:
                evaluation["final_verdict"] = {
                    "usable": verdict_elem.findtext("usable", "").strip() == "true",
                    "confidence": verdict_elem.findtext("confidence", "").strip(),
                    "summary": verdict_elem.findtext("summary", "").strip()
                }
            
            return evaluation
            
        except Exception as e:
            self.logger.error(f"Error parsing execution evaluation XML: {str(e)}", exc_info=True)
            return None
    
    async def execute_and_evaluate(self, node_id: str) -> Dict[str, Any]:
        """
        Execute SQL for a node and evaluate the results.
        
        Args:
            node_id: The node ID to execute SQL for
            
        Returns:
            Execution and evaluation results
        """
        self.logger.info(f"Executing and evaluating SQL for node {node_id}")
        
        # Check if node has SQL
        node = await self.tree_manager.get_node(node_id)
        if not node:
            return {"error": "Node not found"}
        
        if not node.sql:
            return {"error": "Node has no SQL to execute"}
        
        # Run the agent (execution happens in pre_callback)
        result = await self.agent.run({"node_id": node_id})
        
        # Get the evaluation result
        evaluation = await self.memory.get(f"sql_evaluation_{node_id}")
        
        # Get the execution result from node
        node = await self.tree_manager.get_node(node_id)  # Refresh node
        
        return {
            "node_id": node_id,
            "execution": {
                "success": node.status == NodeStatus.EXECUTED_SUCCESS,
                "row_count": node.executionResult.rowCount if node.executionResult else 0,
                "error": node.executionResult.error if node.executionResult else None
            },
            "evaluation": evaluation
        }
    
    async def execute_all_generated_sql(self) -> Dict[str, Any]:
        """Execute and evaluate all nodes with generated SQL."""
        executable_nodes = await self.tree_manager.get_executable_nodes()
        
        results = {
            "executed": [],
            "failed": [],
            "skipped": []
        }
        
        for node in executable_nodes:
            try:
                result = await self.execute_and_evaluate(node.nodeId)
                if result.get("execution", {}).get("success"):
                    results["executed"].append(node.nodeId)
                else:
                    results["failed"].append(node.nodeId)
            except Exception as e:
                self.logger.error(f"Failed to execute SQL for node {node.nodeId}: {str(e)}")
                results["failed"].append(node.nodeId)
        
        return results
    
    async def get_execution_summary(self, node_id: str) -> Dict[str, Any]:
        """Get a summary of execution and evaluation for a node."""
        node = await self.tree_manager.get_node(node_id)
        if not node:
            return {"error": "Node not found"}
        
        summary = {
            "node_id": node_id,
            "intent": node.intent,
            "has_sql": bool(node.sql),
            "status": node.status.value,
            "execution": None,
            "evaluation": None
        }
        
        if node.executionResult:
            summary["execution"] = {
                "row_count": node.executionResult.rowCount,
                "has_error": bool(node.executionResult.error),
                "error": node.executionResult.error
            }
        
        evaluation = await self.memory.get(f"sql_evaluation_{node_id}")
        if evaluation:
            summary["evaluation"] = {
                "status": evaluation.get("status"),
                "matches_intent": evaluation.get("result_analysis", {}).get("matches_intent"),
                "usable": evaluation.get("final_verdict", {}).get("usable"),
                "improvement_count": len(evaluation.get("improvements", [])),
                "high_priority_improvements": len([s for s in evaluation.get("improvements", []) 
                                                 if s.get("priority") == "high"])
            }
        
        return summary
    
    async def get_improvement_suggestions(self, node_id: str) -> List[Dict[str, Any]]:
        """Get improvement suggestions for a node's SQL."""
        evaluation = await self.memory.get(f"sql_evaluation_{node_id}")
        if not evaluation:
            return []
        
        return evaluation.get("improvements", [])