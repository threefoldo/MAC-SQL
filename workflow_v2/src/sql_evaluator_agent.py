"""
SQL Evaluator Agent for text-to-SQL tree orchestration.

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
from base_memory_agent import BaseMemoryAgent
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from sql_executor import SQLExecutor
from memory_content_types import (
    QueryNode, NodeStatus, ExecutionResult
)
from utils import parse_xml_hybrid
from success_pattern_agent import SuccessPatternAgent
from failure_pattern_agent import FailurePatternAgent


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
        from task_context_manager import TaskContextManager
        from database_schema_manager import DatabaseSchemaManager
        
        self.tree_manager = QueryTreeManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
        self.task_manager = TaskContextManager(self.memory)
        self.schema_manager = DatabaseSchemaManager(self.memory)
        
        # Initialize intelligent pattern agents
        self.success_pattern_agent = SuccessPatternAgent(self.memory, self.llm_config)
        self.failure_pattern_agent = FailurePatternAgent(self.memory, self.llm_config)
    
    def _build_system_message(self) -> str:
        """Build the system message for SQL result analysis"""
        from prompts.prompt_loader import PromptLoader
        loader = PromptLoader()
        return loader.get_prompt("sql_evaluator", version="v1.2")
    
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
        # Get SQL from generation field
        sql = node.generation.get("sql") if node.generation else None
        context["sql"] = sql
        
        # Get SQL context information from the node (explanation and considerations from SQL generator)
        node_dict = node.to_dict()
        sql_explanation = node_dict.get("sqlExplanation")
        sql_considerations = node_dict.get("sqlConsiderations")
        query_type = node_dict.get("queryType")
        
        if sql_explanation:
            context["sql_explanation"] = sql_explanation
        if sql_considerations:
            context["sql_considerations"] = sql_considerations
        if query_type:
            context["query_type"] = query_type
        
        # Get execution result if available from evaluation field
        if node.evaluation and "execution_result" in node.evaluation:
            context["execution_result"] = self._format_execution_result(node.evaluation["execution_result"])
        else:
            # Execute SQL if we have it
            sql = node.generation.get("sql") if node.generation else None
            if sql:
                # Get task context to get database name
                task_context = await self.task_manager.get()
                if not task_context:
                    self.logger.error("No task context found")
                    return context
                
                db_name = task_context.databaseName
                if not db_name:
                    self.logger.error("No database name in task context")
                    return context
                
                # Get data path and dataset name from database schema
                schema_summary = await self.schema_manager.get_schema_summary()
                data_path = None
                dataset_name = "bird"  # Default to bird
                
                if schema_summary and "metadata" in schema_summary:
                    metadata = schema_summary["metadata"]
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
                    result_dict = executor.execute_sql(sql, db_name)
                    
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
                    
                    # Also store in evaluation field
                    await self.tree_manager.update_node(node_id, {
                        "evaluation": {
                            "execution_result": execution_result
                        }
                    })
                    
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
        self.logger.info(f"Full context: {context}")
        return context
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse the analysis results, update memory, and perform intelligent learning"""
        if not result.messages:
            self.logger.warning("No messages in result")
            return
            
        last_message = result.messages[-1].content
        # Log the raw output for debugging
        self.logger.info(f"Raw LLM output: {last_message}")
        
        try:
            # Extract XML from response
            evaluation_result = self._parse_evaluation_xml(last_message)
            
            if evaluation_result:
                # Analysis is now stored directly in the node's evaluation field
                
                # Get node ID
                node_id = None
                if "node:" in task:
                    parts = task.split(" - ", 1)
                    if parts[0].startswith("node:"):
                        node_id = parts[0][5:]
                
                if not node_id:
                    node_id = await self.tree_manager.get_current_node_id()
                
                if node_id:
                    # Get the current node to preserve existing evaluation data
                    node = await self.tree_manager.get_node(node_id)
                    if node:
                        # Merge the new evaluation result with existing evaluation data
                        existing_evaluation = node.evaluation or {}
                        updated_evaluation = {**existing_evaluation, **evaluation_result}
                        
                        # Store evaluation result in the node's evaluation field
                        await self.tree_manager.update_node(node_id, {"evaluation": updated_evaluation})
                        self.logger.info(f"Stored complete evaluation result in query tree node {node_id}")
                        
                        # INTELLIGENT LEARNING: Call pattern agents to update rules
                        await self._update_pattern_rules(evaluation_result)
                        
                        # Also update the execution_analysis memory as expected by tests/orchestrator
                        execution_context = await self.memory.get("execution_analysis")
                        if execution_context:
                            execution_context["evaluation"] = evaluation_result
                            execution_context["last_update"] = datetime.now().isoformat()
                            await self.memory.set("execution_analysis", execution_context)
                    
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
                    
                    # Log execution status from node's evaluation field
                    if node and node.evaluation and "execution_result" in node.evaluation:
                        exec_result = node.evaluation["execution_result"]
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
                    
                    # Log generator context review if available
                    context_review = evaluation_result.get('generator_context_review')
                    if context_review:
                        self.logger.info(f"  Generator context review:")
                        if context_review.get('generator_reasoning'):
                            self.logger.info(f"    - Generator reasoning: {context_review['generator_reasoning']}")
                        if context_review.get('reasoning_validity'):
                            self.logger.info(f"    - Reasoning validity: {context_review['reasoning_validity'].upper()}")
                        if context_review.get('context_notes'):
                            self.logger.info(f"    - Context notes: {context_review['context_notes']}")
                    
                    # Log issues if any
                    issues = evaluation_result.get('issues', [])
                    if issues:
                        self.logger.info(f"  Issues found:")
                        for issue in issues:
                            if isinstance(issue, dict):
                                self.logger.info(f"    - [{issue.get('severity', 'N/A').upper()}] {issue.get('description', 'N/A')}")
                            else:
                                self.logger.info(f"    - {issue}")
                    
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
            # Try to store raw output in node's evaluation field
            node_id = await self.tree_manager.get_current_node_id()
            if node_id:
                await self.tree_manager.update_node(node_id, {
                    "evaluation": {"raw_output": last_message, "parse_error": str(e)}
                })
    
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
    
    def _format_execution_result(self, exec_result) -> Dict[str, Any]:
        """Format ExecutionResult for the prompt - handles both dict and ExecutionResult object"""
        # Handle dict format (from evaluation field)
        if isinstance(exec_result, dict):
            result = {
                "status": "success" if not exec_result.get("error") else "error",
                "row_count": exec_result.get("rowCount", exec_result.get("row_count", 0)),
                "data": exec_result.get("data", [])
            }
            
            # Add error if present
            if exec_result.get("error"):
                result["error"] = exec_result["error"]
        else:
            # Handle ExecutionResult object
            result = {
                "status": "success" if not exec_result.error else "error",
                "row_count": exec_result.rowCount,
                "data": exec_result.data
            }
            
            # Add error if present
            if exec_result.error:
                result["error"] = exec_result.error
        
        # Add sample data if we have full data
        if isinstance(result["data"], list) and result["data"]:
            result["sample_data"] = result["data"][:10]  # First 10 rows
            if result["row_count"] > 10:
                result["note"] = f"Showing first 10 of {result['row_count']} rows"
        
        return result
    
    def _parse_evaluation_xml(self, output: str) -> Optional[Dict[str, Any]]:
        """Parse the evaluation XML output using hybrid approach with robust error handling"""
        try:
            # Try v1.2 format first
            result = parse_xml_hybrid(output, 'sql_evaluation')
            if result:
                # Convert v1.2 nested structure to flat structure expected by rest of code
                converted = {}
                
                # Safe extraction from intent_alignment with type checking
                if 'intent_alignment' in result:
                    intent_alignment = result['intent_alignment']
                    if isinstance(intent_alignment, dict):
                        converted['answers_intent'] = intent_alignment.get('answers_intent', 'unknown')
                        converted['query_type'] = intent_alignment.get('query_type', 'unknown')
                    else:
                        self.logger.warning(f"intent_alignment is not a dict: {type(intent_alignment)}")
                        converted['answers_intent'] = 'unknown'
                        converted['query_type'] = 'unknown'
                
                # Safe extraction from result_classification with type checking
                if 'result_classification' in result:
                    result_classification = result['result_classification']
                    if isinstance(result_classification, dict):
                        converted['result_quality'] = result_classification.get('overall_assessment', 'unknown')
                        converted['confidence_score'] = result_classification.get('confidence_score', 0.5)
                        converted['continue_workflow'] = result_classification.get('continue_workflow', 'yes')
                        converted['retry_recommended'] = result_classification.get('retry_recommended', 'no')
                    else:
                        self.logger.warning(f"result_classification is not a dict: {type(result_classification)}")
                        converted['result_quality'] = 'unknown'
                        converted['confidence_score'] = 0.5
                        converted['continue_workflow'] = 'yes'
                        converted['retry_recommended'] = 'no'
                
                # Safe extraction from execution_analysis with type checking
                if 'execution_analysis' in result:
                    execution_analysis = result['execution_analysis']
                    if isinstance(execution_analysis, dict):
                        converted['execution_status'] = execution_analysis.get('status', 'unknown')
                        converted['row_count'] = execution_analysis.get('row_count', 0)
                        converted['column_count'] = execution_analysis.get('column_count', 0)
                    else:
                        self.logger.warning(f"execution_analysis is not a dict: {type(execution_analysis)}")
                        converted['execution_status'] = 'unknown'
                        converted['row_count'] = 0
                        converted['column_count'] = 0
                
                # Safely merge other top-level fields
                for key, value in result.items():
                    if key not in converted and key not in ['intent_alignment', 'result_classification', 'execution_analysis']:
                        converted[key] = value
                
                result = converted
            else:
                # Fallback to v1.0/v1.1 format
                result = parse_xml_hybrid(output, 'evaluation')
            
            if result:
                # Safely convert confidence_score to float with proper error handling
                confidence_score = result.get("confidence_score", 0.5)
                try:
                    if confidence_score is not None and str(confidence_score).strip():
                        result["confidence_score"] = float(confidence_score)
                    else:
                        result["confidence_score"] = 0.5
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Failed to convert confidence_score '{confidence_score}' to float: {str(e)}")
                    result["confidence_score"] = 0.5
                
                # Ensure all critical fields have defaults
                result.setdefault('answers_intent', 'unknown')
                result.setdefault('result_quality', 'unknown')
                result.setdefault('execution_status', 'unknown')
                result.setdefault('row_count', 0)
                result.setdefault('column_count', 0)
                
                return result
            
        except Exception as e:
            self.logger.error(f"Error in XML parsing: {str(e)}", exc_info=True)
        
        return None
    
    async def _perform_intelligent_learning(self, node_id: str, node: Any, evaluation_result: Dict[str, Any]) -> None:
        """
        Perform intelligent learning using dedicated LLM pattern agents.
        
        This method delegates pattern analysis to specialized Success and Failure Pattern Agents
        that maintain intelligent repositories for each database.
        """
        try:
            # 1. Extract complete workflow context
            workflow_context = await self._extract_complete_workflow_context(node)
            
            # 2. Get database name for pattern agents
            task_context = await self.task_manager.get()
            database_name = task_context.databaseName if task_context else "unknown"
            
            # 3. Prepare execution data for pattern agents
            execution_data = {
                "database_name": database_name,
                "workflow_context": workflow_context,
                "evaluation_result": evaluation_result,
                "node_id": node_id,
                "timestamp": datetime.now().isoformat()
            }
            
            # 4. Determine if execution was successful or failed
            result_quality = evaluation_result.get("result_quality", "unknown")
            answers_intent = evaluation_result.get("answers_intent", "unknown")
            execution_status = workflow_context.get("evaluation", {}).get("execution_result", {}).get("status", "unknown")
            
            is_successful = (
                execution_status == "success" and
                result_quality in ["excellent", "good"] and
                answers_intent == "yes"
            )
            
            # 5. Delegate to appropriate pattern agent
            if is_successful:
                await self._analyze_with_success_agent(execution_data)
                self.logger.info(f"Success pattern analysis initiated for node {node_id}")
            else:
                await self._analyze_with_failure_agent(execution_data)
                self.logger.info(f"Failure pattern analysis initiated for node {node_id}")
            
            # 6. Generate and store strategic guidance from pattern repositories
            await self._generate_guidance_from_patterns(node_id, database_name)
            
            self.logger.info(f"Intelligent learning with pattern agents completed for node {node_id}")
            
        except Exception as e:
            self.logger.error(f"Error in intelligent learning: {str(e)}", exc_info=True)
    
    async def _update_pattern_rules(self, evaluation_result: Dict[str, Any]) -> None:
        """Update pattern rules based on evaluation result"""
        try:
            # Determine if execution was successful or failed based on evaluation
            result_quality = evaluation_result.get("result_quality", "unknown")
            answers_intent = evaluation_result.get("answers_intent", "unknown")
            
            # Check execution status from current node evaluation
            node_id = await self.tree_manager.get_current_node_id()
            if not node_id:
                self.logger.error("No current node ID for pattern analysis")
                return
            
            node = await self.tree_manager.get_node(node_id)
            execution_status = "unknown"
            if node and node.evaluation and "execution_result" in node.evaluation:
                execution_status = node.evaluation["execution_result"].get("status", "unknown")
            
            # Determine if this was a success or failure
            is_successful = (
                execution_status == "success" and
                result_quality in ["excellent", "good"] and
                answers_intent == "yes"
            )
            
            # Call appropriate pattern agent (they will read context from memory managers)
            if is_successful:
                await self.success_pattern_agent.run(goal="analyze_successful_execution")
                self.logger.info(f"Success pattern agent called for node {node_id} - updating DO rules")
            else:
                await self.failure_pattern_agent.run(goal="analyze_failed_execution")
                self.logger.info(f"Failure pattern agent called for node {node_id} - updating DON'T rules")
            
        except Exception as e:
            self.logger.error(f"Error updating pattern rules: {str(e)}", exc_info=True)
    
    async def _generate_guidance_from_patterns(self, node_id: str, database_name: str) -> None:
        """Generate strategic guidance by querying pattern repositories"""
        try:
            # Get guidance from both pattern agents for all agent types
            agent_types = ["schema_linker", "sql_generator", "query_analyzer"]
            
            strategic_guidance = {
                f"{agent_type}_guidance": [] for agent_type in agent_types
            }
            strategic_guidance["orchestrator_guidance"] = []
            
            # Query both success and failure pattern agents for guidance
            for agent_type in agent_types:
                # Get success-based guidance
                success_guidance = await self.success_pattern_agent.get_success_guidance(
                    self.memory, database_name, agent_type
                )
                if success_guidance:
                    strategic_guidance[f"{agent_type}_guidance"].append(f"SUCCESS PATTERNS:\n{success_guidance}")
                
                # Get failure-avoidance guidance
                failure_guidance = await self.failure_pattern_agent.get_failure_avoidance_guidance(
                    self.memory, database_name, agent_type
                )
                if failure_guidance:
                    strategic_guidance[f"{agent_type}_guidance"].append(f"AVOID FAILURES:\n{failure_guidance}")
            
            # Store strategic guidance for agents to access
            await self._store_strategic_guidance(node_id, strategic_guidance)
            
            self.logger.info(f"Strategic guidance generated from pattern repositories for {database_name}")
            
        except Exception as e:
            self.logger.error(f"Error generating guidance from patterns: {str(e)}", exc_info=True)
    
    async def _extract_complete_workflow_context(self, node: Any) -> Dict[str, Any]:
        """Extract complete workflow context for learning analysis"""
        context = {
            "original_query": node.intent,
            "evidence": node.evidence,
            "schema_linking": node.schema_linking,
            "generation": node.generation,
            "evaluation": node.evaluation,
            "generation_attempts": node.generation_attempts,
            "node_history": []
        }
        
        # Get node operation history for deeper analysis
        node_history = await self.history_manager.get_node_operations(node.nodeId)
        if node_history:
            context["node_history"] = [op.to_dict() for op in node_history]
        
        # Get task context for broader perspective
        task_context = await self.task_manager.get()
        if task_context:
            context["task_context"] = task_context.to_dict()
        
        return context
    
    def _analyze_workflow_patterns(self, workflow_context: Dict[str, Any], evaluation_result: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze workflow patterns to identify learning opportunities"""
        
        patterns = {
            "success_indicators": [],
            "failure_patterns": [],
            "agent_performance": {
                "schema_linker": {"issues": [], "successes": []},
                "query_analyzer": {"issues": [], "successes": []}, 
                "sql_generator": {"issues": [], "successes": []}
            },
            "systemic_issues": [],
            "improvement_opportunities": []
        }
        
        # Analyze evaluation results
        result_quality = evaluation_result.get("result_quality", "unknown")
        answers_intent = evaluation_result.get("answers_intent", "unknown")
        
        if result_quality in ["excellent", "good"] and answers_intent == "yes":
            patterns["success_indicators"].extend([
                "sql_execution_successful",
                "results_match_intent", 
                "quality_assessment_positive"
            ])
            
            # Analyze what made this successful
            if workflow_context.get("generation", {}).get("query_type") == "simple":
                patterns["agent_performance"]["sql_generator"]["successes"].append("simple_query_approach")
            
            schema_tables = self._extract_schema_tables(workflow_context.get("schema_linking", {}))
            if len(schema_tables) <= 2:
                patterns["agent_performance"]["schema_linker"]["successes"].append("minimal_table_selection")
                
        else:
            # Analyze failure patterns
            patterns["failure_patterns"].append(f"quality_{result_quality}")
            patterns["failure_patterns"].append(f"intent_match_{answers_intent}")
            
            # Specific failure analysis
            failure_analysis = self._analyze_specific_failures(workflow_context, evaluation_result)
            patterns["agent_performance"].update(failure_analysis["agent_issues"])
            patterns["systemic_issues"].extend(failure_analysis["systemic_issues"])
            patterns["improvement_opportunities"].extend(failure_analysis["improvements"])
        
        return patterns
    
    def _analyze_specific_failures(self, workflow_context: Dict[str, Any], evaluation_result: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze specific failure modes for targeted learning"""
        
        analysis = {
            "agent_issues": {
                "schema_linker": {"issues": [], "successes": []},
                "query_analyzer": {"issues": [], "successes": []},
                "sql_generator": {"issues": [], "successes": []}
            },
            "systemic_issues": [],
            "improvements": []
        }
        
        # Analyze execution result details
        execution_result = workflow_context.get("evaluation", {}).get("execution_result", {})
        generated_sql = workflow_context.get("generation", {}).get("sql", "")
        
        if execution_result.get("status") == "error":
            error_msg = execution_result.get("error", "").lower()
            
            if "column" in error_msg or "no such column" in error_msg:
                analysis["agent_issues"]["schema_linker"]["issues"].append("column_mapping_error")
                analysis["improvements"].append("improve_column_precision_mapping")
                
            if "table" in error_msg or "no such table" in error_msg:
                analysis["agent_issues"]["schema_linker"]["issues"].append("table_selection_error")
                analysis["improvements"].append("improve_table_selection_logic")
        
        # Analyze SQL complexity vs success
        if "subquery" in generated_sql.lower() or "select" in generated_sql.lower().count("select") > 1:
            if evaluation_result.get("result_quality") == "poor":
                analysis["agent_issues"]["sql_generator"]["issues"].append("over_complex_sql")
                analysis["improvements"].append("prefer_simple_queries")
        
        # Analyze column precision issues
        execution_data = execution_result.get("data", [])
        if execution_data and len(execution_data) > 0:
            if isinstance(execution_data[0], (list, tuple)):
                column_count = len(execution_data[0])
                if column_count > 3:  # Heuristic for too many columns
                    analysis["agent_issues"]["sql_generator"]["issues"].append("extra_columns_selected")
                    analysis["improvements"].append("enforce_column_precision")
        
        # Analyze evidence adherence
        evidence = workflow_context.get("evidence", "")
        if evidence and "=" in evidence:  # Evidence contains formulas
            if "cast" not in generated_sql.lower() and "%" in evidence.lower():
                analysis["agent_issues"]["sql_generator"]["issues"].append("evidence_formula_ignored")
                analysis["improvements"].append("follow_evidence_formulas_exactly")
        
        return analysis
    
    def _extract_schema_tables(self, schema_linking: Dict[str, Any]) -> List[str]:
        """Extract table names from schema linking result"""
        tables = []
        
        if "selected_tables" in schema_linking:
            selected_tables = schema_linking["selected_tables"]
            if isinstance(selected_tables, dict) and "table" in selected_tables:
                table_data = selected_tables["table"]
                if isinstance(table_data, list):
                    tables = [t.get("name", "") for t in table_data if isinstance(t, dict)]
                elif isinstance(table_data, dict):
                    tables = [table_data.get("name", "")]
        
        return [t for t in tables if t]
    
    async def _update_learning_repository(self, pattern_analysis: Dict[str, Any]) -> None:
        """Update the persistent learning repository with new patterns"""
        
        # Get existing learning repository
        learning_repo = await self.memory.get("intelligent_learning_repository", {
            "success_patterns": {},
            "failure_patterns": {},
            "agent_performance_history": {},
            "strategic_guidance": {},
            "pattern_frequency": {}
        })
        
        # Update success patterns
        for success in pattern_analysis["success_indicators"]:
            if success not in learning_repo["success_patterns"]:
                learning_repo["success_patterns"][success] = {"count": 0, "contexts": []}
            learning_repo["success_patterns"][success]["count"] += 1
        
        # Update failure patterns  
        for failure in pattern_analysis["failure_patterns"]:
            if failure not in learning_repo["failure_patterns"]:
                learning_repo["failure_patterns"][failure] = {"count": 0, "contexts": []}
            learning_repo["failure_patterns"][failure]["count"] += 1
        
        # Update agent performance tracking
        for agent_name, performance in pattern_analysis["agent_performance"].items():
            if agent_name not in learning_repo["agent_performance_history"]:
                learning_repo["agent_performance_history"][agent_name] = {"issues": {}, "successes": {}}
            
            for issue in performance["issues"]:
                if issue not in learning_repo["agent_performance_history"][agent_name]["issues"]:
                    learning_repo["agent_performance_history"][agent_name]["issues"][issue] = 0
                learning_repo["agent_performance_history"][agent_name]["issues"][issue] += 1
            
            for success in performance["successes"]:
                if success not in learning_repo["agent_performance_history"][agent_name]["successes"]:
                    learning_repo["agent_performance_history"][agent_name]["successes"][success] = 0
                learning_repo["agent_performance_history"][agent_name]["successes"][success] += 1
        
        # Store updated repository
        await self.memory.set("intelligent_learning_repository", learning_repo)
        
    def _generate_strategic_guidance(self, pattern_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate strategic guidance for future agent operations"""
        
        guidance = {
            "schema_linker_guidance": [],
            "sql_generator_guidance": [],
            "query_analyzer_guidance": [],
            "orchestrator_guidance": []
        }
        
        # Schema Linker guidance
        schema_issues = pattern_analysis["agent_performance"]["schema_linker"]["issues"]
        if "column_mapping_error" in schema_issues:
            guidance["schema_linker_guidance"].append(
                "CRITICAL: Previous attempts had column mapping errors. Verify all columns exist in selected tables."
            )
        if "table_selection_error" in schema_issues:
            guidance["schema_linker_guidance"].append(
                "CRITICAL: Previous attempts had table selection errors. Double-check table names against schema."
            )
            
        # SQL Generator guidance
        sql_issues = pattern_analysis["agent_performance"]["sql_generator"]["issues"]
        if "over_complex_sql" in sql_issues:
            guidance["sql_generator_guidance"].append(
                "CRITICAL: Previous attempts were over-complex and failed. Prefer simple ORDER BY LIMIT over subqueries."
            )
        if "extra_columns_selected" in sql_issues:
            guidance["sql_generator_guidance"].append(
                "CRITICAL: Previous attempts selected too many columns. Only select columns explicitly requested."
            )
        if "evidence_formula_ignored" in sql_issues:
            guidance["sql_generator_guidance"].append(
                "CRITICAL: Previous attempts ignored evidence formulas. Implement evidence calculations exactly as specified."
            )
            
        # Orchestrator guidance
        if len(pattern_analysis["failure_patterns"]) > 2:
            guidance["orchestrator_guidance"].append(
                "Consider schema re-linking if multiple failures occur."
            )
        
        return guidance
    
    async def _store_strategic_guidance(self, node_id: str, strategic_guidance: Dict[str, Any]) -> None:
        """Store strategic guidance for other agents to access"""
        
        # Store node-specific guidance
        guidance_key = f"node_{node_id}_strategic_guidance"
        await self.memory.set(guidance_key, strategic_guidance)
        
        # Store global guidance that accumulates across nodes
        global_guidance = await self.memory.get("global_strategic_guidance", {
            "schema_linker_guidance": [],
            "sql_generator_guidance": [],
            "query_analyzer_guidance": [],
            "orchestrator_guidance": []
        })
        
        # Merge new guidance (avoid duplicates)
        for agent_type, new_guidance_list in strategic_guidance.items():
            if agent_type in global_guidance:
                for guidance_item in new_guidance_list:
                    if guidance_item not in global_guidance[agent_type]:
                        global_guidance[agent_type].append(guidance_item)
        
        await self.memory.set("global_strategic_guidance", global_guidance)
        
        self.logger.info(f"Strategic guidance stored for node {node_id} and updated global guidance")