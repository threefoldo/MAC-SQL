"""
Task Status Checker for tree orchestration.

This tool checks the overall status of the query tree and determines what should happen next.
It examines all nodes in the tree and provides clear guidance to the coordinator.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from autogen_core.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Union
from keyvalue_memory import KeyValueMemory
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from memory_content_types import NodeStatus


class TaskStatusCheckerArgs(BaseModel):
    """Arguments for the task status checker tool"""
    task: str = Field(default="", description="The task instruction (optional)")


class TaskStatusChecker(BaseTool):
    """
    Checks the overall task status and determines next actions.
    
    This is a simple deterministic function that:
    1. Examines the entire query tree
    2. Identifies which nodes need processing
    3. Determines if the task is complete
    4. Provides clear guidance on what to do next
    
    No LLM calls - output is determined solely by node status.
    """
    
    def __init__(self, memory: KeyValueMemory):
        """Initialize with memory instance"""
        super().__init__(
            name="task_status_checker",
            description="Check task status and determine next action based on query tree state",
            args_type=TaskStatusCheckerArgs,
            return_type=str
        )
        self.memory = memory
        self.tree_manager = QueryTreeManager(memory)
        self.history_manager = NodeHistoryManager(memory)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.MAX_RETRIES = 3  # Maximum retry attempts per node
    
    async def analyze_tree(self) -> Dict[str, Any]:
        """
        Analyze tree and handle simple node selection rules.
        
        Key responsibilities:
        1. Check schema context
        2. Handle current node pointer (empty → root)
        3. Categorize nodes by processing state
        4. Apply simple node movement rules
        5. Track retry limits
        6. Generate status for orchestrator
        """
        
        # Check schema context first (fundamental requirement)
        schema_context = await self.memory.get("schema_linking")
        has_schema_analysis = bool(schema_context and schema_context.get("schema_analysis"))
        
        # Check if tree exists
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree or len(tree["nodes"]) == 0:
            return {
                "tree_exists": False,
                "has_schema_analysis": has_schema_analysis,
                "current_node_id": None,
                "total_nodes": 0,
                "nodes_by_state": {},
                "completion_status": "no_tree"
            }
        
        # Handle current node pointer - simple rule: if empty, set to root
        current_node_id = await self.tree_manager.get_current_node_id()
        root_id = tree.get("rootId")
        
        if not current_node_id and root_id:
            current_node_id = root_id
            await self.tree_manager.set_current_node_id(current_node_id)
            self.logger.info(f"Set current node to root: {current_node_id}")
        
        # Categorize all nodes by state for status reporting
        nodes_by_state = {
            "no_sql": [],
            "need_eval": [], 
            "good_sql": [],
            "bad_sql": [],
            "max_retries": []  # Nodes that hit retry limit
        }
        
        node_details = {}
        
        # Track tree structure for better node selection
        tree_structure = {
            "root_id": root_id,
            "leaf_nodes": [],  # Nodes with no children
            "parent_nodes": [],  # Nodes with children
            "node_depths": {}  # For priority processing
        }
        
        # Categorize each node by processing state and build tree structure
        for node_id, node_data in tree["nodes"].items():
            node_info = {
                "node_id": node_id,
                "intent": node_data.get("intent", ""),
                "has_mapping": bool(node_data.get("mapping")),
                "has_sql": bool(node_data.get("sql")),
                "has_execution": bool(node_data.get("executionResult")),
                "is_root": not node_data.get("parentId"),
                "parent_id": node_data.get("parentId"),
                "children": node_data.get("childIds", []),
                "retry_count": 0,
                "reached_max_retries": False
            }
            
            # Build tree structure info
            if not node_info["children"]:
                tree_structure["leaf_nodes"].append(node_id)
            else:
                tree_structure["parent_nodes"].append(node_id)
            
            # Get retry count for this node
            retry_count = await self.history_manager.get_node_retry_count(node_id)
            node_info["retry_count"] = retry_count
            
            # Check if reached max retries
            if retry_count >= self.MAX_RETRIES:
                node_info["reached_max_retries"] = True
                nodes_by_state["max_retries"].append(node_info)
                node_info["state"] = "max_retries"
            else:
                # Determine node processing state
                if not node_data.get("sql"):
                    nodes_by_state["no_sql"].append(node_info)
                    node_info["state"] = "no_sql"
                    
                elif node_data.get("sql") and not node_data.get("executionResult"):
                    nodes_by_state["need_eval"].append(node_info)
                    node_info["state"] = "need_eval"
                    
                elif node_data.get("sql") and node_data.get("executionResult"):
                    # Check evaluation quality
                    analysis_key = f"node_{node_id}_analysis"
                    analysis = await self.memory.get(analysis_key)
                    
                    if analysis:
                        quality = analysis.get("result_quality", "").lower()
                        node_info["quality"] = quality
                        
                        if quality in ["excellent", "good"]:
                            nodes_by_state["good_sql"].append(node_info)
                            node_info["state"] = "good_sql"
                        else:
                            nodes_by_state["bad_sql"].append(node_info)
                            node_info["state"] = "bad_sql"
                            # Store detailed error info for orchestrator feedback loops
                            exec_result = node_data.get("executionResult", {})
                            node_info["error"] = exec_result.get("error", "")
                            node_info["error_type"] = self._classify_error(exec_result.get("error", ""))
                    else:
                        # Has execution result but no analysis - treat as needs evaluation
                        nodes_by_state["need_eval"].append(node_info)
                        node_info["state"] = "need_eval"
            
            node_details[node_id] = node_info
        
        # Apply simple node movement rules
        current_node_info = node_details.get(current_node_id) if current_node_id else None
        
        # Rule: If current node has good SQL or reached max retries, move to next
        if current_node_info and current_node_info.get("state") in ["good_sql", "max_retries"]:
            next_node = self._select_next_unprocessed_node(nodes_by_state, tree_structure)
            
            if next_node:
                await self.tree_manager.set_current_node_id(next_node)
                current_node_id = next_node
                self.logger.info(f"Moved to next unprocessed node: {next_node}")
        
        # Determine completion status
        completion_status = self._determine_completion_status(nodes_by_state, has_schema_analysis)
        
        return {
            "tree_exists": True,
            "has_schema_analysis": has_schema_analysis,
            "current_node_id": current_node_id,
            "total_nodes": len(tree["nodes"]),
            "nodes_by_state": nodes_by_state,
            "node_details": node_details,
            "tree_structure": tree_structure,
            "completion_status": completion_status,
            "current_node_info": current_node_info,
            "full_tree": tree  # For orchestrator use
        }
    
    def _classify_error(self, error_msg: str) -> str:
        """Classify error message for orchestrator feedback loop decisions."""
        if not error_msg:
            return "unknown"
        
        error_lower = error_msg.lower()
        
        # Schema-related errors
        if any(keyword in error_lower for keyword in [
            "table", "column", "no such", "not found", "doesn't exist", 
            "unknown table", "unknown column", "ambiguous column"
        ]):
            return "schema"
        
        # SQL syntax/logic errors  
        elif any(keyword in error_lower for keyword in [
            "syntax", "group by", "having", "subquery", "join",
            "near", "unexpected", "syntax error"
        ]):
            return "syntax"
        
        # Data type errors
        elif any(keyword in error_lower for keyword in [
            "type", "cast", "convert", "datatype", "mismatch"
        ]):
            return "datatype"
        
        # Empty result (might need different approach)
        elif any(keyword in error_lower for keyword in [
            "empty", "no rows", "zero rows", "no results"
        ]):
            return "empty_result"
        
        else:
            return "unknown"
    
    def _select_next_unprocessed_node(self, nodes_by_state: Dict[str, List], tree_structure: Dict[str, Any]) -> Optional[str]:
        """
        Select next unprocessed node with smart priority:
        1. Leaf nodes first (easier to process)
        2. Then parent nodes
        3. Priority: no_sql > need_eval > bad_sql
        """
        
        # Get all unprocessed nodes
        unprocessed_by_priority = [
            ("no_sql", nodes_by_state["no_sql"]),
            ("need_eval", nodes_by_state["need_eval"]), 
            ("bad_sql", nodes_by_state["bad_sql"])
        ]
        
        # For each priority level, prefer leaf nodes first
        for state_name, node_list in unprocessed_by_priority:
            if not node_list:
                continue
                
            # Separate leaf nodes and parent nodes
            leaf_nodes = [node for node in node_list 
                         if node["node_id"] in tree_structure["leaf_nodes"]]
            parent_nodes = [node for node in node_list 
                           if node["node_id"] in tree_structure["parent_nodes"]]
            
            # Prefer leaf nodes first
            if leaf_nodes:
                return leaf_nodes[0]["node_id"]
            elif parent_nodes:
                return parent_nodes[0]["node_id"]
        
        return None
    
    def _determine_completion_status(self, nodes_by_state: Dict[str, List], has_schema_analysis: bool) -> str:
        """Determine overall completion status."""
        
        if not has_schema_analysis:
            return "need_schema"
        
        # Count active unprocessed nodes (excluding max_retries)
        active_unprocessed = (len(nodes_by_state["no_sql"]) + 
                            len(nodes_by_state["need_eval"]) + 
                            len(nodes_by_state["bad_sql"]))
        
        if active_unprocessed == 0:
            return "complete"
        elif len(nodes_by_state["good_sql"]) > 0 and active_unprocessed > 0:
            return "in_progress"
        else:
            return "processing"
    
    def generate_status_report(self, tree_analysis: Dict[str, Any]) -> str:
        """
        Generate clear, structured status report for orchestrator agent.
        
        The report provides all information needed for feedback loop decisions:
        - Schema context status
        - Tree existence and completion status  
        - Current node details and state
        - Error classification for context changes
        - Processing progress summary
        """
        
        # Handle no tree case
        if not tree_analysis.get("tree_exists"):
            has_schema = tree_analysis.get("has_schema_analysis", False)
            if not has_schema:
                return "STATUS: No schema analysis found\nNEXT: Call schema_linker to analyze schema"
            else:
                return "STATUS: No query tree found\nNEXT: Call query_analyzer to create tree structure"
        
        # Extract key information
        completion_status = tree_analysis.get("completion_status", "unknown")
        current_node_id = tree_analysis.get("current_node_id")
        current_node_info = tree_analysis.get("current_node_info", {})
        nodes_by_state = tree_analysis.get("nodes_by_state", {})
        has_schema = tree_analysis.get("has_schema_analysis", False)
        
        # Build structured status report
        status_lines = []
        
        # Overall status
        if completion_status == "complete":
            status_lines.append("STATUS: All nodes processed successfully")
            status_lines.append("TERMINATE")
            return "\n".join(status_lines)
        elif completion_status == "need_schema":
            status_lines.append("STATUS: No schema analysis found")
            status_lines.append("NEXT: Call schema_linker to analyze schema")
            return "\n".join(status_lines)
        
        # Progress summary
        total_nodes = tree_analysis.get("total_nodes", 0)
        no_sql_count = len(nodes_by_state.get("no_sql", []))
        need_eval_count = len(nodes_by_state.get("need_eval", []))
        good_sql_count = len(nodes_by_state.get("good_sql", []))
        bad_sql_count = len(nodes_by_state.get("bad_sql", []))
        max_retries_count = len(nodes_by_state.get("max_retries", []))
        
        status_lines.extend([
            f"PROGRESS: {good_sql_count}/{total_nodes} nodes complete",
            f"PENDING: {no_sql_count} no_sql, {need_eval_count} need_eval, {bad_sql_count} bad_sql",
            f"FAILED: {max_retries_count} reached max retries"
        ])
        
        # Current node details
        if current_node_id and current_node_info:
            state = current_node_info.get('state', 'unknown')
            intent = current_node_info.get('intent', 'N/A')[:80]
            retry_count = current_node_info.get('retry_count', 0)
            
            status_lines.extend([
                "",
                f"CURRENT NODE: {current_node_id}",
                f"State: {state}",
                f"Intent: {intent}",
                f"Retries: {retry_count}/{self.MAX_RETRIES}"
            ])
            
            # Add state-specific details
            if state == "no_sql":
                status_lines.append("NEXT: Call sql_generator to generate SQL")
                
            elif state == "need_eval":
                status_lines.append("NEXT: Call sql_evaluator to evaluate SQL")
                
            elif state == "bad_sql":
                error = current_node_info.get('error', '')
                error_type = current_node_info.get('error_type', 'unknown')
                quality = current_node_info.get('quality', 'unknown')
                
                status_lines.extend([
                    f"Quality: {quality}",
                    f"Error type: {error_type}",
                    f"Error: {error[:100]}{'...' if len(error) > 100 else ''}"
                ])
                
                # Feedback loop guidance
                if error_type == "schema":
                    status_lines.append("NEXT: Call schema_linker to change schema context")
                elif error_type in ["syntax", "datatype"]:
                    status_lines.append("NEXT: Call query_analyzer to change decomposition context")
                else:
                    status_lines.append("NEXT: Call schema_linker (most common fix)")
                    
            elif state == "good_sql":
                quality = current_node_info.get('quality', 'unknown')
                status_lines.extend([
                    f"Quality: {quality}",
                    "NEXT: TaskStatusChecker will move to next node"
                ])
                
            elif state == "max_retries":
                status_lines.extend([
                    f"Reached max retries ({retry_count})",
                    "NEXT: TaskStatusChecker will move to next node"
                ])
        
        return "\n".join(status_lines)
    
    async def run(self, args: TaskStatusCheckerArgs, cancellation_token=None) -> str:
        """
        Check the current task status and generate status report.
        Simple node selection rules are handled internally.
        
        Args:
            args: TaskStatusCheckerArgs with optional task field
            cancellation_token: Optional cancellation token (for AutoGen compatibility)
        
        Returns:
            A status report for the orchestrator to make agent decisions
        """
        # Handle case where args might be a string (for backward compatibility)
        if isinstance(args, str):
            args = TaskStatusCheckerArgs(task=args)
        
        self.logger.debug("Analyzing tree status and handling node selection...")
        
        # Analyze tree and handle simple node selection
        tree_analysis = await self.analyze_tree()
        
        # Generate status report for orchestrator
        return self.generate_status_report(tree_analysis)
    
    def get_tool(self):
        """Return self as the tool instance for use with agents"""
        return self