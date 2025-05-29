"""
Simple Task Status Checker - just check state and recommend next action
"""

import logging
from typing import Dict, Any
from autogen_core.tools import BaseTool
from pydantic import BaseModel, Field
from keyvalue_memory import KeyValueMemory
from query_tree_manager import QueryTreeManager


class TaskStatusCheckerArgs(BaseModel):
    """Arguments for the task status checker tool"""
    goal: str = Field(default="", description="The goal to achieve (optional)")


class TaskStatusChecker(BaseTool):
    """
    Simple status checker - just determine current state and recommend next action.
    """
    
    def __init__(self, memory: KeyValueMemory):
        """Initialize with memory instance"""
        super().__init__(
            name="task_status_checker",
            description="Check current state and choose the current node",
            args_type=TaskStatusCheckerArgs,
            return_type=str
        )
        self.memory = memory
        self.tree_manager = QueryTreeManager(memory)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def run(self, args: TaskStatusCheckerArgs, cancellation_token=None) -> str:
        """Navigate node tree, check all nodes, and report comprehensive status."""
        
        # 1. Check query tree  
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree or len(tree["nodes"]) == 0:
            return "STATUS: No query tree found"
        
        # 2. Analyze all nodes in the tree
        root_id = tree.get("rootId")
        if not root_id:
            return "STATUS: No root node in tree"
        
        # Get comprehensive status of all nodes
        node_statuses = await self._analyze_all_nodes(tree)
        
        # 3. Navigate tree and set current node
        current_node_id = await self._navigate_tree(tree, node_statuses, root_id)
        
        # 4. Generate comprehensive status report
        return self._generate_tree_status_report(tree, node_statuses, current_node_id)
    
    async def _analyze_all_nodes(self, tree):
        """Analyze the status of all nodes in the tree."""
        node_statuses = {}
        
        for node_id, node_data in tree["nodes"].items():
            # Check schema linking
            has_schema_linking = bool(node_data.get("schema_linking"))
            
            # Check SQL generation
            has_sql = bool(node_data.get("generation", {}).get("sql") or node_data.get("sql"))
            
            # Check execution (stored in evaluation field)
            has_execution = bool(node_data.get("evaluation", {}).get("execution_result"))
            
            # Check evaluation quality from node's evaluation field
            quality = "none"
            evaluation = node_data.get("evaluation", {})
            if evaluation:
                quality = evaluation.get("result_quality", "").lower()
            
            # Determine overall node status
            if quality in ["excellent", "good"]:
                status = "complete"
            elif has_execution and quality not in ["excellent", "good"]:
                status = "bad_sql"
            elif has_sql and not has_execution:
                status = "needs_eval"
            else:
                status = "needs_sql"
            
            node_statuses[node_id] = {
                "status": status,
                "has_schema_linking": has_schema_linking,
                "has_sql": has_sql,
                "has_execution": has_execution,
                "quality": quality,
                "intent": node_data.get("intent", ""),
                "children": node_data.get("childIds", []),
                "parent": node_data.get("parentId")
            }
        
        return node_statuses
    
    async def _navigate_tree(self, tree, node_statuses, root_id):
        """Navigate tree according to processing rules and set current node."""
        current_node_id = await self.tree_manager.get_current_node_id()
        
        # Set to root if no current node
        if not current_node_id:
            current_node_id = root_id
            await self.tree_manager.set_current_node_id(current_node_id)
            self.logger.info(f"Set current node to root: {current_node_id}")
            return current_node_id
        
        # Check if current node exists in tree
        if current_node_id not in node_statuses:
            current_node_id = root_id
            await self.tree_manager.set_current_node_id(current_node_id)
            self.logger.info(f"Reset current node to root: {current_node_id}")
            return current_node_id
        
        current_status = node_statuses[current_node_id]
        
        # If current node has unprocessed or bad SQL child nodes, select first child
        for child_id in current_status["children"]:
            child_status = node_statuses.get(child_id, {})
            if child_status.get("status") in ["needs_sql", "needs_eval", "bad_sql"]:
                await self.tree_manager.set_current_node_id(child_id)
                self.logger.info(f"Moved to child node: {child_id}")
                return child_id
        
        # If current node and all children are complete, move to next sibling or parent
        if current_status["status"] == "complete":
            # Check if all children are complete
            all_children_complete = all(
                node_statuses.get(child_id, {}).get("status") == "complete"
                for child_id in current_status["children"]
            )
            
            if all_children_complete:
                # Move to next sibling or parent
                next_node = self._find_next_node(tree, node_statuses, current_node_id)
                if next_node and next_node != current_node_id:
                    await self.tree_manager.set_current_node_id(next_node)
                    self.logger.info(f"Moved to next node: {next_node}")
                    return next_node
        
        # Stay on current node
        return current_node_id
    
    def _find_next_node(self, tree, node_statuses, current_node_id):
        """Find next sibling or parent node to process."""
        current_status = node_statuses[current_node_id]
        parent_id = current_status["parent"]
        
        if parent_id:
            parent_status = node_statuses[parent_id]
            current_index = parent_status["children"].index(current_node_id)
            
            # Check next siblings
            for i in range(current_index + 1, len(parent_status["children"])):
                sibling_id = parent_status["children"][i]
                sibling_status = node_statuses.get(sibling_id, {})
                if sibling_status.get("status") != "complete":
                    return sibling_id
            
            # No incomplete siblings, move to parent
            return self._find_next_node(tree, node_statuses, parent_id)
        
        return current_node_id  # No more nodes to process
    
    def _generate_tree_status_report(self, tree, node_statuses, current_node_id):
        """Generate comprehensive tree status report."""
        total_nodes = len(node_statuses)
        complete_nodes = sum(1 for s in node_statuses.values() if s["status"] == "complete")
        
        # Count nodes by status
        status_counts = {"complete": 0, "needs_sql": 0, "needs_eval": 0, "bad_sql": 0}
        for status_info in node_statuses.values():
            status = status_info["status"]
            if status in status_counts:
                status_counts[status] += 1
        
        # Build report
        report_lines = [
            f"TREE OVERVIEW: {complete_nodes}/{total_nodes} nodes complete",
            f"PENDING: {status_counts['needs_sql']} need SQL, {status_counts['needs_eval']} need eval, {status_counts['bad_sql']} bad SQL",
            f"CURRENT_NODE: {current_node_id}"
        ]
        
        # Add current node details
        current_status = node_statuses.get(current_node_id, {})
        if current_status:
            intent = current_status["intent"][:60] + "..." if len(current_status["intent"]) > 60 else current_status["intent"]
            report_lines.extend([
                f"CURRENT_STATUS: {current_status['status']}",
                f"CURRENT_INTENT: {intent}"
            ])
        
        # Overall completion status
        if complete_nodes == total_nodes:
            report_lines.append("OVERALL_STATUS: All nodes complete")
        else:
            report_lines.append("OVERALL_STATUS: Processing in progress")
        
        return "\n".join(report_lines)
    
    def get_tool(self):
        """Return self as the tool instance for use with agents"""
        return self