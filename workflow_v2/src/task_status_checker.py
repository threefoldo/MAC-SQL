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
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def analyze_tree(self) -> Dict[str, Any]:
        """Analyze the tree and return status information"""
        # Get the complete tree
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree:
            return {"error": "No query tree found"}
        
        # Get current node
        current_node_id = await self.tree_manager.get_current_node_id()
        
        # Analyze tree status
        tree_analysis = {
            "current_node_id": current_node_id,
            "total_nodes": len(tree["nodes"]),
            "processed_good": [],
            "processed_poor": [],
            "unprocessed": [],
            "node_details": {},
            "full_nodes": tree["nodes"]  # Store full node data for detailed access
        }
        
        # Examine each node
        for node_id, node_data in tree["nodes"].items():
            node_info = {
                "node_id": node_id,
                "intent": node_data.get("intent", "")[:80] + "...",
                "has_mapping": bool(node_data.get("mapping")),
                "has_sql": bool(node_data.get("sql")),
                "has_result": bool(node_data.get("executionResult")),
                "is_root": not node_data.get("parentId"),
                "children": node_data.get("childIds", [])
            }
            
            # Check evaluation status
            if node_data.get("sql"):
                analysis_key = f"node_{node_id}_analysis"
                analysis = await self.memory.get(analysis_key)
                if analysis:
                    quality = analysis.get("result_quality", "").lower()
                    node_info["quality"] = quality
                    
                    if quality in ["excellent", "good"]:
                        tree_analysis["processed_good"].append(node_info)
                    else:
                        tree_analysis["processed_poor"].append(node_info)
                else:
                    # Has SQL but no evaluation yet
                    node_info["needs_evaluation"] = True
                    tree_analysis["unprocessed"].append(node_info)
            else:
                # No SQL yet
                tree_analysis["unprocessed"].append(node_info)
            
            tree_analysis["node_details"][node_id] = node_info
        
        # Determine the recommended next node
        if tree_analysis["unprocessed"]:
            # Prioritize nodes that need processing
            # For complex queries, process children before parent
            for node in tree_analysis["unprocessed"]:
                if not node["children"]:  # Leaf nodes first
                    tree_analysis["recommended_node"] = node["node_id"]
                    tree_analysis["recommended_action"] = "process"
                    break
            else:
                # If all unprocessed are parent nodes, pick the first one
                tree_analysis["recommended_node"] = tree_analysis["unprocessed"][0]["node_id"]
                tree_analysis["recommended_action"] = "process"
        
        elif tree_analysis["processed_poor"]:
            # Nodes that need improvement
            tree_analysis["recommended_node"] = tree_analysis["processed_poor"][0]["node_id"]
            tree_analysis["recommended_action"] = "retry"
        
        else:
            # All nodes processed with good results
            tree_analysis["recommended_action"] = "complete"
        
        self.logger.info(f"Tree status: {len(tree_analysis['processed_good'])} good, "
                        f"{len(tree_analysis['processed_poor'])} poor, "
                        f"{len(tree_analysis['unprocessed'])} unprocessed")
        
        return tree_analysis
    
    def generate_action_message(self, tree_analysis: Dict[str, Any]) -> str:
        """Generate the ACTION message based on tree analysis"""
        # Handle error case first
        if tree_analysis.get("error"):
            return f"STATUS REPORT:\n- Error: {tree_analysis['error']}\n\nACTION: ERROR: {tree_analysis['error']}"
        
        # Extract counts
        total = tree_analysis.get("total_nodes", 0)
        good_count = len(tree_analysis.get("processed_good", []))
        poor_count = len(tree_analysis.get("processed_poor", []))
        unprocessed_count = len(tree_analysis.get("unprocessed", []))
        
        # Build status report
        status_report = f"""STATUS REPORT:
- Total nodes: {total}
- Processed with good results: {good_count}
- Needs processing: {unprocessed_count}
- Needs improvement: {poor_count}"""
        
        # Get current node details
        current_node_content = ""
        current_node_id = tree_analysis.get("current_node_id")
        if current_node_id and current_node_id in tree_analysis.get("full_nodes", {}):
            # Get full node data
            full_node = tree_analysis["full_nodes"][current_node_id]
            node_info = tree_analysis["node_details"].get(current_node_id, {})
            
            current_node_content = f"""
CURRENT NODE CONTENT:
- Node ID: {current_node_id}
- Intent: {full_node.get("intent", "No intent")}
- Status: {full_node.get("status", "unknown")}
- Parent ID: {full_node.get("parentId", "None")}
- Children: {full_node.get("childIds", [])}"""
            
            # Add mapping info if available
            if full_node.get("mapping"):
                mapping = full_node["mapping"]
                current_node_content += "\n- Mapping:"
                if mapping.get("tables"):
                    tables = [t.get("name", "unknown") for t in mapping["tables"]]
                    current_node_content += f"\n  - Tables: {', '.join(tables)}"
                if mapping.get("columns"):
                    cols = [f"{c.get('table', 'unknown')}.{c.get('column', 'unknown')}" for c in mapping["columns"][:5]]
                    current_node_content += f"\n  - Columns: {', '.join(cols)}"
                    if len(mapping["columns"]) > 5:
                        current_node_content += f" (and {len(mapping['columns']) - 5} more)"
            
            # Add SQL if available
            if full_node.get("sql"):
                sql_preview = full_node["sql"].strip().replace('\n', ' ')[:150]
                current_node_content += f"\n- SQL: {sql_preview}"
                if len(full_node["sql"]) > 150:
                    current_node_content += "..."
            
            # Add execution result info if available
            if full_node.get("executionResult"):
                exec_result = full_node["executionResult"]
                current_node_content += f"\n- Execution Result:"
                current_node_content += f"\n  - Status: {exec_result.get('status', 'unknown')}"
                current_node_content += f"\n  - Row count: {exec_result.get('rowCount', 0)}"
                if exec_result.get("error"):
                    current_node_content += f"\n  - Error: {exec_result['error']}"
                elif exec_result.get("data") and len(exec_result["data"]) > 0:
                    # Show first row of data
                    first_row = exec_result["data"][0]
                    if isinstance(first_row, dict):
                        preview = str(first_row)[:100]
                    else:
                        preview = str(first_row)[:100]
                    current_node_content += f"\n  - First row: {preview}..."
            
            # Add quality info if available
            if "quality" in node_info:
                current_node_content += f"\n- Evaluation Quality: {node_info['quality']}"
        
        # Determine action based on analysis
        if tree_analysis.get("error"):
            action = f"ACTION: ERROR: {tree_analysis['error']}"
        
        elif tree_analysis["recommended_action"] == "complete":
            action = f"ACTION: TASK COMPLETE: All nodes ({unprocessed_count} + {poor_count} = 0) have been successfully processed"
        
        elif tree_analysis["recommended_action"] == "process":
            node_id = tree_analysis["recommended_node"]
            node_info = tree_analysis["node_details"].get(node_id, {})
            intent = node_info.get("intent", "unknown intent")
            if node_info.get("needs_evaluation"):
                action = f"ACTION: PROCESS NODE: {node_id} needs evaluation of its SQL results"
            else:
                action = f"ACTION: PROCESS NODE: {node_id} needs SQL generation for '{intent}'"
        
        elif tree_analysis["recommended_action"] == "retry":
            node_id = tree_analysis["recommended_node"]
            node_info = tree_analysis["node_details"].get(node_id, {})
            quality = node_info.get("quality", "poor")
            action = f"ACTION: RETRY NODE: {node_id} has {quality} quality results and needs improvement"
        
        else:
            action = "ACTION: ERROR: Unknown tree state"
        
        return f"{status_report}{current_node_content}\n\n{action}"
    
    async def run(self, args: TaskStatusCheckerArgs, cancellation_token=None) -> str:
        """
        Check the current task status and determine next action.
        
        Args:
            args: TaskStatusCheckerArgs with optional task field
            cancellation_token: Optional cancellation token (for AutoGen compatibility)
        
        Returns:
            A status report with clear action directive
        """
        # Handle case where args might be a string (for backward compatibility)
        if isinstance(args, str):
            # Convert string to TaskStatusCheckerArgs
            args = TaskStatusCheckerArgs(task=args)
        
        self.logger.info("Checking overall task status...")
        
        # Analyze the tree
        tree_analysis = await self.analyze_tree()
        
        # Update the current node if we have a recommendation
        if tree_analysis.get("recommended_node") and tree_analysis["recommended_action"] != "complete":
            await self.tree_manager.set_current_node_id(tree_analysis["recommended_node"])
            self.logger.info(f"Updated current node to: {tree_analysis['recommended_node']}")
        
        # Generate and return the action message
        return self.generate_action_message(tree_analysis)
    
    def get_tool(self):
        """Return self as the tool instance for use with agents"""
        return self