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
        
        self.logger.info("="*60)
        self.logger.info("TASK STATUS CHECKER - Starting Analysis")
        self.logger.info("="*60)
        
        # 1. Check query tree  
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree or len(tree["nodes"]) == 0:
            status = "STATUS: No query tree found"
            self.logger.warning(status)
            return status
        
        # 2. Analyze all nodes in the tree
        root_id = tree.get("rootId")
        if not root_id:
            status = "STATUS: No root node in tree"
            self.logger.warning(status)
            return status
        
        self.logger.info(f"Analyzing tree with {len(tree['nodes'])} nodes")
        
        # Get comprehensive status of all nodes
        node_statuses = await self._analyze_all_nodes(tree)
        
        # 3. Navigate tree and set current node
        current_node_id = await self._navigate_tree(tree, node_statuses, root_id)
        
        # 4. Generate comprehensive status report
        status_report = self._generate_tree_status_report(tree, node_statuses, current_node_id)
        
        self.logger.info("TASK STATUS REPORT:")
        self.logger.info(status_report)
        self.logger.info("="*60)
        
        return status_report
    
    async def _analyze_all_nodes(self, tree):
        """Analyze the status of all nodes in the tree."""
        node_statuses = {}
        
        # First pass: analyze individual node status
        for node_id, node_data in tree["nodes"].items():
            # Check schema linking
            has_schema_linking = bool(node_data.get("schema_linking"))
            
            # Check SQL generation
            has_sql = bool(node_data.get("generation", {}).get("sql") or node_data.get("sql"))
            
            # Check execution (stored in generation field primarily, evaluation field as fallback)
            has_execution = bool(
                node_data.get("generation", {}).get("execution_result") or 
                node_data.get("evaluation", {}).get("execution_result")
            )
            
            # Check evaluation quality from node's evaluation field
            quality = "none"
            evaluation = node_data.get("evaluation", {})
            if evaluation:
                quality = evaluation.get("result_quality", "").lower()
            
            # Check attempt count from node data directly
            attempt_count = node_data.get("generation_attempts", 0)
            max_attempts_reached = attempt_count >= 3
            
            # Determine initial node status
            if quality in ["excellent", "good"]:
                status = "complete"
            elif max_attempts_reached:
                # Force completion after 3 attempts regardless of quality
                status = "complete"
                self.logger.info(f"Node {node_id} marked complete after {attempt_count} attempts (max reached)")
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
                "parent": node_data.get("parentId"),
                "attempt_count": attempt_count,
                "max_attempts_reached": max_attempts_reached
            }
        
        # Second pass: update parent node status based on children completion
        for node_id, node_status in node_statuses.items():
            children = node_status["children"]
            if children and len(children) > 0:
                # This is a parent node with children
                # Check if all children have finished processing (either complete or max attempts reached)
                all_children_finished = True
                
                for child_id in children:
                    if child_id in node_statuses:
                        child_status = node_statuses[child_id]["status"]
                        child_attempts = node_statuses[child_id]["attempt_count"]
                        child_max_reached = node_statuses[child_id]["max_attempts_reached"]
                        
                        # Child is finished if it's complete OR has reached max attempts
                        if child_status != "complete" and not child_max_reached:
                            all_children_finished = False
                            break
                    else:
                        all_children_finished = False
                        break
                
                # Move to parent when all children are finished (regardless of SQL quality)
                if all_children_finished:
                    if node_status["status"] in ["needs_sql", "needs_eval", "bad_sql"]:
                        # Mark parent as needing SQL generation (will be handled by SQLGeneratorAgent)
                        # Do NOT mark as complete yet - let SQLGeneratorAgent handle SQL combination
                        node_statuses[node_id]["status"] = "needs_sql"
                        self.logger.info(f"Parent node {node_id} ready for SQL generation (all children finished)")
        
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
        """Generate comprehensive tree status report including current node content."""
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
        
        # Add detailed current node content
        current_status = node_statuses.get(current_node_id, {})
        current_node_data = tree["nodes"].get(current_node_id, {})
        
        if current_status and current_node_data:
            # Show full intent without truncation
            intent = current_status["intent"]
            report_lines.extend([
                f"CURRENT_STATUS: {current_status['status']}",
                f"CURRENT_INTENT: {intent}"
            ])
            
            # Add current node content details
            report_lines.append("CURRENT_NODE_CONTENT:")
            
            # Show attempt count for debugging
            attempt_count = current_status.get("attempt_count", 0)
            max_attempts_reached = current_status.get("max_attempts_reached", False)
            report_lines.append(f"  - Attempts: {attempt_count}/3 {'(MAX REACHED)' if max_attempts_reached else ''}")
            
            # Schema linking status with full details
            has_schema = current_status["has_schema_linking"]
            schema_info = "none"
            if has_schema:
                schema_linking = current_node_data.get("schema_linking", {})
                # Show full schema linking information
                selected_tables = schema_linking.get("selected_tables", {})
                if isinstance(selected_tables, dict) and "table" in selected_tables:
                    tables = selected_tables["table"]
                    if isinstance(tables, list):
                        table_names = [t.get("name", "unknown") for t in tables if isinstance(t, dict)]
                        schema_info = f"tables: {', '.join(table_names)}"
                        # Add column information if available
                        for table in tables:
                            if isinstance(table, dict) and "columns" in table:
                                columns = table.get("columns", {})
                                if isinstance(columns, dict) and "column" in columns:
                                    col_list = columns["column"]
                                    if isinstance(col_list, list):
                                        col_names = [c.get("name", "unknown") for c in col_list if isinstance(c, dict)]
                                        schema_info += f"; {table.get('name', 'table')} columns: {', '.join(col_names)}"
                    elif isinstance(tables, dict):
                        schema_info = f"table: {tables.get('name', 'unknown')}"
            report_lines.append(f"  - Schema linked: {has_schema} ({schema_info})")
            
            # SQL generation status with full SQL
            has_sql = current_status["has_sql"]
            sql_info = "none"
            if has_sql:
                generation = current_node_data.get("generation", {})
                sql = generation.get("sql") or current_node_data.get("sql")
                if sql:
                    sql_info = str(sql).strip().replace('\n', ' ')  # Show full SQL without truncation
            report_lines.append(f"  - SQL generated: {has_sql}")
            if has_sql and sql_info != "none":
                report_lines.append(f"    SQL: {sql_info}")
            
            # Evaluation status with full details
            has_execution = current_status["has_execution"]
            quality = current_status["quality"]
            exec_info = "none"
            if has_execution:
                # Check generation field first, then evaluation field
                exec_result = None
                if current_node_data.get("generation", {}).get("execution_result"):
                    exec_result = current_node_data["generation"]["execution_result"]
                elif current_node_data.get("evaluation", {}).get("execution_result"):
                    exec_result = current_node_data["evaluation"]["execution_result"]
                
                if exec_result:
                    row_count = exec_result.get("row_count", 0)
                    success = exec_result.get("status") == "success"
                    exec_info = f"{row_count} rows, {'success' if success else 'error'}"
                    # Add error details if present
                    if not success and exec_result.get("error"):
                        exec_info += f" - {exec_result.get('error')}"
            report_lines.append(f"  - Execution: {has_execution} ({exec_info}), Quality: {quality}")
            
            # Show full errors and suggestions if in bad_sql state
            if current_status["status"] == "bad_sql":
                evaluation = current_node_data.get("evaluation", {})
                issues = evaluation.get("issues", {})
                suggestions = evaluation.get("suggestions", {})
                if issues or suggestions:
                    report_lines.append("  - Issues detected:")
                    if isinstance(issues, dict) and "issue" in issues:
                        issue_list = issues["issue"] if isinstance(issues["issue"], list) else [issues["issue"]]
                        for issue in issue_list:  # Show all issues without truncation
                            if isinstance(issue, dict):
                                desc = issue.get("description", "")
                                severity = issue.get("severity", "unknown")
                                report_lines.append(f"    * [{severity}] {desc}")
                    
                    # Show suggestions without truncation
                    if isinstance(suggestions, dict) and "suggestion" in suggestions:
                        suggestion_list = suggestions["suggestion"] if isinstance(suggestions["suggestion"], list) else [suggestions["suggestion"]]
                        if suggestion_list:
                            report_lines.append("  - Suggestions:")
                            for suggestion in suggestion_list:
                                if isinstance(suggestion, str):
                                    report_lines.append(f"    * {suggestion}")
                                elif isinstance(suggestion, dict) and "text" in suggestion:
                                    report_lines.append(f"    * {suggestion['text']}")
        
        # Overall completion status
        if complete_nodes == total_nodes:
            report_lines.append("OVERALL_STATUS: All nodes complete")
        else:
            report_lines.append("OVERALL_STATUS: Processing in progress")
        
        return "\n".join(report_lines)
    
    
    
    def get_tool(self):
        """Return self as the tool instance for use with agents"""
        return self