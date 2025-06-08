"""
Node history manager for text-to-SQL tree orchestration.

This module provides node operation history stored in KeyValueMemory.
All nodes share QueryNode structure for consistency and essential information storage.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from memory_content_types import NodeOperation, NodeOperationType, QueryNode, NodeStatus


class NodeHistoryManager:
    """Manages node operation history with QueryNode structure consistency."""
    
    def __init__(self, memory: KeyValueMemory):
        """
        Initialize the node history manager.
        
        Args:
            memory: The KeyValueMemory instance to use for storage
        """
        self.memory = memory
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def initialize(self) -> None:
        """Initialize an empty node history."""
        await self.memory.set("nodeHistory", [])
        self.logger.info("Initialized empty node history")
    
    def _extract_essential_node_info(self, node: QueryNode) -> Dict[str, Any]:
        """
        Extract essential information from a QueryNode, removing verbose content.
        
        Args:
            node: The QueryNode to extract from
            
        Returns:
            Dict with essential node information
        """
        essential = {
            "nodeId": node.nodeId,
            "status": node.status.value,
            "intent": node.intent,
            "parentId": node.parentId,
            "childIds": node.childIds[:],  # Copy list
        }
        
        # Include evidence if present
        if node.evidence:
            essential["evidence"] = node.evidence
        
        # Schema linking - only essential parts
        if node.schema_linking:
            schema_essential = {}
            if "selected_tables" in node.schema_linking:
                schema_essential["selected_tables"] = node.schema_linking["selected_tables"]
            if "column_mapping" in node.schema_linking:
                schema_essential["column_mapping"] = node.schema_linking["column_mapping"]
            if "foreign_keys" in node.schema_linking:
                schema_essential["foreign_keys"] = node.schema_linking["foreign_keys"]
            if schema_essential:
                essential["schema_linking"] = schema_essential
        
        # Generation - only SQL and core info, no explanations
        if node.generation:
            gen_essential = {}
            if "sql" in node.generation:
                gen_essential["sql"] = node.generation["sql"]
            if "sql_type" in node.generation:
                gen_essential["sql_type"] = node.generation["sql_type"]
            if "confidence" in node.generation:
                gen_essential["confidence"] = node.generation["confidence"]
            if gen_essential:
                essential["generation"] = gen_essential
        
        # Check for execution result in generation field first (SQL Generator now executes)
        execution_result = None
        if node.generation and "execution_result" in node.generation:
            execution_result = node.generation["execution_result"]
        elif node.evaluation and "execution_result" in node.evaluation:
            execution_result = node.evaluation["execution_result"]
        
        # Evaluation - results and core metrics, no detailed explanations
        if node.evaluation or execution_result:
            eval_essential = {}
            if execution_result:
                if isinstance(execution_result, dict):
                    eval_essential["execution_result"] = {
                        "data": execution_result.get("data", [])[:5],  # Limit to 5 rows
                        "rowCount": execution_result.get("rowCount", 0),
                        "error": execution_result.get("error")
                    }
                else:
                    eval_essential["execution_result"] = execution_result
            
            if "success" in node.evaluation:
                eval_essential["success"] = node.evaluation["success"]
            if "error_type" in node.evaluation:
                eval_essential["error_type"] = node.evaluation["error_type"]
            if "quality_score" in node.evaluation:
                eval_essential["quality_score"] = node.evaluation["quality_score"]
            if eval_essential:
                essential["evaluation"] = eval_essential
        
        # Decomposition - only structure, no explanations
        if node.decomposition:
            decomp_essential = {}
            if "subqueries" in node.decomposition:
                decomp_essential["subqueries"] = node.decomposition["subqueries"]
            if "join_strategy" in node.decomposition:
                decomp_essential["join_strategy"] = node.decomposition["join_strategy"]
            if decomp_essential:
                essential["decomposition"] = decomp_essential
        
        return essential
    
    async def record_node_operation(self, node: QueryNode, operation_type: NodeOperationType, 
                                  additional_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Record any node operation with QueryNode structure.
        
        Args:
            node: The QueryNode to record
            operation_type: The type of operation being performed
            additional_data: Optional additional data to include
        """
        essential_info = self._extract_essential_node_info(node)
        
        # Add any additional data
        if additional_data:
            essential_info.update(additional_data)
        
        operation = NodeOperation(
            timestamp=datetime.now().isoformat(),
            nodeId=node.nodeId,
            operation=operation_type,
            data=essential_info
        )
        
        history = await self.memory.get("nodeHistory")
        if history is None:
            history = []
        
        history.append(operation.to_dict())
        await self.memory.set("nodeHistory", history)
        self.logger.info(f"Recorded {operation_type.value} operation for node {node.nodeId}")
    
    async def record_create(self, node: QueryNode) -> None:
        """
        Record node creation operation.
        
        Args:
            node: The QueryNode that was created
        """
        await self.record_node_operation(node, NodeOperationType.CREATE)
    
    async def record_generate_sql(self, node: QueryNode) -> None:
        """
        Record SQL generation operation.
        
        Args:
            node: The QueryNode with generated SQL
        """
        await self.record_node_operation(node, NodeOperationType.GENERATE_SQL)
    
    async def record_execute(self, node: QueryNode, error: Optional[str] = None) -> None:
        """
        Record SQL execution operation.
        
        Args:
            node: The QueryNode with execution results
            error: Optional execution error
        """
        additional_data = {"error": error} if error else None
        await self.record_node_operation(node, NodeOperationType.EXECUTE, additional_data)
    
    async def record_revise(self, node: QueryNode, reason: Optional[str] = None) -> None:
        """
        Record node revision operation.
        
        Args:
            node: The revised QueryNode
            reason: Optional reason for revision
        """
        additional_data = {"reason": reason} if reason else None
        await self.record_node_operation(node, NodeOperationType.REVISE, additional_data)
    
    async def record_delete(self, node: QueryNode, reason: Optional[str] = None) -> None:
        """
        Record node deletion operation.
        
        Args:
            node: The QueryNode being deleted
            reason: Optional reason for deletion
        """
        additional_data = {"reason": reason} if reason else None
        await self.record_node_operation(node, NodeOperationType.DELETE, additional_data)
    
    async def get_all_operations(self) -> List[NodeOperation]:
        """
        Get all operations in the history.
        
        Returns:
            List of all operations
        """
        history = await self.memory.get("nodeHistory")
        if not history:
            return []
        
        return [NodeOperation.from_dict(op) for op in history]
    
    async def get_node_operations(self, node_id: str) -> List[NodeOperation]:
        """
        Get all operations for a specific node.
        
        Args:
            node_id: The node ID
            
        Returns:
            List of operations for the node
        """
        all_operations = await self.get_all_operations()
        return [op for op in all_operations if op.nodeId == node_id]
    
    async def get_operations_by_type(self, operation_type: NodeOperationType) -> List[NodeOperation]:
        """
        Get all operations of a specific type.
        
        Args:
            operation_type: The operation type to filter by
            
        Returns:
            List of operations of the specified type
        """
        all_operations = await self.get_all_operations()
        return [op for op in all_operations if op.operation == operation_type]
    
    async def get_current_node_state(self, node_id: str) -> Optional[QueryNode]:
        """
        Reconstruct the current QueryNode state from operation history.
        
        Args:
            node_id: The node ID
            
        Returns:
            Current QueryNode state or None if not found
        """
        operations = await self.get_node_operations(node_id)
        if not operations:
            return None
        
        # Start with the latest CREATE or REVISE operation that has full node data
        base_operation = None
        for op in reversed(operations):
            if (op.operation in [NodeOperationType.CREATE, NodeOperationType.REVISE] and 
                "nodeId" in op.data and "status" in op.data and "intent" in op.data):
                base_operation = op
                break
        
        if not base_operation:
            return None
        
        # Build QueryNode from base operation data
        data = base_operation.data.copy()
        
        # Apply subsequent operations in chronological order
        base_time = base_operation.timestamp
        for op in operations:
            if op.timestamp > base_time:
                if op.operation == NodeOperationType.GENERATE_SQL:
                    if "generation" not in data:
                        data["generation"] = {}
                    # Get SQL from the operation data (it's stored in the generation field)
                    if "generation" in op.data and "sql" in op.data["generation"]:
                        data["generation"]["sql"] = op.data["generation"]["sql"]
                    elif "sql" in op.data:
                        data["generation"]["sql"] = op.data["sql"]
                    
                    if "generation" in op.data:
                        if "sql_type" in op.data["generation"]:
                            data["generation"]["sql_type"] = op.data["generation"]["sql_type"]
                        if "confidence" in op.data["generation"]:
                            data["generation"]["confidence"] = op.data["generation"]["confidence"]
                    elif "sql_type" in op.data:
                        data["generation"]["sql_type"] = op.data["sql_type"]
                    elif "confidence" in op.data:
                        data["generation"]["confidence"] = op.data["confidence"]
                    
                    data["status"] = NodeStatus.SQL_GENERATED.value
                
                elif op.operation == NodeOperationType.EXECUTE:
                    # Check for execution result in generation field first (SQL Generator now executes)
                    exec_result = None
                    if "generation" in op.data and "execution_result" in op.data["generation"]:
                        exec_result = op.data["generation"]["execution_result"]
                        if "generation" not in data:
                            data["generation"] = {}
                        data["generation"]["execution_result"] = exec_result
                    elif "evaluation" in op.data and "execution_result" in op.data["evaluation"]:
                        exec_result = op.data["evaluation"]["execution_result"]
                        if "evaluation" not in data:
                            data["evaluation"] = {}
                        data["evaluation"]["execution_result"] = exec_result
                    elif "result" in op.data:
                        exec_result = op.data["result"]
                        # Store in generation field by default for new executions
                        if "generation" not in data:
                            data["generation"] = {}
                        data["generation"]["execution_result"] = exec_result
                    
                    if op.data.get("error"):
                        data["status"] = NodeStatus.EXECUTED_FAILED.value
                        data["evaluation"]["error"] = op.data["error"]
                    else:
                        data["status"] = NodeStatus.EXECUTED_SUCCESS.value
                        data["evaluation"]["success"] = True
        
        # Convert back to QueryNode
        try:
            return QueryNode.from_dict(data)
        except Exception as e:
            self.logger.error(f"Failed to reconstruct QueryNode from history: {e}")
            return None
    
    async def get_node_attempts_summary(self, node_id: str) -> Dict[str, Any]:
        """
        Get a complete summary of all attempts for a specific node.
        
        Args:
            node_id: The node ID
            
        Returns:
            Dictionary with complete node attempt history
        """
        operations = await self.get_node_operations(node_id)
        
        attempts = []
        current_attempt = {}
        
        for op in operations:
            if op.operation == NodeOperationType.CREATE:
                current_attempt = {
                    "attempt_number": 1,
                    "created": op.timestamp,
                    "intent": op.data.get("intent", ""),
                    "sql_generated": None,
                    "execution_result": None,
                    "final_status": "created"
                }
            
            elif op.operation == NodeOperationType.GENERATE_SQL:
                if current_attempt:
                    # Extract SQL from appropriate location
                    sql = ""
                    confidence = None
                    if "generation" in op.data:
                        sql = op.data["generation"].get("sql", "")
                        confidence = op.data["generation"].get("confidence")
                    else:
                        sql = op.data.get("sql", "")
                        confidence = op.data.get("confidence")
                    
                    current_attempt["sql_generated"] = {
                        "timestamp": op.timestamp,
                        "sql": sql,
                        "confidence": confidence
                    }
                    current_attempt["final_status"] = "sql_generated"
            
            elif op.operation == NodeOperationType.EXECUTE:
                if current_attempt:
                    # Extract result from appropriate location (check generation first)
                    result = None
                    error = op.data.get("error")
                    
                    if "generation" in op.data and "execution_result" in op.data["generation"]:
                        result = op.data["generation"]["execution_result"]
                    elif "evaluation" in op.data and "execution_result" in op.data["evaluation"]:
                        result = op.data["evaluation"]["execution_result"]
                    elif "result" in op.data:
                        result = op.data["result"]
                    elif "execution_result" in op.data:
                        result = op.data["execution_result"]
                    
                    current_attempt["execution_result"] = {
                        "timestamp": op.timestamp,
                        "result": result,
                        "error": error,
                        "success": not bool(error)
                    }
                    current_attempt["final_status"] = "executed_success" if not error else "executed_failed"
            
            elif op.operation == NodeOperationType.REVISE:
                # Save current attempt and start new one
                if current_attempt:
                    attempts.append(current_attempt.copy())
                
                attempt_num = len(attempts) + 2  # +1 for current, +1 for next
                current_attempt = {
                    "attempt_number": attempt_num,
                    "revised": op.timestamp,
                    "intent": op.data.get("intent", current_attempt.get("intent", "")),
                    "sql_generated": None,
                    "execution_result": None,
                    "final_status": "revised",
                    "revision_reason": op.data.get("reason", "")
                }
            
            elif op.operation == NodeOperationType.DELETE:
                if current_attempt:
                    current_attempt["deleted"] = op.timestamp
                    current_attempt["final_status"] = "deleted"
        
        # Add the final attempt
        if current_attempt:
            attempts.append(current_attempt)
        
        return {
            "node_id": node_id,
            "total_attempts": len(attempts),
            "attempts": attempts,
            "final_status": attempts[-1]["final_status"] if attempts else "unknown"
        }
    
    async def get_node_sql_evolution(self, node_id: str) -> List[Dict[str, Any]]:
        """
        Get the evolution of SQL for a node through all attempts.
        
        Args:
            node_id: The node ID
            
        Returns:
            List of SQL generations with metadata
        """
        operations = await self.get_node_operations(node_id)
        
        sql_evolution = []
        attempt = 1
        
        for op in operations:
            if op.operation == NodeOperationType.GENERATE_SQL:
                # Extract SQL from the appropriate location
                sql = ""
                sql_type = None
                confidence = None
                
                if "generation" in op.data:
                    generation = op.data["generation"]
                    sql = generation.get("sql", "")
                    sql_type = generation.get("sql_type")
                    confidence = generation.get("confidence")
                else:
                    sql = op.data.get("sql", "")
                    sql_type = op.data.get("sql_type")
                    confidence = op.data.get("confidence")
                
                sql_entry = {
                    "attempt": attempt,
                    "timestamp": op.timestamp,
                    "sql": sql,
                    "sql_type": sql_type,
                    "confidence": confidence
                }
                sql_evolution.append(sql_entry)
                attempt += 1
        
        return sql_evolution
    
    async def get_node_execution_history(self, node_id: str) -> List[Dict[str, Any]]:
        """
        Get the complete execution history for a node.
        
        Args:
            node_id: The node ID
            
        Returns:
            List of execution attempts with results
        """
        operations = await self.get_node_operations(node_id)
        
        executions = []
        for op in operations:
            if op.operation == NodeOperationType.EXECUTE:
                # Extract result from the appropriate location (check generation first)
                result = None
                error = op.data.get("error")
                
                if "generation" in op.data and "execution_result" in op.data["generation"]:
                    result = op.data["generation"]["execution_result"]
                elif "evaluation" in op.data and "execution_result" in op.data["evaluation"]:
                    result = op.data["evaluation"]["execution_result"]
                elif "result" in op.data:
                    result = op.data["result"]
                elif "execution_result" in op.data:
                    result = op.data["execution_result"]
                
                execution = {
                    "timestamp": op.timestamp,
                    "result": result,
                    "error": error,
                    "success": not bool(error)
                }
                executions.append(execution)
        
        return executions
    
    async def get_node_lifecycle(self, node_id: str) -> Dict[str, Any]:
        """
        Get the complete lifecycle of a node.
        
        Args:
            node_id: The node ID
            
        Returns:
            Dictionary with lifecycle information
        """
        operations = await self.get_node_operations(node_id)
        
        lifecycle = {
            "nodeId": node_id,
            "created": None,
            "sql_generated": None,
            "executed": None,
            "revised_count": 0,
            "deleted": None,
            "total_operations": len(operations)
        }
        
        for op in operations:
            if op.operation == NodeOperationType.CREATE:
                lifecycle["created"] = op.timestamp
            elif op.operation == NodeOperationType.GENERATE_SQL:
                lifecycle["sql_generated"] = op.timestamp
            elif op.operation == NodeOperationType.EXECUTE:
                lifecycle["executed"] = op.timestamp
            elif op.operation == NodeOperationType.REVISE:
                lifecycle["revised_count"] += 1
            elif op.operation == NodeOperationType.DELETE:
                lifecycle["deleted"] = op.timestamp
        
        return lifecycle
    
    async def get_history_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the operation history.
        
        Returns:
            Dictionary with history statistics
        """
        all_operations = await self.get_all_operations()
        
        operation_counts = {}
        for op_type in NodeOperationType:
            operation_counts[op_type.value] = 0
        
        unique_nodes = set()
        failed_executions = 0
        successful_executions = 0
        total_sql_generated = 0
        
        for op in all_operations:
            operation_counts[op.operation.value] += 1
            unique_nodes.add(op.nodeId)
            
            if op.operation == NodeOperationType.EXECUTE:
                if op.data.get("error"):
                    failed_executions += 1
                else:
                    successful_executions += 1
            
            elif op.operation == NodeOperationType.GENERATE_SQL:
                total_sql_generated += 1
        
        return {
            "total_operations": len(all_operations),
            "unique_nodes": len(unique_nodes),
            "operation_counts": operation_counts,
            "execution_stats": {
                "total_executions": failed_executions + successful_executions,
                "successful_executions": successful_executions,
                "failed_executions": failed_executions,
                "success_rate": successful_executions / (failed_executions + successful_executions) if (failed_executions + successful_executions) > 0 else 0
            },
            "sql_generation_count": total_sql_generated,
            "deleted_nodes": operation_counts[NodeOperationType.DELETE.value]
        }
    
    async def get_deleted_nodes(self) -> List[str]:
        """
        Get IDs of all deleted nodes.
        
        Returns:
            List of deleted node IDs
        """
        delete_operations = await self.get_operations_by_type(NodeOperationType.DELETE)
        return [op.nodeId for op in delete_operations]
    
    async def get_failed_executions(self) -> List[NodeOperation]:
        """
        Get all failed execution operations.
        
        Returns:
            List of execution operations that had errors
        """
        executions = await self.get_operations_by_type(NodeOperationType.EXECUTE)
        return [op for op in executions if op.data.get("error")]
    
    async def get_successful_executions(self) -> List[NodeOperation]:
        """
        Get all successful execution operations.
        
        Returns:
            List of execution operations without errors
        """
        executions = await self.get_operations_by_type(NodeOperationType.EXECUTE)
        return [op for op in executions if not op.data.get("error")]