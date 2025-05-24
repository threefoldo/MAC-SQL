"""
Node history manager for text-to-SQL workflow.

This module provides easy access to node operation history stored in KeyValueMemory.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from memory_content_types import NodeOperation, NodeOperationType, QueryMapping


class NodeHistoryManager:
    """Manages node operation history in memory."""
    
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
    
    async def add_operation(self, operation: NodeOperation) -> None:
        """
        Add a new operation to the history.
        
        Args:
            operation: The operation to add
        """
        history = await self.memory.get("nodeHistory")
        if history is None:
            history = []
        
        history.append(operation.to_dict())
        await self.memory.set("nodeHistory", history)
        self.logger.info(f"Added {operation.operation.value} operation for node {operation.nodeId}")
    
    async def record_create(self, node_id: str, intent: str, 
                          mapping: Optional[QueryMapping] = None,
                          combine_strategy: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a node creation operation.
        
        Args:
            node_id: The node ID
            intent: The intent for the node
            mapping: Optional mapping information
            combine_strategy: Optional combine strategy
        """
        data = {"intent": intent}
        if mapping:
            data["mapping"] = mapping.to_dict()
        if combine_strategy:
            data["combineStrategy"] = combine_strategy
        
        operation = NodeOperation(
            timestamp=datetime.now().isoformat(),
            nodeId=node_id,
            operation=NodeOperationType.CREATE,
            data=data
        )
        
        await self.add_operation(operation)
    
    async def record_generate_sql(self, node_id: str, sql: str) -> None:
        """
        Record SQL generation for a node.
        
        Args:
            node_id: The node ID
            sql: The generated SQL
        """
        operation = NodeOperation(
            timestamp=datetime.now().isoformat(),
            nodeId=node_id,
            operation=NodeOperationType.GENERATE_SQL,
            data={"sql": sql}
        )
        
        await self.add_operation(operation)
    
    async def record_execute(self, node_id: str, sql: str, 
                           result: Optional[Any] = None, 
                           error: Optional[str] = None) -> None:
        """
        Record SQL execution for a node.
        
        Args:
            node_id: The node ID
            sql: The executed SQL
            result: Optional execution result
            error: Optional error message
        """
        data = {"sql": sql}
        if result is not None:
            data["result"] = result
        if error:
            data["error"] = error
        
        operation = NodeOperation(
            timestamp=datetime.now().isoformat(),
            nodeId=node_id,
            operation=NodeOperationType.EXECUTE,
            data=data
        )
        
        await self.add_operation(operation)
    
    async def record_revise(self, node_id: str, 
                          new_intent: Optional[str] = None,
                          new_sql: Optional[str] = None,
                          new_mapping: Optional[QueryMapping] = None,
                          previous_intent: Optional[str] = None,
                          previous_sql: Optional[str] = None,
                          previous_mapping: Optional[QueryMapping] = None) -> None:
        """
        Record a node revision operation.
        
        Args:
            node_id: The node ID
            new_intent: New intent if changed
            new_sql: New SQL if changed
            new_mapping: New mapping if changed
            previous_intent: Previous intent
            previous_sql: Previous SQL
            previous_mapping: Previous mapping
        """
        data = {}
        
        if new_intent:
            data["intent"] = new_intent
        if new_sql:
            data["sql"] = new_sql
        if new_mapping:
            data["mapping"] = new_mapping.to_dict()
        
        if previous_intent:
            data["previousIntent"] = previous_intent
        if previous_sql:
            data["previousSql"] = previous_sql
        if previous_mapping:
            data["previousMapping"] = previous_mapping.to_dict()
        
        operation = NodeOperation(
            timestamp=datetime.now().isoformat(),
            nodeId=node_id,
            operation=NodeOperationType.REVISE,
            data=data
        )
        
        await self.add_operation(operation)
    
    async def record_delete(self, node_id: str, reason: Optional[str] = None) -> None:
        """
        Record node deletion.
        
        Args:
            node_id: The node ID
            reason: Optional reason for deletion
        """
        data = {}
        if reason:
            data["reason"] = reason
        
        operation = NodeOperation(
            timestamp=datetime.now().isoformat(),
            nodeId=node_id,
            operation=NodeOperationType.DELETE,
            data=data
        )
        
        await self.add_operation(operation)
    
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
    
    async def get_latest_operation(self, node_id: str, 
                                 operation_type: Optional[NodeOperationType] = None) -> Optional[NodeOperation]:
        """
        Get the latest operation for a node.
        
        Args:
            node_id: The node ID
            operation_type: Optional operation type filter
            
        Returns:
            The latest operation or None
        """
        operations = await self.get_node_operations(node_id)
        
        if operation_type:
            operations = [op for op in operations if op.operation == operation_type]
        
        return operations[-1] if operations else None
    
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
    
    async def get_revision_history(self, node_id: str) -> List[Dict[str, Any]]:
        """
        Get the revision history for a node.
        
        Args:
            node_id: The node ID
            
        Returns:
            List of revision details
        """
        operations = await self.get_node_operations(node_id)
        revisions = []
        
        for op in operations:
            if op.operation == NodeOperationType.REVISE:
                revision = {
                    "timestamp": op.timestamp,
                    "changes": []
                }
                
                if "intent" in op.data and "previousIntent" in op.data:
                    revision["changes"].append({
                        "field": "intent",
                        "from": op.data["previousIntent"],
                        "to": op.data["intent"]
                    })
                
                if "sql" in op.data and "previousSql" in op.data:
                    revision["changes"].append({
                        "field": "sql",
                        "from": op.data["previousSql"],
                        "to": op.data["sql"]
                    })
                
                if "mapping" in op.data and "previousMapping" in op.data:
                    revision["changes"].append({
                        "field": "mapping",
                        "from": op.data["previousMapping"],
                        "to": op.data["mapping"]
                    })
                
                revisions.append(revision)
        
        return revisions
    
    async def get_operations_in_timerange(self, start_time: str, end_time: str) -> List[NodeOperation]:
        """
        Get operations within a time range.
        
        Args:
            start_time: ISO format start time
            end_time: ISO format end time
            
        Returns:
            List of operations within the time range
        """
        all_operations = await self.get_all_operations()
        return [op for op in all_operations 
                if start_time <= op.timestamp <= end_time]
    
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
        
        for op in all_operations:
            operation_counts[op.operation.value] += 1
            unique_nodes.add(op.nodeId)
            
            if op.operation == NodeOperationType.EXECUTE and op.data.get("error"):
                failed_executions += 1
        
        return {
            "total_operations": len(all_operations),
            "unique_nodes": len(unique_nodes),
            "operation_counts": operation_counts,
            "failed_executions": failed_executions,
            "deleted_nodes": operation_counts[NodeOperationType.DELETE.value]
        }