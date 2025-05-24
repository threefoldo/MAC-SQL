"""
Query tree manager for text-to-SQL workflow.

This module provides easy access to query tree data stored in KeyValueMemory.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from memory_content_types import QueryNode, QueryMapping, CombineStrategy, ExecutionResult, NodeStatus


class QueryTreeManager:
    """Manages query tree data in memory."""
    
    def __init__(self, memory: KeyValueMemory):
        """
        Initialize the query tree manager.
        
        Args:
            memory: The KeyValueMemory instance to use for storage
        """
        self.memory = memory
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def initialize(self, root_intent: str) -> str:
        """
        Initialize a query tree with a root node.
        
        Args:
            root_intent: The intent for the root node (usually the original query)
            
        Returns:
            The root node ID
        """
        root_id = f"node_{datetime.now().timestamp()}_root"
        root_node = QueryNode(
            nodeId=root_id,
            intent=root_intent,
            mapping=QueryMapping()
        )
        
        query_tree = {
            "rootId": root_id,
            "currentNodeId": root_id,
            "nodes": {root_id: root_node.to_dict()}
        }
        
        await self.memory.set("queryTree", query_tree)
        self.logger.info(f"Initialized query tree with root node {root_id}")
        
        return root_id
    
    async def get_tree(self) -> Optional[Dict[str, Any]]:
        """Get the complete query tree."""
        return await self.memory.get("queryTree")
    
    async def get_root_id(self) -> Optional[str]:
        """Get the root node ID."""
        tree = await self.get_tree()
        return tree.get("rootId") if tree else None
    
    async def get_current_node_id(self) -> Optional[str]:
        """Get the current node ID."""
        tree = await self.get_tree()
        return tree.get("currentNodeId") if tree else None
    
    async def set_current_node_id(self, node_id: str) -> None:
        """Set the current node ID."""
        tree = await self.get_tree()
        if tree:
            tree["currentNodeId"] = node_id
            await self.memory.set("queryTree", tree)
            self.logger.info(f"Set current node to {node_id}")
    
    async def add_node(self, node: QueryNode, parent_id: Optional[str] = None) -> None:
        """
        Add a new node to the tree.
        
        Args:
            node: The node to add
            parent_id: Optional parent node ID
        """
        tree = await self.get_tree()
        if not tree:
            raise ValueError("Query tree not initialized")
        
        # Add the node
        tree["nodes"][node.nodeId] = node.to_dict()
        
        # Update parent-child relationships
        if parent_id:
            if parent_id in tree["nodes"]:
                parent_node_data = tree["nodes"][parent_id]
                if "childIds" not in parent_node_data:
                    parent_node_data["childIds"] = []
                parent_node_data["childIds"].append(node.nodeId)
                node.parentId = parent_id
                tree["nodes"][node.nodeId]["parentId"] = parent_id
            else:
                raise ValueError(f"Parent node {parent_id} not found")
        
        await self.memory.set("queryTree", tree)
        self.logger.info(f"Added node {node.nodeId} to tree")
    
    async def get_node(self, node_id: str) -> Optional[QueryNode]:
        """
        Get a specific node.
        
        Args:
            node_id: The node ID
            
        Returns:
            QueryNode if found, None otherwise
        """
        tree = await self.get_tree()
        if tree and "nodes" in tree and node_id in tree["nodes"]:
            return QueryNode.from_dict(tree["nodes"][node_id])
        return None
    
    async def update_node(self, node_id: str, updates: Dict[str, Any]) -> None:
        """
        Update a node with new data.
        
        Args:
            node_id: The node ID to update
            updates: Dictionary of updates to apply
        """
        tree = await self.get_tree()
        if not tree or "nodes" not in tree or node_id not in tree["nodes"]:
            raise ValueError(f"Node {node_id} not found")
        
        tree["nodes"][node_id].update(updates)
        await self.memory.set("queryTree", tree)
        self.logger.info(f"Updated node {node_id}")
    
    async def update_node_sql(self, node_id: str, sql: str) -> None:
        """Update the SQL for a node."""
        await self.update_node(node_id, {"sql": sql, "status": NodeStatus.SQL_GENERATED.value})
    
    async def update_node_result(self, node_id: str, result: ExecutionResult, success: bool) -> None:
        """Update the execution result for a node."""
        status = NodeStatus.EXECUTED_SUCCESS if success else NodeStatus.EXECUTED_FAILED
        await self.update_node(node_id, {
            "executionResult": result.to_dict(),
            "status": status.value
        })
    
    async def update_node_mapping(self, node_id: str, mapping: QueryMapping) -> None:
        """Update the mapping for a node."""
        await self.update_node(node_id, {"mapping": mapping.to_dict()})
    
    async def update_node_combine_strategy(self, node_id: str, strategy: CombineStrategy) -> None:
        """Update the combine strategy for a node."""
        await self.update_node(node_id, {"combineStrategy": strategy.to_dict()})
    
    async def delete_node(self, node_id: str) -> None:
        """
        Delete a node and all its descendants.
        
        Args:
            node_id: The node ID to delete
        """
        tree = await self.get_tree()
        if not tree or "nodes" not in tree:
            return
        
        # Get all nodes to delete (node and descendants)
        nodes_to_delete = await self._get_subtree_nodes(node_id)
        
        # Remove from parent's child list
        node = await self.get_node(node_id)
        if node and node.parentId and node.parentId in tree["nodes"]:
            parent_data = tree["nodes"][node.parentId]
            if "childIds" in parent_data and node_id in parent_data["childIds"]:
                parent_data["childIds"].remove(node_id)
        
        # Delete all nodes
        for nid in nodes_to_delete:
            if nid in tree["nodes"]:
                del tree["nodes"][nid]
        
        await self.memory.set("queryTree", tree)
        self.logger.info(f"Deleted node {node_id} and {len(nodes_to_delete)-1} descendants")
    
    async def get_children(self, node_id: str) -> List[QueryNode]:
        """Get all child nodes of a node."""
        node = await self.get_node(node_id)
        if not node:
            return []
        
        children = []
        for child_id in node.childIds:
            child = await self.get_node(child_id)
            if child:
                children.append(child)
        
        return children
    
    async def get_parent(self, node_id: str) -> Optional[QueryNode]:
        """Get the parent node."""
        node = await self.get_node(node_id)
        if node and node.parentId:
            return await self.get_node(node.parentId)
        return None
    
    async def get_siblings(self, node_id: str) -> List[QueryNode]:
        """Get all sibling nodes."""
        parent = await self.get_parent(node_id)
        if not parent:
            return []
        
        siblings = []
        for child_id in parent.childIds:
            if child_id != node_id:
                child = await self.get_node(child_id)
                if child:
                    siblings.append(child)
        
        return siblings
    
    async def get_ancestors(self, node_id: str) -> List[QueryNode]:
        """Get all ancestor nodes from parent to root."""
        ancestors = []
        current_id = node_id
        
        while True:
            parent = await self.get_parent(current_id)
            if not parent:
                break
            ancestors.append(parent)
            current_id = parent.nodeId
        
        return ancestors
    
    async def get_path_to_root(self, node_id: str) -> List[str]:
        """Get the path of node IDs from this node to root."""
        path = [node_id]
        current_id = node_id
        
        while True:
            parent = await self.get_parent(current_id)
            if not parent:
                break
            path.append(parent.nodeId)
            current_id = parent.nodeId
        
        path.reverse()
        return path
    
    async def _get_subtree_nodes(self, node_id: str) -> List[str]:
        """Get all nodes in a subtree (including the root)."""
        nodes = [node_id]
        to_process = [node_id]
        
        while to_process:
            current_id = to_process.pop(0)
            node = await self.get_node(current_id)
            if node:
                for child_id in node.childIds:
                    nodes.append(child_id)
                    to_process.append(child_id)
        
        return nodes
    
    async def get_executable_nodes(self) -> List[QueryNode]:
        """Get all nodes that have SQL but haven't been executed."""
        tree = await self.get_tree()
        if not tree or "nodes" not in tree:
            return []
        
        executable = []
        for node_data in tree["nodes"].values():
            if node_data.get("sql") and node_data.get("status") == NodeStatus.SQL_GENERATED.value:
                node = QueryNode.from_dict(node_data)
                executable.append(node)
        
        return executable
    
    async def get_failed_nodes(self) -> List[QueryNode]:
        """Get all nodes that failed execution."""
        tree = await self.get_tree()
        if not tree or "nodes" not in tree:
            return []
        
        failed = []
        for node_data in tree["nodes"].values():
            if node_data.get("status") == NodeStatus.EXECUTED_FAILED.value:
                node = QueryNode.from_dict(node_data)
                failed.append(node)
        
        return failed
    
    async def get_successful_nodes(self) -> List[QueryNode]:
        """Get all nodes that executed successfully."""
        tree = await self.get_tree()
        if not tree or "nodes" not in tree:
            return []
        
        successful = []
        for node_data in tree["nodes"].values():
            if node_data.get("status") == NodeStatus.EXECUTED_SUCCESS.value:
                node = QueryNode.from_dict(node_data)
                successful.append(node)
        
        return successful
    
    async def get_leaf_nodes(self) -> List[QueryNode]:
        """Get all leaf nodes (nodes with no children)."""
        tree = await self.get_tree()
        if not tree or "nodes" not in tree:
            return []
        
        leaves = []
        for node_data in tree["nodes"].values():
            if not node_data.get("childIds", []):
                node = QueryNode.from_dict(node_data)
                leaves.append(node)
        
        return leaves
    
    async def find_nodes_by_intent(self, intent_pattern: str) -> List[QueryNode]:
        """Find nodes whose intent contains the given pattern."""
        tree = await self.get_tree()
        if not tree or "nodes" not in tree:
            return []
        
        matching = []
        pattern_lower = intent_pattern.lower()
        
        for node_data in tree["nodes"].values():
            if pattern_lower in node_data.get("intent", "").lower():
                node = QueryNode.from_dict(node_data)
                matching.append(node)
        
        return matching
    
    async def get_tree_stats(self) -> Dict[str, Any]:
        """Get statistics about the query tree."""
        tree = await self.get_tree()
        if not tree or "nodes" not in tree:
            return {
                "total_nodes": 0,
                "depth": 0,
                "leaf_nodes": 0,
                "executed_nodes": 0,
                "failed_nodes": 0
            }
        
        nodes = tree["nodes"]
        leaf_count = sum(1 for n in nodes.values() if not n.get("childIds", []))
        executed_count = sum(1 for n in nodes.values() if n.get("status") in [
            NodeStatus.EXECUTED_SUCCESS.value, 
            NodeStatus.EXECUTED_FAILED.value
        ])
        failed_count = sum(1 for n in nodes.values() if n.get("status") == NodeStatus.EXECUTED_FAILED.value)
        
        # Calculate depth
        max_depth = 0
        root_id = tree.get("rootId")
        if root_id:
            max_depth = await self._calculate_tree_depth(root_id)
        
        return {
            "total_nodes": len(nodes),
            "depth": max_depth,
            "leaf_nodes": leaf_count,
            "executed_nodes": executed_count,
            "failed_nodes": failed_count
        }
    
    async def _calculate_tree_depth(self, node_id: str, current_depth: int = 0) -> int:
        """Calculate the maximum depth of the tree from a given node."""
        node = await self.get_node(node_id)
        if not node or not node.childIds:
            return current_depth
        
        max_child_depth = current_depth
        for child_id in node.childIds:
            child_depth = await self._calculate_tree_depth(child_id, current_depth + 1)
            max_child_depth = max(max_child_depth, child_depth)
        
        return max_child_depth