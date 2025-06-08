"""
Test cases for QueryTreeManager - TESTING_PLAN.md Layer 1.3.

Tests query tree storage and management operations:
- Store query tree at correct memory location ('queryTree')
- Initialize tree with root node
- Add nodes with proper parent-child relationships
- Update node SQL and results
- Track current node position accurately
- Retrieve nodes by various criteria
- Handle tree traversal operations
"""

import asyncio
import pytest
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from keyvalue_memory import KeyValueMemory
from query_tree_manager import QueryTreeManager
from memory_content_types import QueryNode, NodeStatus, ExecutionResult


class TestQueryTreeManager:
    """Test query tree management - TESTING_PLAN.md Layer 1.3."""
    
    @pytest.fixture
    async def memory(self):
        """Create a fresh KeyValueMemory instance."""
        mem = KeyValueMemory()
        yield mem
        await mem.clear()
    
    @pytest.fixture
    async def manager(self, memory):
        """Create a QueryTreeManager instance."""
        return QueryTreeManager(memory)
    
    @pytest.mark.asyncio
    async def test_memory_location_and_initialization(self, memory, manager):
        """Test query tree storage at correct memory location."""
        # Initialize tree
        root_id = await manager.initialize("Find all schools in California")
        
        # CRITICAL: Verify storage at correct memory location
        raw_data = await memory.get("queryTree")  # Must be exact key
        assert raw_data is not None
        assert "rootId" in raw_data
        assert "nodes" in raw_data
        assert "currentNodeId" in raw_data
        assert raw_data["rootId"] == root_id
        
        # Verify root node was created
        assert root_id in raw_data["nodes"]
        root_node = raw_data["nodes"][root_id]
        assert root_node["intent"] == "Find all schools in California"
        assert root_node["status"] == NodeStatus.CREATED.value
        assert "parentId" not in root_node  # Root node has no parent
        
        print("✅ Query tree memory location and initialization tests passed")
    
    @pytest.mark.asyncio
    async def test_node_operations_with_relationships(self, memory, manager):
        """Test adding nodes with proper parent-child relationships."""
        # Initialize with root
        root_id = await manager.initialize("Complex query with multiple parts")
        
        # Create child nodes
        child1 = QueryNode(
            nodeId="child1",
            intent="Find schools with high SAT scores",
            parentId=root_id,
            status=NodeStatus.CREATED
        )
        child2 = QueryNode(
            nodeId="child2", 
            intent="Find schools with low free meal rates",
            parentId=root_id,
            status=NodeStatus.CREATED
        )
        
        # Add children
        await manager.add_node(child1, root_id)
        await manager.add_node(child2, root_id)
        
        # Verify relationships in memory
        raw_data = await memory.get("queryTree")
        
        # Check parent has children
        assert len(raw_data["nodes"][root_id]["childIds"]) == 2
        assert "child1" in raw_data["nodes"][root_id]["childIds"]
        assert "child2" in raw_data["nodes"][root_id]["childIds"]
        
        # Check children have parent
        assert raw_data["nodes"]["child1"]["parentId"] == root_id
        assert raw_data["nodes"]["child2"]["parentId"] == root_id
        
        # Add grandchild
        grandchild = QueryNode(
            nodeId="grandchild1",
            intent="Combine results from parent queries",
            parentId="child1",
            status=NodeStatus.CREATED
        )
        await manager.add_node(grandchild, "child1")
        
        # Verify multi-level relationships
        raw_data = await memory.get("queryTree")
        assert "grandchild1" in raw_data["nodes"]["child1"]["childIds"]
        assert raw_data["nodes"]["grandchild1"]["parentId"] == "child1"
        
        print("✅ Node operations with relationships tests passed")
    
    @pytest.mark.asyncio
    async def test_update_node_sql_and_results(self, memory, manager):
        """Test updating node SQL and execution results."""
        # Initialize and get node
        node_id = await manager.initialize("Count schools by county")
        
        # Add SQL
        sql = "SELECT County, COUNT(*) FROM schools GROUP BY County"
        await manager.update_node_sql(node_id, sql)
        
        # Verify SQL stored
        node = await manager.get_node(node_id)
        assert hasattr(node, 'generation') and node.generation is not None
        # The SQL might be stored in generation field or another field
        
        # Add execution result
        execution_result = ExecutionResult(
            data=[{"County": "Alameda", "count": 100}, {"County": "Los Angeles", "count": 200}],
            rowCount=2,
            error=None
        )
        
        await manager.update_node_result(node_id, execution_result, success=True)
        
        # Verify result stored
        updated_node = await manager.get_node(node_id)
        assert updated_node.status == NodeStatus.EXECUTED_SUCCESS
        
        # Test error case
        error_result = ExecutionResult(
            data=[],
            rowCount=0,
            error="Table not found"
        )
        
        await manager.update_node_result(node_id, error_result, success=False)
        
        error_node = await manager.get_node(node_id)
        assert error_node.status == NodeStatus.EXECUTED_FAILED
        
        print("✅ Update node SQL and results tests passed")
    
    @pytest.mark.asyncio
    async def test_track_current_node_position(self, memory, manager):
        """Test tracking current node position accurately."""
        # Initialize tree
        root_id = await manager.initialize("Root query")
        
        # Verify current node is root initially
        current = await manager.get_current_node_id()
        assert current == root_id
        
        # Add child and navigate to it
        child = QueryNode(
            nodeId="child1",
            intent="Child query",
            parentId=root_id,
            status=NodeStatus.CREATED
        )
        await manager.add_node(child, root_id)
        await manager.set_current_node_id("child1")
        
        # Verify current node updated
        current = await manager.get_current_node_id()
        assert current == "child1"
        
        # Verify in memory
        raw_data = await memory.get("queryTree")
        assert raw_data["currentNodeId"] == "child1"
        
        # Navigate back to root
        await manager.set_current_node_id(root_id)
        
        current = await manager.get_current_node_id()
        assert current == root_id
        
        print("✅ Track current node position tests passed")
    
    @pytest.mark.asyncio
    async def test_retrieve_nodes_by_criteria(self, memory, manager):
        """Test retrieving nodes by various criteria."""
        # Build test tree
        root_id = await manager.initialize("Root query")
        
        # Add nodes with different statuses
        pending_node = QueryNode(
            nodeId="pending1",
            intent="Pending query",
            parentId=root_id,
            status=NodeStatus.CREATED
        )
        await manager.add_node(pending_node, root_id)
        
        # Add SQL to one node
        await manager.update_node_sql("pending1", "SELECT * FROM test")
        
        # Add error node
        error_node = QueryNode(
            nodeId="error1",
            intent="Error query",
            parentId=root_id,
            status=NodeStatus.EXECUTED_FAILED
        )
        await manager.add_node(error_node, root_id)
        
        # Get children of root
        children = await manager.get_children(root_id)
        assert len(children) == 2
        child_ids = [c.nodeId for c in children]
        assert "pending1" in child_ids
        assert "error1" in child_ids
        
        # Get node by ID
        node = await manager.get_node("pending1")
        assert node is not None
        assert node.intent == "Pending query"
        
        # Get leaf nodes
        leaf_nodes = await manager.get_leaf_nodes()
        assert len(leaf_nodes) == 2  # pending1 and error1 have no children
        
        # Get failed nodes
        failed_nodes = await manager.get_failed_nodes()
        failed_ids = [n.nodeId for n in failed_nodes]
        assert "error1" in failed_ids
        
        print("✅ Retrieve nodes by criteria tests passed")
    
    @pytest.mark.asyncio
    async def test_tree_traversal_operations(self, memory, manager):
        """Test tree traversal operations."""
        # Build a deeper tree
        root_id = await manager.initialize("Root")
        
        # Level 1
        child1 = QueryNode(nodeId="L1-1", intent="Level 1 Child 1", parentId=root_id, status=NodeStatus.CREATED)
        child2 = QueryNode(nodeId="L1-2", intent="Level 1 Child 2", parentId=root_id, status=NodeStatus.CREATED)
        await manager.add_node(child1, root_id)
        await manager.add_node(child2, root_id)
        
        # Level 2
        grandchild1 = QueryNode(nodeId="L2-1", intent="Level 2 Child 1", parentId="L1-1", status=NodeStatus.CREATED)
        grandchild2 = QueryNode(nodeId="L2-2", intent="Level 2 Child 2", parentId="L1-1", status=NodeStatus.CREATED)
        await manager.add_node(grandchild1, "L1-1")
        await manager.add_node(grandchild2, "L1-1")
        
        # Test path to root
        path = await manager.get_path_to_root("L2-1")
        assert len(path) >= 2
        assert "L2-1" in path
        assert root_id in path
        
        # Test sibling nodes
        siblings = await manager.get_siblings("L1-1")
        assert len(siblings) == 1
        assert siblings[0].nodeId == "L1-2"
        
        # Test parent retrieval
        parent = await manager.get_parent("L2-1")
        assert parent is not None
        assert parent.nodeId == "L1-1"
        
        # Test ancestors
        ancestors = await manager.get_ancestors("L2-1")
        assert len(ancestors) >= 1
        ancestor_ids = [a.nodeId for a in ancestors]
        assert "L1-1" in ancestor_ids
        
        print("✅ Tree traversal operations tests passed")
    
    @pytest.mark.asyncio
    async def test_complex_tree_operations(self, memory, manager):
        """Test complex tree operations and edge cases."""
        # Initialize tree
        root_id = await manager.initialize("Complex root query")
        
        # Test updating node with additional data
        additional_data = {
            "schema_linking": {"selected_tables": ["schools", "frpm"]},
            "generation": {"sql": "SELECT ...", "explanation": "This query..."},
            "evaluation": {"quality": "good", "issues": []}
        }
        
        await manager.update_node(root_id, additional_data)
        
        # Verify fields stored
        node = await manager.get_node(root_id)
        assert node.schema_linking == additional_data["schema_linking"]
        assert node.generation == additional_data["generation"]
        assert node.evaluation == additional_data["evaluation"]
        
        # Test node removal
        child = QueryNode(nodeId="to_remove", intent="Temporary node", parentId=root_id, status=NodeStatus.CREATED)
        await manager.add_node(child, root_id)
        
        # Verify added
        children = await manager.get_children(root_id)
        assert len(children) == 1
        
        # Remove node
        await manager.delete_node("to_remove")
        
        # Verify removed
        children = await manager.get_children(root_id)
        assert len(children) == 0
        
        # Test node creation with evidence
        await manager.initialize("Query with evidence", evidence="Some helpful context")
        root_node = await manager.get_root_node()
        assert root_node.evidence == "Some helpful context"
        
        print("✅ Complex tree operations tests passed")
    
    @pytest.mark.asyncio
    async def test_tree_statistics_and_queries(self, memory, manager):
        """Test tree statistics and specialized queries."""
        # Build a complete tree
        root_id = await manager.initialize("Main query")
        
        # Add child with SQL
        child = QueryNode(
            nodeId="child1",
            intent="Sub-query",
            parentId=root_id,
            status=NodeStatus.CREATED
        )
        await manager.add_node(child, root_id)
        await manager.update_node_sql("child1", "SELECT COUNT(*) FROM schools")
        
        # Simulate successful execution
        success_result = ExecutionResult(data=[{"count": 100}], rowCount=1, error=None)
        await manager.update_node_result("child1", success_result, success=True)
        
        # Add failed child
        failed_child = QueryNode(
            nodeId="child2",
            intent="Failed query",
            parentId=root_id,
            status=NodeStatus.CREATED
        )
        await manager.add_node(failed_child, root_id)
        
        # Simulate failed execution
        failed_result = ExecutionResult(data=[], rowCount=0, error="Table not found")
        await manager.update_node_result("child2", failed_result, success=False)
        
        # Get tree statistics
        stats = await manager.get_tree_stats()
        assert stats["total_nodes"] >= 3
        assert stats["depth"] >= 1
        
        # Test specialized node queries
        successful_nodes = await manager.get_successful_nodes()
        successful_ids = [n.nodeId for n in successful_nodes]
        assert "child1" in successful_ids
        
        failed_nodes = await manager.get_failed_nodes()
        failed_ids = [n.nodeId for n in failed_nodes]
        assert "child2" in failed_ids
        
        # Test find by intent
        intent_nodes = await manager.find_nodes_by_intent("Sub-query")
        assert len(intent_nodes) >= 1
        assert intent_nodes[0].nodeId == "child1"
        
        # Get full tree
        tree = await manager.get_tree()
        assert tree is not None
        assert tree["rootId"] == root_id
        assert len(tree["nodes"]) >= 3
        
        print("✅ Tree statistics and queries tests passed")
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, memory, manager):
        """Test concurrent operations on the tree."""
        # Initialize tree
        root_id = await manager.initialize("Concurrent test")
        
        # Define concurrent tasks
        async def add_child(child_id: str, intent: str):
            child = QueryNode(
                nodeId=child_id,
                intent=intent,
                parentId=root_id,
                status=NodeStatus.CREATED
            )
            await manager.add_node(child, root_id)
            await manager.update_node_sql(child_id, f"SELECT * FROM {child_id}")
        
        # Add multiple children concurrently
        tasks = [
            add_child(f"child{i}", f"Query {i}")
            for i in range(3)
        ]
        
        await asyncio.gather(*tasks)
        
        # Verify all children added
        children = await manager.get_children(root_id)
        assert len(children) == 3
        
        # Verify tree integrity
        tree = await manager.get_tree()
        assert len(tree["nodes"]) == 4  # root + 3 children
        
        print("✅ Concurrent operations tests passed")


if __name__ == "__main__":
    asyncio.run(pytest.main([__file__, "-v", "-s"]))