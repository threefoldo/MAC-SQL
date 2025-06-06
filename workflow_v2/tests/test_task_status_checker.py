"""
Test the TaskStatusChecker tool with the updated Pydantic model.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from keyvalue_memory import KeyValueMemory
from query_tree_manager import QueryTreeManager
from task_status_checker import TaskStatusChecker, TaskStatusCheckerArgs
from memory_content_types import QueryNode, NodeStatus


async def test_task_status_checker():
    """Test the task status checker with various tree states"""
    print("Testing TaskStatusChecker with Pydantic model...")
    print("=" * 60)
    
    # Initialize memory and managers
    memory = KeyValueMemory()
    tree_manager = QueryTreeManager(memory)
    
    # Initialize the task status checker
    checker = TaskStatusChecker(memory)
    
    # Test 1: Empty tree
    print("\nTest 1: Empty tree")
    args = TaskStatusCheckerArgs(task="Check empty tree")
    result = await checker.run(args)
    print(f"Result:\n{result}")
    assert "No query tree found" in result, "Should report no tree found for empty tree"
    
    # Test 2: Tree with unprocessed nodes
    print("\n\nTest 2: Tree with unprocessed nodes")
    print("-" * 40)
    
    # Create a simple tree
    root_id = await tree_manager.initialize("Find highest eligible free rate for K-12 students in Alameda County")
    
    # Add child nodes
    node1 = QueryNode(
        nodeId="node_001",
        intent="Filter schools in Alameda County",
        parentId=root_id,
        status=NodeStatus.CREATED
    )
    await tree_manager.add_node(node1, root_id)
    
    node2 = QueryNode(
        nodeId="node_002", 
        intent="Find highest eligible free rate",
        parentId=root_id,
        status=NodeStatus.CREATED
    )
    await tree_manager.add_node(node2, root_id)
    
    # Check status
    args = TaskStatusCheckerArgs(task="Check tree with unprocessed nodes")
    result = await checker.run(args)
    print(f"Result:\n{result}")
    assert "CURRENT_NODE: node_001" in result, "Should set current node"
    assert "CURRENT_STATUS: needs_sql" in result, "Should show node needs SQL"
    assert "Processing in progress" in result, "Should indicate processing is in progress"
    assert "node_001" in result or "node_002" in result, "Should mention a specific node"
    
    # Test 3: Tree with some processed nodes
    print("\n\nTest 3: Tree with processed nodes")
    print("-" * 40)
    
    # Update node1 with SQL and good results
    await tree_manager.update_node_sql("node_001", "SELECT * FROM schools WHERE County = 'Alameda'")
    await tree_manager.update_node("node_001", {"status": NodeStatus.EXECUTED_SUCCESS.value})
    
    # Store evaluation result in node's evaluation field
    await tree_manager.update_node("node_001", {
        "evaluation": {
            "result_quality": "excellent",
            "answers_intent": "yes"
        }
    })
    
    # Check status
    args = TaskStatusCheckerArgs(task="Check tree with partial processing")
    result = await checker.run(args)
    print(f"Result:\n{result}")
    assert "Processing in progress" in result, "Should still have nodes to process"
    assert "1/3 nodes complete" in result or "PENDING:" in result, "Should show progress"
    
    # Test 4: All nodes processed successfully
    print("\n\nTest 4: All nodes processed successfully")
    print("-" * 40)
    
    # Update node2 with SQL and good results
    await tree_manager.update_node_sql("node_002", "SELECT MAX(eligible_free_rate) FROM frpm")
    await tree_manager.update_node("node_002", {"status": NodeStatus.EXECUTED_SUCCESS.value})
    
    # Store evaluation result in node's evaluation field
    await tree_manager.update_node("node_002", {
        "evaluation": {
            "result_quality": "good",
            "answers_intent": "yes"
        }
    })
    
    # Update root node as well
    await tree_manager.update_node_sql(root_id, "WITH alameda_schools AS (...)")
    await tree_manager.update_node(root_id, {"status": NodeStatus.EXECUTED_SUCCESS.value})
    await tree_manager.update_node(root_id, {
        "evaluation": {
            "result_quality": "excellent",
            "answers_intent": "yes"
        }
    })
    
    # Check status
    args = TaskStatusCheckerArgs(task="Check fully processed tree")
    result = await checker.run(args)
    print(f"Result:\n{result}")
    assert "All nodes complete" in result, "Should indicate task completion"
    
    # Test 5: Node with poor quality results
    print("\n\nTest 5: Node with poor quality results")
    print("-" * 40)
    
    # Update node2 with poor quality
    await tree_manager.update_node("node_002", {
        "evaluation": {
            "result_quality": "poor",
            "answers_intent": "no"
        }
    })
    
    # Check status
    args = TaskStatusCheckerArgs(task="Check tree with poor quality node")
    result = await checker.run(args)
    print(f"Result:\n{result}")
    assert "bad_sql" in result or "bad SQL" in result, "Should show bad SQL status"
    assert "Processing in progress" in result, "Should still be processing due to bad SQL"
    
    print("\n\n" + "=" * 60)
    print("✅ All tests passed!")
    print("The TaskStatusChecker works correctly with the Pydantic model.")


async def test_tool_interface():
    """Test that the tool interface works correctly"""
    print("\n\nTesting Tool Interface...")
    print("=" * 60)
    
    memory = KeyValueMemory()
    checker = TaskStatusChecker(memory)
    
    # Get the tool
    tool = checker.get_tool()
    
    # Check tool properties
    print(f"Tool name: {tool.name}")
    print(f"Tool description: {tool.description}")
    print(f"Args type: {tool._args_type}")
    print(f"Return type: {tool._return_type}")
    
    # Verify args_type is the Pydantic model
    assert tool._args_type == TaskStatusCheckerArgs, "Args type should be TaskStatusCheckerArgs"
    
    # Test creating args
    args = TaskStatusCheckerArgs(task="Test task")
    print(f"\nCreated args: {args}")
    print(f"Args dict: {args.model_dump()}")
    
    # Test that the Pydantic model has the required method
    assert hasattr(TaskStatusCheckerArgs, 'model_json_schema'), "TaskStatusCheckerArgs should have model_json_schema method"
    
    schema = TaskStatusCheckerArgs.model_json_schema()
    print(f"\nArgs schema: {schema}")
    
    print("\n✅ Tool interface test passed!")


async def main():
    """Run all tests"""
    await test_task_status_checker()
    await test_tool_interface()


if __name__ == "__main__":
    asyncio.run(main())