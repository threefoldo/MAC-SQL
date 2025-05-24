"""
Basic workflow test - Simple functionality validation
"""

import src.asyncio as asyncio
from src.datetime import datetime

# Import setup for tests
import src.sys as sys
from src.pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.memory import KeyValueMemory
from src.task_context_manager import TaskContextManager
from src.query_tree_manager import QueryTreeManager
from src.database_schema_manager import DatabaseSchemaManager
from src.node_history_manager import NodeHistoryManager
from src.memory_types import (
    TaskContext, TaskStatus, QueryNode, NodeStatus, 
    QueryMapping, TableMapping, ColumnMapping,
    ExecutionResult, NodeOperation, NodeOperationType
)


async def test_basic_workflow():
    """Test basic workflow functionality with correct data structures."""
    print("=== Basic Workflow Test ===")
    
    # Setup
    memory = KeyValueMemory(name="basic_test")
    task_manager = TaskContextManager(memory=memory)
    query_manager = QueryTreeManager(memory=memory)
    history_manager = NodeHistoryManager(memory=memory)
    
    # Test 1: Create task
    print("\n1. Testing task creation...")
    task_id = "basic_001"
    await task_manager.initialize(
        task_id=task_id,
        original_query="Count total customers",
        database_name="test_db"
    )
    
    task = await task_manager.get()
    assert task.taskId == task_id
    assert task.originalQuery == "Count total customers"
    print("✓ Task created successfully")
    
    # Initialize query tree
    await query_manager.initialize("Count total customers")
    
    # Test 2: Create simple query node
    print("\n2. Testing query node creation...")
    node = QueryNode(
        nodeId="node_001",
        intent="Count all customers",
        mapping=QueryMapping(
            tables=[TableMapping(name="customers", alias="c", purpose="Count rows")],
            columns=[ColumnMapping(table="customers", column="*", usedFor="count")]
        ),
        sql="SELECT COUNT(*) FROM customers",
        status=NodeStatus.SQL_GENERATED,
        executionResult=ExecutionResult(
            data=[[42]],  # Simple data structure
            rowCount=1
        )
    )
    
    await query_manager.add_node(node)
    # Note: Root node was already created by initialize()
    print("✓ Query node created successfully")
    
    # Test 3: Record operation
    print("\n3. Testing operation recording...")
    operation = NodeOperation(
        timestamp=datetime.now().isoformat(),
        nodeId="node_001",
        operation=NodeOperationType.GENERATE_SQL,
        data={"sql": node.sql}
    )
    
    await history_manager.add_operation(operation)
    operations = await history_manager.get_node_operations("node_001")
    assert len(operations) == 1
    print("✓ Operation recorded successfully")
    
    # Test 4: Verify data retrieval
    print("\n4. Testing data retrieval...")
    retrieved_node = await query_manager.get_node("node_001")
    assert retrieved_node.nodeId == "node_001"
    assert retrieved_node.sql == "SELECT COUNT(*) FROM customers"
    assert retrieved_node.executionResult.rowCount == 1
    print("✓ Data retrieved successfully")
    
    # Test 5: Update task status
    print("\n5. Testing task updates...")
    await task_manager.update_status(TaskStatus.COMPLETED)
    updated_task = await task_manager.get()
    assert updated_task.status == TaskStatus.COMPLETED
    print("✓ Task updated successfully")
    
    print("\n=== All Basic Workflow Tests Passed! ===")
    return True


async def test_multi_node_workflow():
    """Test workflow with multiple nodes."""
    print("\n=== Multi-Node Workflow Test ===")
    
    # Setup
    memory = KeyValueMemory(name="multi_test")
    task_manager = TaskContextManager(memory=memory)
    query_manager = QueryTreeManager(memory=memory)
    
    # Create task
    task_id = "multi_001"
    await task_manager.initialize(
        task_id=task_id,
        original_query="Find top customers by sales",
        database_name="sales_db"
    )
    
    # Initialize query tree
    await query_manager.initialize("Find top customers by sales")
    
    # Create parent node
    parent = QueryNode(
        nodeId="parent_001",
        intent="Find top customers by sales",
        mapping=QueryMapping(tables=[], columns=[]),
        childIds=["child_001", "child_002"],
        status=NodeStatus.CREATED
    )
    
    # Create child nodes
    child1 = QueryNode(
        nodeId="child_001",
        intent="Get customer sales totals",
        mapping=QueryMapping(
            tables=[
                TableMapping(name="customers", alias="c", purpose="Customer info"),
                TableMapping(name="orders", alias="o", purpose="Order amounts")
            ],
            columns=[
                ColumnMapping(table="customers", column="name", usedFor="select"),
                ColumnMapping(table="orders", column="amount", usedFor="aggregate")
            ]
        ),
        parentId="parent_001",
        sql="SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.id",
        status=NodeStatus.EXECUTED_SUCCESS,
        executionResult=ExecutionResult(
            data=[["Alice", 5000], ["Bob", 3000], ["Carol", 7000]],
            rowCount=3
        )
    )
    
    child2 = QueryNode(
        nodeId="child_002", 
        intent="Rank customers by sales",
        mapping=QueryMapping(
            tables=[TableMapping(name="customer_sales", alias="cs", purpose="Ranking")],
            columns=[ColumnMapping(table="customer_sales", column="total_sales", usedFor="ordering")]
        ),
        parentId="parent_001",
        sql="SELECT * FROM customer_sales ORDER BY total_sales DESC LIMIT 5",
        status=NodeStatus.EXECUTED_SUCCESS,
        executionResult=ExecutionResult(
            data=[["Carol", 7000], ["Alice", 5000], ["Bob", 3000]],
            rowCount=3
        )
    )
    
    # Add nodes
    await query_manager.add_node(parent)
    await query_manager.add_node(child1)
    await query_manager.add_node(child2)
    # Root node already created by initialize()
    
    # Verify tree structure - get all nodes
    tree = await query_manager.get_tree()
    assert len(tree["nodes"]) >= 3  # At least our nodes
    print("✓ Multi-node tree created successfully")
    
    # Test tree navigation
    children = await query_manager.get_children("parent_001")
    assert len(children) == 2
    assert children[0].nodeId in ["child_001", "child_002"]
    print("✓ Tree navigation working")
    
    print("=== Multi-Node Workflow Tests Passed! ===")
    return True


async def run_all_tests():
    """Run all basic workflow tests."""
    results = []
    
    try:
        success1 = await test_basic_workflow()
        results.append(("Basic Workflow", success1))
    except Exception as e:
        print(f"Basic workflow failed: {e}")
        results.append(("Basic Workflow", False))
    
    try:
        success2 = await test_multi_node_workflow()
        results.append(("Multi-Node Workflow", success2))
    except Exception as e:
        print(f"Multi-node workflow failed: {e}")
        results.append(("Multi-Node Workflow", False))
    
    # Summary
    print(f"\n{'='*50}")
    print("TEST SUMMARY")
    print(f"{'='*50}")
    
    for test_name, success in results:
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"{status:10} {test_name}")
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    print(f"\nResults: {passed}/{total} tests passed")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    exit(0 if success else 1)