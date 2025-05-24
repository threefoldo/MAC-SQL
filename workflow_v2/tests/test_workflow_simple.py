"""Simple test of workflow memory components.

This test validates the basic functionality of memory components
without the complexity of the full XML workflow.
"""

import src.json as json
import src.asyncio as asyncio
from src.datetime import datetime
from src.typing import Dict, Any

# Import setup for tests
import src.sys as sys
from src.pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.memory_types import (
    TableSchema, ColumnInfo,
    QueryNode, NodeStatus, QueryMapping, TableMapping, ColumnMapping,
    ExecutionResult,
    TaskContext, TaskStatus,
    NodeOperation, NodeOperationType
)
from src.memory import KeyValueMemory
from src.task_context_manager import TaskContextManager
from src.query_tree_manager import QueryTreeManager
from src.database_schema_manager import DatabaseSchemaManager
from src.node_history_manager import NodeHistoryManager


async def test_workflow_memory():
    """Test basic workflow memory operations."""
    print("\n" + "="*60)
    print("Testing Workflow Memory Components")
    print("="*60)
    
    # Create KeyValueMemory instance
    kv_memory = KeyValueMemory(name="test_memory")
    
    # Create managers
    task_manager = TaskContextManager(memory=kv_memory)
    tree_manager = QueryTreeManager(memory=kv_memory)
    schema_manager = DatabaseSchemaManager(memory=kv_memory)
    history_manager = NodeHistoryManager(memory=kv_memory)
    
    # 1. Test Task Context
    print("\n1. Testing Task Context Manager...")
    await task_manager.initialize(
        task_id="test_001",
        original_query="Find top districts with charter schools",
        database_name="california_schools"
    )
    
    # Update status
    await task_manager.update_status(TaskStatus.PROCESSING)
    
    # Retrieve context
    context = await task_manager.get()
    if context:
        print(f"   ✓ Task initialized: {context.taskId}")
        print(f"   ✓ Query: {context.originalQuery[:30]}...")
        print(f"   ✓ Status: {context.status.value}")
    
    # 2. Test Query Tree
    print("\n2. Testing Query Tree Manager...")
    
    # Initialize tree
    root_id = await tree_manager.initialize(root_intent="Main query")
    print(f"   ✓ Tree initialized with root: {root_id}")
    
    # Add a child node
    child_node = QueryNode(
        nodeId="child_1",
        intent="Calculate average scores",
        mapping=QueryMapping(
            tables=[TableMapping(name="satscores", alias="s", purpose="Get scores")],
            columns=[ColumnMapping(table="satscores", column="AvgScrMath", usedFor="aggregate")]
        ),
        sql="SELECT AVG(AvgScrMath) FROM satscores",
        status=NodeStatus.SQL_GENERATED
    )
    
    await tree_manager.add_node(child_node, parent_id=root_id)
    print(f"   ✓ Added child node: {child_node.nodeId}")
    
    # Get tree structure
    tree = await tree_manager.get_tree()
    if tree:
        print(f"   ✓ Tree has {len(tree['nodes'])} nodes")
    
    # 3. Test Database Schema
    print("\n3. Testing Database Schema Manager...")
    
    # Create table schema
    table = TableSchema(
        name="test_table",
        columns={
            'id': ColumnInfo(dataType='INTEGER', nullable=False, isPrimaryKey=True, isForeignKey=False),
            'name': ColumnInfo(dataType='TEXT', nullable=True, isPrimaryKey=False, isForeignKey=False)
        },
        metadata={'test': True}
    )
    
    await schema_manager.add_table(table)
    print(f"   ✓ Added table: {table.name}")
    
    # Retrieve table
    retrieved = await schema_manager.get_table("test_table")
    if retrieved:
        print(f"   ✓ Retrieved table with {len(retrieved.columns)} columns")
    
    # 4. Test Node History
    print("\n4. Testing Node History Manager...")
    
    # Initialize history
    await history_manager.initialize()
    
    # Add operation
    operation = NodeOperation(
        timestamp=datetime.now().isoformat(),
        nodeId="child_1",
        operation=NodeOperationType.CREATE,
        data={'intent': 'Calculate average scores'}
    )
    
    await history_manager.add_operation(operation)
    print(f"   ✓ Added operation: {operation.operation.value}")
    
    # Get operations
    ops = await history_manager.get_node_operations("child_1")
    print(f"   ✓ Retrieved {len(ops)} operations for child_1")
    
    # 5. Test Complex Query Tree
    print("\n5. Testing Complex Query Tree...")
    
    # Add multiple nodes with relationships
    nodes = [
        QueryNode(
            nodeId="sq1",
            intent="Get charter schools",
            mapping=QueryMapping(
                tables=[TableMapping(name="frpm", alias="f", purpose="Charter info")],
                columns=[ColumnMapping(table="frpm", column="Charter School (Y/N)", usedFor="filter")]
            ),
            sql="SELECT * FROM frpm WHERE `Charter School (Y/N)` = 1",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(data=[[1, 'School A']], rowCount=1)
        ),
        QueryNode(
            nodeId="sq2",
            intent="Calculate averages",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="frpm", alias="f", purpose="Join"),
                    TableMapping(name="satscores", alias="s", purpose="Scores")
                ],
                columns=[
                    ColumnMapping(table="frpm", column="CDSCode", usedFor="join"),
                    ColumnMapping(table="satscores", column="cds", usedFor="join"),
                    ColumnMapping(table="satscores", column="AvgScrMath", usedFor="aggregate")
                ]
            ),
            childIds=["sq1"],
            sql="SELECT AVG(s.AvgScrMath) FROM frpm f JOIN satscores s ON f.CDSCode = s.cds",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(data=[[650.5]], rowCount=1)
        )
    ]
    
    for node in nodes:
        await tree_manager.add_node(node)
        print(f"   ✓ Added node: {node.nodeId} - {node.intent}")
    
    # Get final tree
    final_tree = await tree_manager.get_tree()
    
    # 6. Export to JSON
    print("\n6. Testing JSON Export...")
    
    result = {
        'task_context': context.to_dict() if context else None,
        'query_tree': final_tree,
        'test_status': 'completed'
    }
    
    output_file = "test_workflow_simple_output.json"
    with open(output_file, 'w') as f:
        json.dump(result, f, indent=2)
    print(f"   ✓ Saved results to {output_file}")
    
    # Check memory usage
    print("\n7. Memory Statistics:")
    print(f"   ✓ Total memory entries: {len(kv_memory._store)}")
    
    # List all keys
    keys = set()
    for item in kv_memory._store:
        if item.metadata and 'variable_name' in item.metadata:
            keys.add(item.metadata['variable_name'])
    print(f"   ✓ Keys in memory: {keys}")
    
    print("\n" + "="*60)
    print("All tests completed successfully!")
    print("="*60)


async def main():
    """Run the simple workflow test."""
    try:
        await test_workflow_memory()
        return 0
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))