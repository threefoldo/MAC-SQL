"""
Layer 3: Test Memory Managers
"""

import src.asyncio as asyncio
import src.pytest as pytest
from src.datetime import datetime
from src.typing import Dict, Any, List


# Import setup for tests
import src.sys as sys
from src.pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.memory import KeyValueMemory
from src.task_context_manager import TaskContextManager
from src.database_schema_manager import DatabaseSchemaManager
from src.query_tree_manager import QueryTreeManager
from src.node_history_manager import NodeHistoryManager
from src.memory_types import (
    TaskStatus, NodeStatus, NodeOperationType,
    TableSchema, ColumnInfo, QueryNode, QueryMapping,
    TableMapping, ColumnMapping, JoinMapping, CombineStrategy,
    CombineStrategyType, ExecutionResult
)


class TestTaskContextManager:
    """Test task context management."""
    
    async def test_task_initialization(self):
        """Test task initialization."""
        memory = KeyValueMemory()
        manager = TaskContextManager(memory)
        
        # Initialize task
        task = await manager.initialize(
            task_id="test_task_001",
            original_query="Find all premium customers",
            database_name="ecommerce_db"
        )
        
        assert task.taskId == "test_task_001"
        assert task.originalQuery == "Find all premium customers"
        assert task.databaseName == "ecommerce_db"
        assert task.status == TaskStatus.INITIALIZING
        
        # Verify storage
        stored_task = await manager.get()
        assert stored_task.taskId == task.taskId
        
        print("✅ Task initialization tests passed")
    
    async def test_status_updates(self):
        """Test task status transitions."""
        memory = KeyValueMemory()
        manager = TaskContextManager(memory)
        
        # Initialize
        await manager.initialize("test_002", "test query", "test_db")
        
        # Update to processing
        await manager.mark_as_processing()
        status = await manager.get_status()
        assert status == TaskStatus.PROCESSING
        
        # Update to completed
        await manager.mark_as_completed()
        assert await manager.is_completed() == True
        assert await manager.is_failed() == False
        
        # Test failed status
        await manager.mark_as_failed()
        assert await manager.is_failed() == True
        assert await manager.is_completed() == False
        
        print("✅ Status update tests passed")
    
    async def test_getters(self):
        """Test individual getter methods."""
        memory = KeyValueMemory()
        manager = TaskContextManager(memory)
        
        await manager.initialize(
            task_id="test_003",
            original_query="Complex query",
            database_name="analytics_db"
        )
        
        assert await manager.get_task_id() == "test_003"
        assert await manager.get_original_query() == "Complex query"
        assert await manager.get_database_name() == "analytics_db"
        
        print("✅ Getter methods tests passed")


class TestDatabaseSchemaManager:
    """Test database schema management."""
    
    async def test_schema_initialization(self):
        """Test schema initialization."""
        memory = KeyValueMemory()
        manager = DatabaseSchemaManager(memory)
        
        await manager.initialize()
        
        # Should start empty
        tables = await manager.get_all_tables()
        assert len(tables) == 0
        
        print("✅ Schema initialization tests passed")
    
    async def test_table_operations(self):
        """Test adding and retrieving tables."""
        memory = KeyValueMemory()
        manager = DatabaseSchemaManager(memory)
        await manager.initialize()
        
        # Create table schema
        customers_table = TableSchema(
            name="customers",
            columns={
                "id": ColumnInfo(
                    dataType="INTEGER",
                    nullable=False,
                    isPrimaryKey=True,
                    isForeignKey=False
                ),
                "name": ColumnInfo(
                    dataType="VARCHAR(100)",
                    nullable=False,
                    isPrimaryKey=False,
                    isForeignKey=False
                ),
                "email": ColumnInfo(
                    dataType="VARCHAR(100)",
                    nullable=True,
                    isPrimaryKey=False,
                    isForeignKey=False
                )
            },
            sampleData=[
                {"id": 1, "name": "John Doe", "email": "john@example.com"},
                {"id": 2, "name": "Jane Smith", "email": None}
            ]
        )
        
        # Add table
        await manager.add_table(customers_table)
        
        # Retrieve table
        retrieved = await manager.get_table("customers")
        assert retrieved.name == "customers"
        assert len(retrieved.columns) == 3
        assert retrieved.columns["id"].isPrimaryKey == True
        
        # Get all tables
        all_tables = await manager.get_all_tables()
        assert len(all_tables) == 1
        assert "customers" in all_tables
        
        # Get table names
        names = await manager.get_table_names()
        assert names == ["customers"]
        
        print("✅ Table operations tests passed")
    
    async def test_column_operations(self):
        """Test column-specific operations."""
        memory = KeyValueMemory()
        manager = DatabaseSchemaManager(memory)
        await manager.initialize()
        
        # Add table with foreign key
        orders_table = TableSchema(
            name="orders",
            columns={
                "id": ColumnInfo(
                    dataType="INTEGER",
                    nullable=False,
                    isPrimaryKey=True,
                    isForeignKey=False
                ),
                "customer_id": ColumnInfo(
                    dataType="INTEGER",
                    nullable=False,
                    isPrimaryKey=False,
                    isForeignKey=True,
                    references={"table": "customers", "column": "id"}
                ),
                "total": ColumnInfo(
                    dataType="DECIMAL(10,2)",
                    nullable=False,
                    isPrimaryKey=False,
                    isForeignKey=False
                )
            }
        )
        
        await manager.add_table(orders_table)
        
        # Get specific column
        customer_id_col = await manager.get_column("orders", "customer_id")
        assert customer_id_col.isForeignKey == True
        assert customer_id_col.references["table"] == "customers"
        
        # Get all columns
        columns = await manager.get_columns("orders")
        assert len(columns) == 3
        
        # Get primary keys
        pks = await manager.get_primary_keys("orders")
        assert pks == ["id"]
        
        # Get foreign keys
        fks = await manager.get_foreign_keys("orders")
        assert len(fks) == 1
        assert fks[0]["column"] == "customer_id"
        assert fks[0]["references_table"] == "customers"
        
        print("✅ Column operations tests passed")
    
    async def test_relationship_finding(self):
        """Test finding relationships between tables."""
        memory = KeyValueMemory()
        manager = DatabaseSchemaManager(memory)
        await manager.initialize()
        
        # Add customers table
        customers = TableSchema(
            name="customers",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False, 
                               isPrimaryKey=True, isForeignKey=False)
            }
        )
        await manager.add_table(customers)
        
        # Add orders table with FK
        orders = TableSchema(
            name="orders",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False),
                "customer_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=False, isForeignKey=True,
                    references={"table": "customers", "column": "id"}
                )
            }
        )
        await manager.add_table(orders)
        
        # Find relationships
        rels = await manager.find_relationships("orders", "customers")
        assert len(rels) == 1
        assert rels[0]["from_table"] == "orders"
        assert rels[0]["to_table"] == "customers"
        assert rels[0]["type"] == "foreign_key"
        
        print("✅ Relationship finding tests passed")
    
    async def test_metadata_and_search(self):
        """Test metadata and search operations."""
        memory = KeyValueMemory()
        manager = DatabaseSchemaManager(memory)
        await manager.initialize()
        
        # Add tables with different column types
        table1 = TableSchema(
            name="table1",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False),
                "name": ColumnInfo(dataType="VARCHAR(50)", nullable=True,
                                 isPrimaryKey=False, isForeignKey=False)
            },
            metadata={"rowCount": 1000, "indexes": ["idx_name"]}
        )
        
        table2 = TableSchema(
            name="table2",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False),
                "amount": ColumnInfo(dataType="DECIMAL(10,2)", nullable=False,
                                   isPrimaryKey=False, isForeignKey=False)
            }
        )
        
        await manager.add_table(table1)
        await manager.add_table(table2)
        
        # Test metadata
        meta = await manager.get_table_metadata("table1")
        assert meta["rowCount"] == 1000
        
        # Update metadata
        await manager.set_table_metadata("table2", {"rowCount": 500})
        meta2 = await manager.get_table_metadata("table2")
        assert meta2["rowCount"] == 500
        
        # Search columns by type
        int_cols = await manager.search_columns_by_type("INTEGER")
        assert len(int_cols) == 2
        
        # Get schema summary
        summary = await manager.get_schema_summary()
        assert summary["table_count"] == 2
        assert summary["total_columns"] == 4
        assert summary["total_primary_keys"] == 2
        
        print("✅ Metadata and search tests passed")


class TestQueryTreeManager:
    """Test query tree management."""
    
    async def test_tree_initialization(self):
        """Test tree initialization with root node."""
        memory = KeyValueMemory()
        manager = QueryTreeManager(memory)
        
        root_id = await manager.initialize("Find all customers who ordered last month")
        
        # Verify root created
        assert root_id is not None
        assert "root" in root_id
        
        # Get root node
        root = await manager.get_node(root_id)
        assert root.intent == "Find all customers who ordered last month"
        assert root.status == NodeStatus.CREATED
        assert len(root.childIds) == 0
        
        # Check tree structure
        tree = await manager.get_tree()
        assert tree["rootId"] == root_id
        assert tree["currentNodeId"] == root_id
        
        print("✅ Tree initialization tests passed")
    
    async def test_node_operations(self):
        """Test adding, updating, and deleting nodes."""
        memory = KeyValueMemory()
        manager = QueryTreeManager(memory)
        
        root_id = await manager.initialize("Main query")
        
        # Add child node
        child1 = QueryNode(
            nodeId="child_1",
            intent="Subquery 1",
            mapping=QueryMapping(
                tables=[TableMapping(name="customers", purpose="Get customer data")]
            )
        )
        
        await manager.add_node(child1, root_id)
        
        # Verify parent-child relationship
        root = await manager.get_node(root_id)
        assert "child_1" in root.childIds
        
        child = await manager.get_node("child_1")
        assert child.parentId == root_id
        
        # Update node
        await manager.update_node("child_1", {"status": NodeStatus.SQL_GENERATED.value})
        updated = await manager.get_node("child_1")
        assert updated.status == NodeStatus.SQL_GENERATED
        
        # Add SQL
        await manager.update_node_sql("child_1", "SELECT * FROM customers")
        updated = await manager.get_node("child_1")
        assert updated.sql == "SELECT * FROM customers"
        
        print("✅ Node operations tests passed")
    
    async def test_tree_navigation(self):
        """Test tree navigation methods."""
        memory = KeyValueMemory()
        manager = QueryTreeManager(memory)
        
        # Build a tree structure
        root_id = await manager.initialize("Root")
        
        # Add two children to root
        child1 = QueryNode(nodeId="child_1", intent="Child 1", mapping=QueryMapping())
        child2 = QueryNode(nodeId="child_2", intent="Child 2", mapping=QueryMapping())
        await manager.add_node(child1, root_id)
        await manager.add_node(child2, root_id)
        
        # Add grandchildren
        grandchild1 = QueryNode(nodeId="gc_1", intent="Grandchild 1", mapping=QueryMapping())
        await manager.add_node(grandchild1, "child_1")
        
        # Test get children
        children = await manager.get_children(root_id)
        assert len(children) == 2
        assert any(c.nodeId == "child_1" for c in children)
        
        # Test get parent
        parent = await manager.get_parent("gc_1")
        assert parent.nodeId == "child_1"
        
        # Test get siblings
        siblings = await manager.get_siblings("child_1")
        assert len(siblings) == 1
        assert siblings[0].nodeId == "child_2"
        
        # Test get ancestors
        ancestors = await manager.get_ancestors("gc_1")
        assert len(ancestors) == 2
        assert ancestors[0].nodeId == "child_1"
        assert ancestors[1].nodeId == root_id
        
        # Test path to root
        path = await manager.get_path_to_root("gc_1")
        assert path == [root_id, "child_1", "gc_1"]
        
        print("✅ Tree navigation tests passed")
    
    async def test_node_status_queries(self):
        """Test finding nodes by status."""
        memory = KeyValueMemory()
        manager = QueryTreeManager(memory)
        
        root_id = await manager.initialize("Root")
        
        # Create nodes with different statuses
        nodes = [
            QueryNode(nodeId="n1", intent="Query 1", mapping=QueryMapping(),
                     status=NodeStatus.SQL_GENERATED, sql="SELECT 1"),
            QueryNode(nodeId="n2", intent="Query 2", mapping=QueryMapping(),
                     status=NodeStatus.EXECUTED_SUCCESS,
                     sql="SELECT 2",
                     executionResult=ExecutionResult(data=[], rowCount=5)),
            QueryNode(nodeId="n3", intent="Query 3", mapping=QueryMapping(),
                     status=NodeStatus.EXECUTED_FAILED,
                     sql="SELECT 3",
                     executionResult=ExecutionResult(data=[], rowCount=0, error="Syntax error"))
        ]
        
        for node in nodes:
            await manager.add_node(node, root_id)
        
        # Test executable nodes
        executable = await manager.get_executable_nodes()
        assert len(executable) == 1
        assert executable[0].nodeId == "n1"
        
        # Test failed nodes
        failed = await manager.get_failed_nodes()
        assert len(failed) == 1
        assert failed[0].nodeId == "n3"
        
        # Test successful nodes
        successful = await manager.get_successful_nodes()
        assert len(successful) == 1
        assert successful[0].nodeId == "n2"
        
        # Test leaf nodes
        leaves = await manager.get_leaf_nodes()
        assert len(leaves) == 3  # All are leaves
        
        print("✅ Node status query tests passed")
    
    async def test_tree_statistics(self):
        """Test tree statistics calculation."""
        memory = KeyValueMemory()
        manager = QueryTreeManager(memory)
        
        # Build a tree
        root_id = await manager.initialize("Root")
        
        # Add nodes at different levels
        await manager.add_node(
            QueryNode(nodeId="l1_1", intent="Level 1-1", mapping=QueryMapping()),
            root_id
        )
        await manager.add_node(
            QueryNode(nodeId="l1_2", intent="Level 1-2", mapping=QueryMapping(),
                     status=NodeStatus.EXECUTED_SUCCESS),
            root_id
        )
        await manager.add_node(
            QueryNode(nodeId="l2_1", intent="Level 2-1", mapping=QueryMapping(),
                     status=NodeStatus.EXECUTED_FAILED),
            "l1_1"
        )
        
        # Get stats
        stats = await manager.get_tree_stats()
        assert stats["total_nodes"] == 4  # root + 3
        assert stats["depth"] == 2  # root -> l1 -> l2
        assert stats["leaf_nodes"] == 2  # l1_2 and l2_1
        assert stats["executed_nodes"] == 2  # success + failed
        assert stats["failed_nodes"] == 1
        
        print("✅ Tree statistics tests passed")


class TestNodeHistoryManager:
    """Test node operation history management."""
    
    async def test_history_initialization(self):
        """Test history initialization."""
        memory = KeyValueMemory()
        manager = NodeHistoryManager(memory)
        
        await manager.initialize()
        
        # Should start empty
        history = await manager.get_all_operations()
        assert len(history) == 0
        
        print("✅ History initialization tests passed")
    
    async def test_operation_recording(self):
        """Test recording different operation types."""
        memory = KeyValueMemory()
        manager = NodeHistoryManager(memory)
        await manager.initialize()
        
        # Record create
        await manager.record_create(
            node_id="node_1",
            intent="Find customers",
            mapping=QueryMapping(
                tables=[TableMapping(name="customers", purpose="Main table")]
            )
        )
        
        # Record SQL generation
        await manager.record_generate_sql(
            node_id="node_1",
            sql="SELECT * FROM customers"
        )
        
        # Record execution
        await manager.record_execute(
            node_id="node_1",
            sql="SELECT * FROM customers",
            result=[{"id": 1}, {"id": 2}],
            error=None
        )
        
        # Record revision
        await manager.record_revise(
            node_id="node_1",
            new_sql="SELECT id, name FROM customers",
            previous_sql="SELECT * FROM customers"
        )
        
        # Record deletion
        await manager.record_delete(
            node_id="node_1",
            reason="No longer needed"
        )
        
        # Verify all operations recorded
        all_ops = await manager.get_all_operations()
        assert len(all_ops) == 5
        
        # Verify operation types
        op_types = [op.operation for op in all_ops]
        assert NodeOperationType.CREATE in op_types
        assert NodeOperationType.GENERATE_SQL in op_types
        assert NodeOperationType.EXECUTE in op_types
        assert NodeOperationType.REVISE in op_types
        assert NodeOperationType.DELETE in op_types
        
        print("✅ Operation recording tests passed")
    
    async def test_operation_queries(self):
        """Test querying operations."""
        memory = KeyValueMemory()
        manager = NodeHistoryManager(memory)
        await manager.initialize()
        
        # Add operations for multiple nodes
        await manager.record_create("node_1", "Query 1")
        await manager.record_generate_sql("node_1", "SELECT 1")
        await manager.record_execute("node_1", "SELECT 1", result=[{"col": 1}])
        
        await manager.record_create("node_2", "Query 2")
        await manager.record_generate_sql("node_2", "SELECT 2")
        await manager.record_execute("node_2", "SELECT 2", error="Table not found")
        
        # Get operations for specific node
        node1_ops = await manager.get_node_operations("node_1")
        assert len(node1_ops) == 3
        
        # Get operations by type
        create_ops = await manager.get_operations_by_type(NodeOperationType.CREATE)
        assert len(create_ops) == 2
        
        execute_ops = await manager.get_operations_by_type(NodeOperationType.EXECUTE)
        assert len(execute_ops) == 2
        
        # Get latest operation
        latest = await manager.get_latest_operation("node_1")
        assert latest.operation == NodeOperationType.EXECUTE
        
        latest_sql = await manager.get_latest_operation("node_1", NodeOperationType.GENERATE_SQL)
        assert latest_sql.data["sql"] == "SELECT 1"
        
        print("✅ Operation query tests passed")
    
    async def test_lifecycle_tracking(self):
        """Test node lifecycle tracking."""
        memory = KeyValueMemory()
        manager = NodeHistoryManager(memory)
        await manager.initialize()
        
        # Complete lifecycle
        await manager.record_create("node_1", "Test query")
        await manager.record_generate_sql("node_1", "SELECT * FROM test")
        await manager.record_execute("node_1", "SELECT * FROM test")
        await manager.record_revise("node_1", new_sql="SELECT id FROM test")
        await manager.record_execute("node_1", "SELECT id FROM test")
        
        # Get lifecycle
        lifecycle = await manager.get_node_lifecycle("node_1")
        assert lifecycle["nodeId"] == "node_1"
        assert lifecycle["created"] is not None
        assert lifecycle["sql_generated"] is not None
        assert lifecycle["executed"] is not None
        assert lifecycle["revised_count"] == 1
        assert lifecycle["total_operations"] == 5
        
        print("✅ Lifecycle tracking tests passed")
    
    async def test_failure_tracking(self):
        """Test tracking failed executions."""
        memory = KeyValueMemory()
        manager = NodeHistoryManager(memory)
        await manager.initialize()
        
        # Add some failed executions
        await manager.record_execute("node_1", "BAD SQL", error="Syntax error")
        await manager.record_execute("node_2", "SELECT * FROM missing", error="Table not found")
        await manager.record_execute("node_3", "SELECT 1", result=[{"col": 1}])  # Success
        
        # Get failed executions
        failed = await manager.get_failed_executions()
        assert len(failed) == 2
        
        # Check error messages
        errors = [op.data["error"] for op in failed]
        assert "Syntax error" in errors
        assert "Table not found" in errors
        
        # Get deleted nodes
        await manager.record_delete("node_1", "Invalid query")
        deleted = await manager.get_deleted_nodes()
        assert "node_1" in deleted
        
        print("✅ Failure tracking tests passed")
    
    async def test_history_summary(self):
        """Test history summary generation."""
        memory = KeyValueMemory()
        manager = NodeHistoryManager(memory)
        await manager.initialize()
        
        # Create a rich history
        nodes = ["node_1", "node_2", "node_3"]
        
        for node in nodes:
            await manager.record_create(node, f"Query for {node}")
            await manager.record_generate_sql(node, f"SELECT * FROM {node}")
        
        # Some executions
        await manager.record_execute("node_1", "SELECT 1", result=[{"col": 1}])
        await manager.record_execute("node_2", "SELECT 2", error="Failed")
        
        # Delete one
        await manager.record_delete("node_3", "Not needed")
        
        # Get summary
        summary = await manager.get_history_summary()
        assert summary["total_operations"] == 9  # 3 create + 3 sql + 2 execute + 1 delete = 9
        assert summary["unique_nodes"] == 3
        assert summary["operation_counts"][NodeOperationType.CREATE.value] == 3
        assert summary["operation_counts"][NodeOperationType.GENERATE_SQL.value] == 3
        assert summary["operation_counts"][NodeOperationType.EXECUTE.value] == 2
        assert summary["failed_executions"] == 1
        assert summary["deleted_nodes"] == 1
        
        print("✅ History summary tests passed")


async def run_all_tests():
    """Run all manager tests."""
    print("="*60)
    print("LAYER 3: MEMORY MANAGERS TESTING")
    print("="*60)
    
    # Test Task Context Manager
    print("\n--- Testing TaskContextManager ---")
    task_tester = TestTaskContextManager()
    await task_tester.test_task_initialization()
    await task_tester.test_status_updates()
    await task_tester.test_getters()
    
    # Test Database Schema Manager
    print("\n--- Testing DatabaseSchemaManager ---")
    schema_tester = TestDatabaseSchemaManager()
    await schema_tester.test_schema_initialization()
    await schema_tester.test_table_operations()
    await schema_tester.test_column_operations()
    await schema_tester.test_relationship_finding()
    await schema_tester.test_metadata_and_search()
    
    # Test Query Tree Manager
    print("\n--- Testing QueryTreeManager ---")
    tree_tester = TestQueryTreeManager()
    await tree_tester.test_tree_initialization()
    await tree_tester.test_node_operations()
    await tree_tester.test_tree_navigation()
    await tree_tester.test_node_status_queries()
    await tree_tester.test_tree_statistics()
    
    # Test Node History Manager
    print("\n--- Testing NodeHistoryManager ---")
    history_tester = TestNodeHistoryManager()
    await history_tester.test_history_initialization()
    await history_tester.test_operation_recording()
    await history_tester.test_operation_queries()
    await history_tester.test_lifecycle_tracking()
    await history_tester.test_failure_tracking()
    await history_tester.test_history_summary()
    
    print("\n✅ All Layer 3 tests passed!")


if __name__ == "__main__":
    asyncio.run(run_all_tests())