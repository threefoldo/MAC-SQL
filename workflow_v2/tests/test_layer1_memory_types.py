"""
Layer 1: Test Memory Types and Data Structures
"""

import asyncio
import pytest
from datetime import datetime
from typing import Dict, Any

# Import setup for tests
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from memory_types import (
    TaskContext, TaskStatus, NodeStatus, NodeOperationType,
    CombineStrategyType, ColumnInfo, TableSchema, TableMapping,
    ColumnMapping, JoinMapping, QueryMapping, CombineStrategy,
    ExecutionResult, QueryNode, NodeOperation
)


class TestMemoryTypes:
    """Test all data structures and their serialization."""
    
    def test_task_context(self):
        """Test TaskContext creation and conversion."""
        # Create TaskContext
        task = TaskContext(
            taskId="test_123",
            originalQuery="Find all customers from New York",
            databaseName="test_db",
            startTime=datetime.now().isoformat(),
            status=TaskStatus.PROCESSING
        )
        
        # Test to_dict
        task_dict = task.to_dict()
        assert task_dict["taskId"] == "test_123"
        assert task_dict["status"] == "processing"
        
        # Test from_dict
        task2 = TaskContext.from_dict(task_dict)
        assert task2.taskId == task.taskId
        assert task2.status == TaskStatus.PROCESSING
        
        print("✅ TaskContext tests passed")
    
    def test_column_info(self):
        """Test ColumnInfo structure."""
        # Create column with foreign key
        col = ColumnInfo(
            dataType="INTEGER",
            nullable=False,
            isPrimaryKey=False,
            isForeignKey=True,
            references={"table": "customers", "column": "id"}
        )
        
        # Test serialization
        col_dict = col.to_dict()
        assert col_dict["isForeignKey"] == True
        assert col_dict["references"]["table"] == "customers"
        
        # Test deserialization
        col2 = ColumnInfo.from_dict(col_dict)
        assert col2.references["column"] == "id"
        
        print("✅ ColumnInfo tests passed")
    
    def test_table_schema(self):
        """Test TableSchema with columns and metadata."""
        # Create table schema
        table = TableSchema(
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
                )
            },
            sampleData=[
                {"id": 1, "name": "John Doe"},
                {"id": 2, "name": "Jane Smith"}
            ],
            metadata={"rowCount": 1000, "indexes": ["idx_name"]}
        )
        
        # Test serialization
        table_dict = table.to_dict()
        assert len(table_dict["columns"]) == 2
        assert len(table_dict["sampleData"]) == 2
        assert table_dict["metadata"]["rowCount"] == 1000
        
        # Test deserialization
        table2 = TableSchema.from_dict("customers", table_dict)
        assert table2.name == "customers"
        assert table2.columns["id"].isPrimaryKey == True
        
        print("✅ TableSchema tests passed")
    
    def test_query_mapping(self):
        """Test QueryMapping with tables, columns, and joins."""
        # Create mapping
        mapping = QueryMapping(
            tables=[
                TableMapping(name="customers", alias="c", purpose="Get customer data"),
                TableMapping(name="orders", alias="o", purpose="Get order data")
            ],
            columns=[
                ColumnMapping(table="customers", column="id", usedFor="join"),
                ColumnMapping(table="customers", column="name", usedFor="select"),
                ColumnMapping(table="orders", column="total", usedFor="aggregate")
            ],
            joins=[
                JoinMapping(
                    from_table="customers",
                    to="orders",
                    on="customers.id = orders.customer_id"
                )
            ]
        )
        
        # Test serialization
        mapping_dict = mapping.to_dict()
        assert len(mapping_dict["tables"]) == 2
        assert len(mapping_dict["columns"]) == 3
        assert len(mapping_dict["joins"]) == 1
        
        # Test deserialization
        mapping2 = QueryMapping.from_dict(mapping_dict)
        assert mapping2.tables[0].name == "customers"
        assert mapping2.joins[0].from_table == "customers"
        
        print("✅ QueryMapping tests passed")
    
    def test_combine_strategy(self):
        """Test CombineStrategy for different types."""
        # Test UNION strategy
        union_strategy = CombineStrategy(
            type=CombineStrategyType.UNION,
            unionType="UNION ALL"
        )
        assert union_strategy.to_dict()["type"] == "union"
        
        # Test JOIN strategy
        join_strategy = CombineStrategy(
            type=CombineStrategyType.JOIN,
            joinType="INNER",
            joinOn=["customer_id", "order_id"]
        )
        join_dict = join_strategy.to_dict()
        assert join_dict["joinType"] == "INNER"
        assert len(join_dict["joinOn"]) == 2
        
        # Test AGGREGATE strategy
        agg_strategy = CombineStrategy(
            type=CombineStrategyType.AGGREGATE,
            aggregateFunction="SUM",
            groupBy=["customer_id", "product_id"]
        )
        agg_dict = agg_strategy.to_dict()
        assert agg_dict["aggregateFunction"] == "SUM"
        
        print("✅ CombineStrategy tests passed")
    
    def test_query_node(self):
        """Test QueryNode with all fields."""
        # Create node
        node = QueryNode(
            nodeId="node_123",
            intent="Find top customers",
            mapping=QueryMapping(
                tables=[TableMapping(name="customers", purpose="Main table")]
            ),
            status=NodeStatus.SQL_GENERATED,
            sql="SELECT * FROM customers LIMIT 10",
            parentId="node_root",
            childIds=["node_456", "node_789"]
        )
        
        # Test serialization
        node_dict = node.to_dict()
        assert node_dict["nodeId"] == "node_123"
        assert node_dict["status"] == "sql_generated"
        assert "sql" in node_dict
        assert len(node_dict["childIds"]) == 2
        
        # Test deserialization
        node2 = QueryNode.from_dict(node_dict)
        assert node2.intent == "Find top customers"
        assert node2.status == NodeStatus.SQL_GENERATED
        
        print("✅ QueryNode tests passed")
    
    def test_execution_result(self):
        """Test ExecutionResult structure."""
        # Create result
        result = ExecutionResult(
            data=[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
            rowCount=2,
            error=None
        )
        
        # Test with error
        error_result = ExecutionResult(
            data=[],
            rowCount=0,
            error="SQL syntax error"
        )
        
        assert result.to_dict()["rowCount"] == 2
        assert error_result.to_dict()["error"] == "SQL syntax error"
        
        print("✅ ExecutionResult tests passed")
    
    def test_node_operation(self):
        """Test NodeOperation recording."""
        # Create operation
        op = NodeOperation(
            timestamp=datetime.now().isoformat(),
            nodeId="node_123",
            operation=NodeOperationType.GENERATE_SQL,
            data={
                "sql": "SELECT * FROM customers",
                "duration": 0.5
            }
        )
        
        # Test serialization
        op_dict = op.to_dict()
        assert op_dict["operation"] == "generate_sql"
        assert op_dict["data"]["sql"] == "SELECT * FROM customers"
        
        # Test deserialization
        op2 = NodeOperation.from_dict(op_dict)
        assert op2.operation == NodeOperationType.GENERATE_SQL
        
        print("✅ NodeOperation tests passed")
    
    def test_enum_conversions(self):
        """Test all enum conversions."""
        # TaskStatus
        assert TaskStatus.PROCESSING.value == "processing"
        assert TaskStatus("completed") == TaskStatus.COMPLETED
        
        # NodeStatus
        assert NodeStatus.SQL_GENERATED.value == "sql_generated"
        assert NodeStatus("executed_success") == NodeStatus.EXECUTED_SUCCESS
        
        # NodeOperationType
        assert NodeOperationType.CREATE.value == "create"
        assert NodeOperationType("execute") == NodeOperationType.EXECUTE
        
        # CombineStrategyType
        assert CombineStrategyType.JOIN.value == "join"
        assert CombineStrategyType("aggregate") == CombineStrategyType.AGGREGATE
        
        print("✅ Enum conversion tests passed")
    
    def test_complex_nested_structure(self):
        """Test complex nested data structure."""
        # Create a complex node with everything
        node = QueryNode(
            nodeId="complex_node",
            intent="Complex query with joins and aggregations",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="orders", alias="o", purpose="Main table"),
                    TableMapping(name="customers", alias="c", purpose="Customer info"),
                    TableMapping(name="products", alias="p", purpose="Product info")
                ],
                columns=[
                    ColumnMapping(table="customers", column="name", usedFor="select"),
                    ColumnMapping(table="products", column="category", usedFor="group"),
                    ColumnMapping(table="orders", column="total", usedFor="aggregate")
                ],
                joins=[
                    JoinMapping(from_table="orders", to="customers", 
                               on="orders.customer_id = customers.id"),
                    JoinMapping(from_table="orders", to="products",
                               on="orders.product_id = products.id")
                ]
            ),
            status=NodeStatus.EXECUTED_SUCCESS,
            sql="""
                SELECT c.name, p.category, SUM(o.total) as total_sales
                FROM orders o
                JOIN customers c ON o.customer_id = c.id
                JOIN products p ON o.product_id = p.id
                GROUP BY c.name, p.category
            """,
            executionResult=ExecutionResult(
                data=[
                    {"name": "John", "category": "Electronics", "total_sales": 1500},
                    {"name": "Jane", "category": "Books", "total_sales": 300}
                ],
                rowCount=2
            ),
            combineStrategy=CombineStrategy(
                type=CombineStrategyType.AGGREGATE,
                aggregateFunction="SUM",
                groupBy=["name", "category"]
            )
        )
        
        # Full serialization/deserialization test
        node_dict = node.to_dict()
        node_restored = QueryNode.from_dict(node_dict)
        
        assert node_restored.nodeId == node.nodeId
        assert len(node_restored.mapping.tables) == 3
        assert len(node_restored.mapping.joins) == 2
        assert node_restored.executionResult.rowCount == 2
        assert node_restored.combineStrategy.type == CombineStrategyType.AGGREGATE
        
        print("✅ Complex nested structure tests passed")


def run_all_tests():
    """Run all memory type tests."""
    print("="*60)
    print("LAYER 1: MEMORY TYPES TESTING")
    print("="*60)
    
    tester = TestMemoryTypes()
    
    tests = [
        tester.test_task_context,
        tester.test_column_info,
        tester.test_table_schema,
        tester.test_query_mapping,
        tester.test_combine_strategy,
        tester.test_query_node,
        tester.test_execution_result,
        tester.test_node_operation,
        tester.test_enum_conversions,
        tester.test_complex_nested_structure
    ]
    
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"❌ {test.__name__} failed: {str(e)}")
            raise
    
    print("\n✅ All Layer 1 tests passed!")


if __name__ == "__main__":
    run_all_tests()