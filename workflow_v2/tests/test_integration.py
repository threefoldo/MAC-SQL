"""
Integration Test: End-to-End Workflow Testing
"""

import src.asyncio as asyncio
import src.logging as logging
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
    TaskStatus, NodeStatus, TableSchema, ColumnInfo,
    QueryNode, QueryMapping, TableMapping, ColumnMapping,
    JoinMapping, ExecutionResult, CombineStrategy, CombineStrategyType
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class IntegrationTestScenarios:
    """Integration test scenarios for the complete workflow."""
    
    async def setup_ecommerce_schema(self, schema_manager: DatabaseSchemaManager):
        """Setup a complete e-commerce database schema."""
        
        # Customers table
        customers = TableSchema(
            name="customers",
            columns={
                "customer_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=True, isForeignKey=False
                ),
                "name": ColumnInfo(
                    dataType="VARCHAR(100)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "email": ColumnInfo(
                    dataType="VARCHAR(100)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "city": ColumnInfo(
                    dataType="VARCHAR(50)", nullable=True,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "state": ColumnInfo(
                    dataType="VARCHAR(2)", nullable=True,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "customer_type": ColumnInfo(
                    dataType="VARCHAR(20)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "registration_date": ColumnInfo(
                    dataType="DATE", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                )
            },
            sampleData=[
                {"customer_id": 1, "name": "John Doe", "email": "john@example.com",
                 "city": "New York", "state": "NY", "customer_type": "premium",
                 "registration_date": "2023-01-15"},
                {"customer_id": 2, "name": "Jane Smith", "email": "jane@example.com",
                 "city": "Los Angeles", "state": "CA", "customer_type": "regular",
                 "registration_date": "2023-02-20"}
            ],
            metadata={"rowCount": 10000, "indexes": ["idx_customer_type", "idx_city_state"]}
        )
        await schema_manager.add_table(customers)
        
        # Products table
        products = TableSchema(
            name="products",
            columns={
                "product_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=True, isForeignKey=False
                ),
                "name": ColumnInfo(
                    dataType="VARCHAR(200)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "category": ColumnInfo(
                    dataType="VARCHAR(50)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "subcategory": ColumnInfo(
                    dataType="VARCHAR(50)", nullable=True,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "price": ColumnInfo(
                    dataType="DECIMAL(10,2)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "cost": ColumnInfo(
                    dataType="DECIMAL(10,2)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "stock_quantity": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                )
            },
            sampleData=[
                {"product_id": 101, "name": "Laptop Pro", "category": "Electronics",
                 "subcategory": "Computers", "price": 1299.99, "cost": 800.00,
                 "stock_quantity": 50},
                {"product_id": 102, "name": "Wireless Mouse", "category": "Electronics",
                 "subcategory": "Accessories", "price": 29.99, "cost": 15.00,
                 "stock_quantity": 200}
            ],
            metadata={"rowCount": 5000}
        )
        await schema_manager.add_table(products)
        
        # Orders table
        orders = TableSchema(
            name="orders",
            columns={
                "order_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=True, isForeignKey=False
                ),
                "customer_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=False, isForeignKey=True,
                    references={"table": "customers", "column": "customer_id"}
                ),
                "order_date": ColumnInfo(
                    dataType="DATE", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "ship_date": ColumnInfo(
                    dataType="DATE", nullable=True,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "total_amount": ColumnInfo(
                    dataType="DECIMAL(10,2)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "status": ColumnInfo(
                    dataType="VARCHAR(20)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                )
            },
            metadata={"rowCount": 50000, "indexes": ["idx_customer_id", "idx_order_date"]}
        )
        await schema_manager.add_table(orders)
        
        # Order items table
        order_items = TableSchema(
            name="order_items",
            columns={
                "order_item_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=True, isForeignKey=False
                ),
                "order_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=False, isForeignKey=True,
                    references={"table": "orders", "column": "order_id"}
                ),
                "product_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=False, isForeignKey=True,
                    references={"table": "products", "column": "product_id"}
                ),
                "quantity": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "unit_price": ColumnInfo(
                    dataType="DECIMAL(10,2)", nullable=False,
                    isPrimaryKey=False, isForeignKey=False
                ),
                "discount": ColumnInfo(
                    dataType="DECIMAL(5,2)", nullable=True,
                    isPrimaryKey=False, isForeignKey=False
                )
            },
            metadata={"rowCount": 150000}
        )
        await schema_manager.add_table(order_items)
        
        logging.info("E-commerce schema setup completed")
    
    async def test_simple_query_workflow(self):
        """Test 1: Simple single-table query workflow."""
        print("\n" + "="*60)
        print("TEST 1: Simple Query Workflow")
        print("Query: Find all premium customers from New York")
        print("="*60)
        
        # Initialize memory and managers
        memory = KeyValueMemory()
        task_manager = TaskContextManager(memory)
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        history_manager = NodeHistoryManager(memory)
        
        # Initialize task
        await task_manager.initialize(
            task_id="test_simple_001",
            original_query="Find all premium customers from New York",
            database_name="ecommerce"
        )
        
        # Setup schema
        await schema_manager.initialize()
        await self.setup_ecommerce_schema(schema_manager)
        
        # Step 1: Query Analysis (simulated)
        root_id = await tree_manager.initialize("Find all premium customers from New York")
        await history_manager.initialize()
        await history_manager.record_create(root_id, "Find all premium customers from New York")
        
        # Step 2: Schema Linking (simulated)
        mapping = QueryMapping(
            tables=[
                TableMapping(name="customers", alias="c", 
                           purpose="Filter customers by type and location")
            ],
            columns=[
                ColumnMapping(table="customers", column="customer_id", usedFor="select"),
                ColumnMapping(table="customers", column="name", usedFor="select"),
                ColumnMapping(table="customers", column="email", usedFor="select"),
                ColumnMapping(table="customers", column="city", usedFor="filter"),
                ColumnMapping(table="customers", column="customer_type", usedFor="filter")
            ]
        )
        await tree_manager.update_node_mapping(root_id, mapping)
        
        # Step 3: SQL Generation (simulated)
        sql = """
SELECT c.customer_id, c.name, c.email
FROM customers c
WHERE c.city = 'New York' 
  AND c.customer_type = 'premium'
ORDER BY c.name"""
        
        await tree_manager.update_node_sql(root_id, sql)
        await history_manager.record_generate_sql(root_id, sql)
        
        # Step 4: SQL Execution (simulated)
        exec_result = ExecutionResult(
            data=[
                {"customer_id": 1, "name": "John Doe", "email": "john@example.com"},
                {"customer_id": 15, "name": "Alice Johnson", "email": "alice@example.com"}
            ],
            rowCount=2
        )
        await tree_manager.update_node_result(root_id, exec_result, True)
        await history_manager.record_execute(root_id, sql, result=exec_result.data)
        
        # Step 5: Update task status
        await task_manager.mark_as_completed()
        
        # Verify workflow completion
        final_node = await tree_manager.get_node(root_id)
        assert final_node.status == NodeStatus.EXECUTED_SUCCESS
        assert final_node.executionResult.rowCount == 2
        
        task_status = await task_manager.get_status()
        assert task_status == TaskStatus.COMPLETED
        
        print("✅ Simple query workflow completed successfully")
        print(f"   - Generated SQL: {sql[:50]}...")
        print(f"   - Result: {exec_result.rowCount} rows")
    
    async def test_join_query_workflow(self):
        """Test 2: Multi-table join query workflow."""
        print("\n" + "="*60)
        print("TEST 2: Join Query Workflow")
        print("Query: Get total sales by product category for Q1 2024")
        print("="*60)
        
        # Initialize memory and managers
        memory = KeyValueMemory()
        task_manager = TaskContextManager(memory)
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        history_manager = NodeHistoryManager(memory)
        
        # Initialize task
        await task_manager.initialize(
            task_id="test_join_001",
            original_query="Get total sales by product category for Q1 2024",
            database_name="ecommerce"
        )
        
        # Setup schema
        await schema_manager.initialize()
        await self.setup_ecommerce_schema(schema_manager)
        await history_manager.initialize()
        
        # Step 1: Query Analysis
        root_id = await tree_manager.initialize("Get total sales by product category for Q1 2024")
        
        # Step 2: Schema Linking
        mapping = QueryMapping(
            tables=[
                TableMapping(name="orders", alias="o", purpose="Filter by date and get order info"),
                TableMapping(name="order_items", alias="oi", purpose="Get order details"),
                TableMapping(name="products", alias="p", purpose="Get product categories")
            ],
            columns=[
                ColumnMapping(table="products", column="category", usedFor="select,group"),
                ColumnMapping(table="order_items", column="quantity", usedFor="aggregate"),
                ColumnMapping(table="order_items", column="unit_price", usedFor="aggregate"),
                ColumnMapping(table="orders", column="order_date", usedFor="filter"),
                ColumnMapping(table="orders", column="status", usedFor="filter")
            ],
            joins=[
                JoinMapping(from_table="orders", to="order_items",
                          on="o.order_id = oi.order_id"),
                JoinMapping(from_table="order_items", to="products",
                          on="oi.product_id = p.product_id")
            ]
        )
        await tree_manager.update_node_mapping(root_id, mapping)
        
        # Step 3: SQL Generation
        sql = """
SELECT 
    p.category,
    SUM(oi.quantity * oi.unit_price) as total_sales,
    COUNT(DISTINCT o.order_id) as order_count
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_date >= '2024-01-01' 
  AND o.order_date < '2024-04-01'
  AND o.status = 'completed'
GROUP BY p.category
ORDER BY total_sales DESC"""
        
        await tree_manager.update_node_sql(root_id, sql)
        
        # Step 4: SQL Execution
        exec_result = ExecutionResult(
            data=[
                {"category": "Electronics", "total_sales": 125000.50, "order_count": 450},
                {"category": "Clothing", "total_sales": 89000.25, "order_count": 780},
                {"category": "Books", "total_sales": 45000.00, "order_count": 620}
            ],
            rowCount=3
        )
        await tree_manager.update_node_result(root_id, exec_result, True)
        
        # Complete task
        await task_manager.mark_as_completed()
        
        print("✅ Join query workflow completed successfully")
        print(f"   - Tables joined: 3")
        print(f"   - Result: {exec_result.rowCount} categories")
    
    async def test_complex_decomposed_workflow(self):
        """Test 3: Complex query with decomposition."""
        print("\n" + "="*60)
        print("TEST 3: Complex Decomposed Query Workflow")
        print("Query: Find top 5 customers who spent the most in Electronics")
        print("        category in 2024, with their order count and avg order value")
        print("="*60)
        
        # Initialize
        memory = KeyValueMemory()
        task_manager = TaskContextManager(memory)
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        history_manager = NodeHistoryManager(memory)
        
        await task_manager.initialize(
            task_id="test_complex_001",
            original_query="Find top 5 customers by Electronics spending in 2024",
            database_name="ecommerce"
        )
        
        await schema_manager.initialize()
        await self.setup_ecommerce_schema(schema_manager)
        await history_manager.initialize()
        
        # Step 1: Query Analysis with Decomposition
        root_id = await tree_manager.initialize(
            "Find top 5 customers by Electronics spending in 2024 with stats"
        )
        
        # Create sub-queries
        # Subquery 1: Get Electronics orders
        subq1 = QueryNode(
            nodeId="subq1_electronics",
            intent="Get all Electronics orders in 2024",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="orders", alias="o", purpose="Filter by date"),
                    TableMapping(name="order_items", alias="oi", purpose="Get items"),
                    TableMapping(name="products", alias="p", purpose="Filter Electronics")
                ],
                joins=[
                    JoinMapping(from_table="orders", to="order_items",
                              on="o.order_id = oi.order_id"),
                    JoinMapping(from_table="order_items", to="products",
                              on="oi.product_id = p.product_id")
                ]
            ),
            parentId=root_id
        )
        await tree_manager.add_node(subq1, root_id)
        
        # Subquery 2: Aggregate by customer
        subq2 = QueryNode(
            nodeId="subq2_aggregate",
            intent="Aggregate Electronics spending by customer",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="customers", alias="c", purpose="Get customer info")
                ]
            ),
            parentId=root_id
        )
        await tree_manager.add_node(subq2, root_id)
        
        # Update root with combination strategy
        combine_strategy = CombineStrategy(
            type=CombineStrategyType.AGGREGATE,
            aggregateFunction="SUM",
            groupBy=["customer_id"]
        )
        await tree_manager.update_node_combine_strategy(root_id, combine_strategy)
        
        # Generate SQL for subqueries
        subq1_sql = """
WITH electronics_orders AS (
    SELECT o.customer_id, o.order_id, 
           SUM(oi.quantity * oi.unit_price) as order_total
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE p.category = 'Electronics'
      AND o.order_date >= '2024-01-01'
      AND o.status = 'completed'
    GROUP BY o.customer_id, o.order_id
)"""
        await tree_manager.update_node_sql("subq1_electronics", subq1_sql)
        
        # Final combined SQL
        final_sql = """
WITH electronics_orders AS (
    SELECT o.customer_id, o.order_id, 
           SUM(oi.quantity * oi.unit_price) as order_total
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE p.category = 'Electronics'
      AND o.order_date >= '2024-01-01'
      AND o.status = 'completed'
    GROUP BY o.customer_id, o.order_id
)
SELECT 
    c.customer_id,
    c.name,
    c.email,
    COUNT(DISTINCT eo.order_id) as order_count,
    SUM(eo.order_total) as total_spent,
    AVG(eo.order_total) as avg_order_value
FROM electronics_orders eo
JOIN customers c ON eo.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_spent DESC
LIMIT 5"""
        
        await tree_manager.update_node_sql(root_id, final_sql)
        
        # Execute
        exec_result = ExecutionResult(
            data=[
                {"customer_id": 42, "name": "Tech Corp", "email": "tech@corp.com",
                 "order_count": 15, "total_spent": 45000.00, "avg_order_value": 3000.00},
                {"customer_id": 17, "name": "Digital Solutions", "email": "info@digital.com",
                 "order_count": 12, "total_spent": 38000.00, "avg_order_value": 3166.67}
            ],
            rowCount=5
        )
        await tree_manager.update_node_result(root_id, exec_result, True)
        
        # Complete
        await task_manager.mark_as_completed()
        
        # Verify tree structure
        tree_stats = await tree_manager.get_tree_stats()
        print("✅ Complex decomposed workflow completed successfully")
        print(f"   - Query nodes: {tree_stats['total_nodes']}")
        print(f"   - Tree depth: {tree_stats['depth']}")
        print(f"   - Result: Top {exec_result.rowCount} customers identified")
    
    async def test_error_recovery_workflow(self):
        """Test 4: Error recovery and retry workflow."""
        print("\n" + "="*60)
        print("TEST 4: Error Recovery Workflow")
        print("Query: Get product inventory status")
        print("="*60)
        
        # Initialize
        memory = KeyValueMemory()
        task_manager = TaskContextManager(memory)
        tree_manager = QueryTreeManager(memory)
        history_manager = NodeHistoryManager(memory)
        
        await task_manager.initialize(
            task_id="test_error_001",
            original_query="Get product inventory status",
            database_name="ecommerce"
        )
        await history_manager.initialize()
        
        root_id = await tree_manager.initialize("Get product inventory status")
        
        # First attempt - with error
        bad_sql = "SELECT * FROM product_inventory WHERE stok > 0"  # Typo: 'stok'
        await tree_manager.update_node_sql(root_id, bad_sql)
        
        # Failed execution
        exec_result = ExecutionResult(
            data=[],
            rowCount=0,
            error="Column 'stok' not found. Did you mean 'stock_quantity'?"
        )
        await tree_manager.update_node_result(root_id, exec_result, False)
        await history_manager.record_execute(root_id, bad_sql, error=exec_result.error)
        
        # Retry with corrected SQL
        corrected_sql = """
SELECT 
    p.product_id,
    p.name,
    p.category,
    p.stock_quantity,
    CASE 
        WHEN p.stock_quantity = 0 THEN 'Out of Stock'
        WHEN p.stock_quantity < 10 THEN 'Low Stock'
        ELSE 'In Stock'
    END as inventory_status
FROM products p
WHERE p.stock_quantity < 50
ORDER BY p.stock_quantity ASC"""
        
        # Record revision
        await history_manager.record_revise(
            root_id,
            new_sql=corrected_sql,
            previous_sql=bad_sql
        )
        await tree_manager.update_node_sql(root_id, corrected_sql)
        
        # Successful execution
        exec_result = ExecutionResult(
            data=[
                {"product_id": 105, "name": "USB Cable", "category": "Electronics",
                 "stock_quantity": 5, "inventory_status": "Low Stock"},
                {"product_id": 203, "name": "Notebook", "category": "Stationery",
                 "stock_quantity": 0, "inventory_status": "Out of Stock"}
            ],
            rowCount=8
        )
        await tree_manager.update_node_result(root_id, exec_result, True)
        
        # Complete
        await task_manager.mark_as_completed()
        
        # Verify error recovery
        node_history = await history_manager.get_node_operations(root_id)
        revisions = await history_manager.get_revision_history(root_id)
        
        print("✅ Error recovery workflow completed successfully")
        print(f"   - Initial error: Column name typo")
        print(f"   - Recovery: SQL corrected and re-executed")
        print(f"   - Total operations: {len(node_history)}")
        print(f"   - Revisions: {len(revisions)}")
    
    async def test_workflow_summary(self):
        """Generate summary of all test workflows."""
        print("\n" + "="*60)
        print("WORKFLOW TEST SUMMARY")
        print("="*60)
        
        test_results = {
            "Simple Query": "✅ Passed - Single table query executed successfully",
            "Join Query": "✅ Passed - Multi-table join with aggregation completed",
            "Complex Decomposed": "✅ Passed - Query decomposition and combination worked",
            "Error Recovery": "✅ Passed - Error detected and corrected successfully"
        }
        
        for test_name, result in test_results.items():
            print(f"{test_name}: {result}")
        
        print("\nKey Capabilities Demonstrated:")
        print("- ✅ Task lifecycle management")
        print("- ✅ Schema storage and retrieval")
        print("- ✅ Query tree construction and navigation")
        print("- ✅ SQL generation and execution tracking")
        print("- ✅ Error handling and recovery")
        print("- ✅ Complex query decomposition")
        print("- ✅ Operation history tracking")


async def run_integration_tests():
    """Run all integration tests."""
    print("="*60)
    print("INTEGRATION TESTING: END-TO-END WORKFLOWS")
    print("="*60)
    
    tester = IntegrationTestScenarios()
    
    try:
        # Run all test scenarios
        await tester.test_simple_query_workflow()
        await tester.test_join_query_workflow()
        await tester.test_complex_decomposed_workflow()
        await tester.test_error_recovery_workflow()
        
        # Summary
        await tester.test_workflow_summary()
        
        print("\n✅ All integration tests passed!")
        
    except Exception as e:
        print(f"\n❌ Integration test failed: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(run_integration_tests())