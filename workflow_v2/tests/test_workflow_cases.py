"""
Additional test cases for the text-to-SQL workflow system.
Tests various real-world scenarios with the actual memory types.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path


# Import setup for tests
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from memory import KeyValueMemory
from task_context_manager import TaskContextManager
from query_tree_manager import QueryTreeManager
from database_schema_manager import DatabaseSchemaManager
from node_history_manager import NodeHistoryManager
from memory_types import (
    TaskContext, TaskStatus, QueryNode, NodeStatus, 
    QueryMapping, TableMapping, ColumnMapping, CombineStrategy, CombineStrategyType,
    ExecutionResult, NodeOperation, NodeOperationType
)


class WorkflowTestCases:
    """Test cases for various workflow scenarios."""
    
    def __init__(self):
        self.memory = KeyValueMemory(name="test_cases")
        self.task_manager = TaskContextManager(memory=self.memory)
        self.query_manager = QueryTreeManager(memory=self.memory)
        self.schema_manager = DatabaseSchemaManager(memory=self.memory)
        self.history_manager = NodeHistoryManager(memory=self.memory)
    
    async def case_1_single_table_aggregation(self):
        """Test Case 1: Simple single-table aggregation query"""
        print("\n=== Case 1: Single Table Aggregation ===")
        
        # Create task
        task_id = "task_agg_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="What is the total amount of all transactions?",
            database_name="financial"
        )
        
        # Initialize query tree
        await self.query_manager.initialize("What is the total amount of all transactions?")
        
        # Create query node - no decomposition needed
        node = QueryNode(
            nodeId="node_agg_001",
            intent="Calculate total transaction amount",
            mapping=QueryMapping(
                tables=[TableMapping(
                    name="transactions",
                    alias="t",
                    purpose="Get transaction amounts"
                )],
                columns=[ColumnMapping(
                    table="transactions",
                    column="amount",
                    usedFor="aggregation"
                )]
            ),
            sql="SELECT SUM(amount) as total_amount FROM transactions",
            status=NodeStatus.SQL_GENERATED,
            executionResult=ExecutionResult(
                data=[["total_amount"], [1500000.50]],
                rowCount=1
            )
        )
        
        await self.query_manager.add_node(node)
        
        # Add operation history
        operation = NodeOperation(
            
            nodeId="node_agg_001",
            operation=NodeOperationType.GENERATE_SQL,
            timestamp=datetime.now().isoformat(),
            data={"sql": node.sql}
        )
        await self.history_manager.add_operation(operation)
        
        print(f"✓ Created aggregation query")
        print(f"  Result: {node.executionResult.data[1][0]}")
        
        return True

    async def case_2_join_with_filter(self):
        """Test Case 2: Join query with filtering conditions"""
        print("\n=== Case 2: Join Query with Filters ===")
        
        # Create task
        task_id = "task_join_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Show all orders from premium customers in 2023",
            database_name="ecommerce"
        )
        
        # Initialize query tree
        await self.query_manager.initialize("Show all orders from premium customers in 2023")
        
        # Create query node
        node = QueryNode(
            nodeId="node_join_001",
            intent="Get orders from premium customers in 2023",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="customers", alias="c", purpose="Filter premium customers"),
                    TableMapping(name="orders", alias="o", purpose="Get order details")
                ],
                columns=[
                    ColumnMapping(table="customers", column="customer_type", usedFor="filter"),
                    ColumnMapping(table="orders", column="order_date", usedFor="filter"),
                    ColumnMapping(table="orders", column="*", usedFor="select")
                ]
            ),
            sql="""SELECT o.* 
                   FROM orders o 
                   JOIN customers c ON o.customer_id = c.customer_id 
                   WHERE c.customer_type = 'Premium' 
                   AND YEAR(o.order_date) = 2023""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=[
                    [1001, 101, "2023-01-15", 250.00],
                    [1002, 105, "2023-02-20", 350.00]
                ],
                rowCount=2
            )
        )
        
        await self.query_manager.add_node(node)
        
        print(f"✓ Created join query with filters")
        print(f"  Found {node.executionResult.rowCount} orders")
        
        return True

    async def case_3_decomposed_analytical_query(self):
        """Test Case 3: Decomposed multi-step analytical query"""
        print("\n=== Case 3: Decomposed Analytical Query ===")
        
        # Create task
        task_id = "task_decomp_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Which product categories have above-average sales?",
            database_name="retail"
        )
        
        # Initialize query tree
        await self.query_manager.initialize("Which product categories have above-average sales?")
        
        # Root node - the main question
        root = QueryNode(
            nodeId="node_decomp_root",
            intent="Find product categories with above-average sales",
            mapping=QueryMapping(tables=[], columns=[]),
            childIds=["node_decomp_001", "node_decomp_002", "node_decomp_003"],
            status=NodeStatus.CREATED,
            combineStrategy=CombineStrategy(
                type=CombineStrategyType.CUSTOM,
                template="Calculate average first, then compare"
            )
        )
        
        # Child 1: Calculate sales by category
        child1 = QueryNode(
            nodeId="node_decomp_001",
            intent="Calculate total sales for each category",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="products", alias="p", purpose="Get category"),
                    TableMapping(name="sales", alias="s", purpose="Get sales amount")
                ],
                columns=[
                    ColumnMapping(table="products", column="category", usedFor="grouping"),
                    ColumnMapping(table="sales", column="amount", usedFor="aggregation")
                ]
            ),
            parentId="node_decomp_root",
            sql="""SELECT p.category, SUM(s.amount) as total_sales
                   FROM sales s
                   JOIN products p ON s.product_id = p.product_id
                   GROUP BY p.category""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=[
                    ["Electronics", 50000.00],
                    ["Clothing", 30000.00],
                    ["Food", 25000.00]
                ],
                rowCount=3
            )
        )
        
        # Child 2: Calculate average sales
        child2 = QueryNode(
            nodeId="node_decomp_002",
            intent="Calculate average sales across all categories",
            mapping=QueryMapping(
                tables=[TableMapping(name="sub_query", alias="sq", purpose="From previous result")],
                columns=[ColumnMapping(table="sub_query", column="total_sales", usedFor="aggregation")]
            ),
            parentId="node_decomp_root",
            sql="""SELECT AVG(total_sales) as avg_sales
                   FROM (
                       SELECT p.category, SUM(s.amount) as total_sales
                       FROM sales s
                       JOIN products p ON s.product_id = p.product_id
                       GROUP BY p.category
                   ) category_sales""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=[[35000.00]],
                rowCount=1
            )
        )
        
        # Child 3: Find above-average categories
        child3 = QueryNode(
            nodeId="node_decomp_003",
            intent="Identify categories with sales above average",
            mapping=QueryMapping(
                tables=[TableMapping(name="combined", alias="c", purpose="Final comparison")],
                columns=[ColumnMapping(table="combined", column="*", usedFor="select")]
            ),
            parentId="node_decomp_root",
            sql="""WITH category_sales AS (
                       SELECT p.category, SUM(s.amount) as total_sales
                       FROM sales s
                       JOIN products p ON s.product_id = p.product_id
                       GROUP BY p.category
                   )
                   SELECT cs.category, cs.total_sales
                   FROM category_sales cs
                   WHERE cs.total_sales > (SELECT AVG(total_sales) FROM category_sales)
                   ORDER BY cs.total_sales DESC""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=[["Electronics", 50000.00]],
                rowCount=1
            )
        )
        
        # Add all nodes
        await self.query_manager.add_node(root)
        await self.query_manager.add_node(child1)
        await self.query_manager.add_node(child2)
        await self.query_manager.add_node(child3)
        
        print(f"✓ Created decomposed query with {len(root.childIds)} sub-queries")
        print(f"  Above-average categories: {child3.executionResult.rowCount}")
        
        return True

    async def case_4_iterative_refinement(self):
        """Test Case 4: Query refinement based on user feedback"""
        print("\n=== Case 4: Iterative Query Refinement ===")
        
        # Create task
        task_id = "task_refine_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Show customer information",
            database_name="crm"
        )
        
        # Initialize query tree
        await self.query_manager.initialize("Show customer information")
        
        # Iteration 1: Too broad
        node1 = QueryNode(
            nodeId="node_refine_001",
            intent="Show all customer information",
            mapping=QueryMapping(
                tables=[TableMapping(name="customers", alias="c", purpose="Get all data")],
                columns=[ColumnMapping(table="customers", column="*", usedFor="select")]
            ),
            sql="SELECT * FROM customers",
            status=NodeStatus.REVISED,
            executionResult=ExecutionResult(
                data=[],
                rowCount=10000,
                error="Too many results. Please be more specific."
            )
        )
        
        # Iteration 2: Add filters
        node2 = QueryNode(
            nodeId="node_refine_002",
            intent="Show active premium customers only",
            mapping=QueryMapping(
                tables=[TableMapping(name="customers", alias="c", purpose="Get filtered data")],
                columns=[
                    ColumnMapping(table="customers", column="status", usedFor="filter"),
                    ColumnMapping(table="customers", column="type", usedFor="filter")
                ]
            ),
            sql="""SELECT customer_id, name, email, last_purchase_date
                   FROM customers 
                   WHERE status = 'Active' 
                   AND type = 'Premium'
                   ORDER BY last_purchase_date DESC""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=[
                    [201, "John Doe", "john@email.com", "2023-12-15"],
                    [205, "Jane Smith", "jane@email.com", "2023-12-10"]
                ],
                rowCount=25
            )
        )
        
        # Add nodes
        await self.query_manager.add_node(node1)
        await self.query_manager.add_node(node2)
        
        # Track refinement history
        for node in [node1, node2]:
            operation = NodeOperation(
                timestamp=datetime.now().isoformat(),
                nodeId=node.nodeId,
                operation=NodeOperationType.GENERATE_SQL,
                data={"iteration": int(node.nodeId[-1])}
            )
            await self.history_manager.add_operation(operation)
        
        print(f"✓ Refined query through 2 iterations")
        print(f"  Final result: {node2.executionResult.rowCount} customers")
        
        return True

    async def case_5_complex_window_functions(self):
        """Test Case 5: Complex query with window functions"""
        print("\n=== Case 5: Window Functions Query ===")
        
        # Create task
        task_id = "task_window_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Rank products by sales within each category",
            database_name="sales_analytics"
        )
        
        # Initialize query tree
        await self.query_manager.initialize("Rank products by sales within each category")
        
        # Create complex window function query
        node = QueryNode(
            nodeId="node_window_001",
            intent="Rank products by sales within categories",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="products", alias="p", purpose="Product info"),
                    TableMapping(name="sales", alias="s", purpose="Sales data")
                ],
                columns=[
                    ColumnMapping(table="products", column="category", usedFor="partition"),
                    ColumnMapping(table="sales", column="revenue", usedFor="ranking")
                ]
            ),
            sql="""WITH product_sales AS (
                       SELECT 
                           p.product_id,
                           p.product_name,
                           p.category,
                           SUM(s.revenue) as total_revenue,
                           COUNT(s.sale_id) as sale_count
                       FROM products p
                       JOIN sales s ON p.product_id = s.product_id
                       GROUP BY p.product_id, p.product_name, p.category
                   )
                   SELECT 
                       product_id,
                       product_name,
                       category,
                       total_revenue,
                       sale_count,
                       RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) as category_rank,
                       PERCENT_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) as percentile
                   FROM product_sales
                   WHERE total_revenue > 0
                   ORDER BY category, category_rank""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=[
                    [101, "Laptop Pro", "Electronics", 150000.00, 150, 1, 0.0],
                    [102, "Smartphone X", "Electronics", 120000.00, 200, 2, 0.33],
                    [201, "Winter Jacket", "Clothing", 50000.00, 100, 1, 0.0]
                ],
                rowCount=50
            )
        )
        
        await self.query_manager.add_node(node)
        
        print(f"✓ Created window function query")
        print(f"  Ranked {node.executionResult.rowCount} products")
        
        return True

    async def case_6_multi_cte_analysis(self):
        """Test Case 6: Multi-CTE analytical query"""
        print("\n=== Case 6: Multi-CTE Analysis ===")
        
        # Create task
        task_id = "task_cte_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Analyze customer churn patterns by segment",
            database_name="customer_analytics"
        )
        
        # Initialize query tree
        await self.query_manager.initialize("Analyze customer churn patterns by segment")
        
        # Create multi-CTE query
        node = QueryNode(
            nodeId="node_cte_001",
            intent="Analyze churn patterns across customer segments",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="customers", alias="c", purpose="Customer data"),
                    TableMapping(name="transactions", alias="t", purpose="Activity tracking"),
                    TableMapping(name="segments", alias="s", purpose="Segmentation")
                ],
                columns=[
                    ColumnMapping(table="segments", column="segment_name", usedFor="grouping"),
                    ColumnMapping(table="customers", column="churn_flag", usedFor="calculation")
                ]
            ),
            sql="""WITH customer_activity AS (
                       SELECT 
                           c.customer_id,
                           c.segment_id,
                           c.join_date,
                           c.churn_date,
                           COUNT(t.transaction_id) as total_transactions,
                           MAX(t.transaction_date) as last_transaction_date,
                           SUM(t.amount) as lifetime_value
                       FROM customers c
                       LEFT JOIN transactions t ON c.customer_id = t.customer_id
                       GROUP BY c.customer_id, c.segment_id, c.join_date, c.churn_date
                   ),
                   segment_metrics AS (
                       SELECT 
                           s.segment_name,
                           COUNT(ca.customer_id) as total_customers,
                           COUNT(ca.churn_date) as churned_customers,
                           AVG(ca.lifetime_value) as avg_lifetime_value,
                           AVG(ca.total_transactions) as avg_transactions
                       FROM customer_activity ca
                       JOIN segments s ON ca.segment_id = s.segment_id
                       GROUP BY s.segment_name
                   ),
                   churn_analysis AS (
                       SELECT 
                           segment_name,
                           total_customers,
                           churned_customers,
                           CAST(churned_customers AS FLOAT) / total_customers as churn_rate,
                           avg_lifetime_value,
                           avg_transactions,
                           CASE 
                               WHEN CAST(churned_customers AS FLOAT) / total_customers > 0.3 THEN 'High Risk'
                               WHEN CAST(churned_customers AS FLOAT) / total_customers > 0.15 THEN 'Medium Risk'
                               ELSE 'Low Risk'
                           END as risk_category
                       FROM segment_metrics
                   )
                   SELECT * FROM churn_analysis
                   ORDER BY churn_rate DESC""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=[
                    ["Basic", 5000, 2000, 0.40, 500.00, 10, "High Risk"],
                    ["Premium", 3000, 300, 0.10, 5000.00, 50, "Low Risk"],
                    ["VIP", 1000, 50, 0.05, 15000.00, 100, "Low Risk"]
                ],
                rowCount=5
            )
        )
        
        await self.query_manager.add_node(node)
        
        print(f"✓ Created multi-CTE analysis query")
        print(f"  Analyzed {node.executionResult.rowCount} customer segments")
        
        return True

    async def run_all_cases(self):
        """Run all test cases."""
        cases = [
            self.case_1_single_table_aggregation,
            self.case_2_join_with_filter,
            self.case_3_decomposed_analytical_query,
            self.case_4_iterative_refinement,
            self.case_5_complex_window_functions,
            self.case_6_multi_cte_analysis
        ]
        
        results = []
        for i, case in enumerate(cases, 1):
            try:
                success = await case()
                results.append((i, case.__name__, success))
            except Exception as e:
                print(f"Error in case {i}: {str(e)}")
                import traceback
                traceback.print_exc()
                results.append((i, case.__name__, False))
        
        # Summary
        print("\n=== Test Summary ===")
        for num, name, success in results:
            status = "✓" if success else "✗"
            print(f"{status} Case {num}: {name.replace('case_', '').replace('_', ' ').title()}")
        
        # Export memory state
        print("\n=== Memory State ===")
        # Get task count
        task_ids = []
        for result in results:
            if result[2]:  # If successful
                case_num = result[0]
                task_ids.extend([
                    "task_agg_001", "task_join_001", "task_decomp_001",
                    "task_refine_001", "task_window_001", "task_cte_001"
                ][:case_num])
        
        print(f"Total tasks created: {len(set(task_ids))}")
        
        # Count nodes
        total_nodes = 0
        for task_id in set(task_ids):
            try:
                tree = await self.query_manager.get_tree()
                if tree and "nodes" in tree:
                    total_nodes += len(tree["nodes"])
            except:
                pass
        
        print(f"Total query nodes: {total_nodes}")
        
        return all(r[2] for r in results)


async def main():
    """Run all test cases."""
    tester = WorkflowTestCases()
    success = await tester.run_all_cases()
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)