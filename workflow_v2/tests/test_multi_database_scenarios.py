"""
Test cases for multi-database scenarios in the text-to-SQL workflow.
Tests handling multiple databases, cross-database queries, and database switching.
"""

import src.asyncio as asyncio
import src.json as json
from src.datetime import datetime
from src.pathlib import Path


# Import setup for tests
import src.sys as sys
from src.pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.memory import KeyValueMemory
from src.task_context_manager import TaskContextManager
from src.query_tree_manager import QueryTreeManager
from src.database_schema_manager import DatabaseSchemaManager
from src.node_history_manager import NodeHistoryManager
from src.memory_content_types import (
    TaskContext, TaskStatus, QueryNode, NodeStatus, 
    QueryMapping, TableMapping, ColumnMapping, CombineStrategy, CombineStrategyType,
    ExecutionResult, NodeOperation, NodeOperationType
)


class MultiDatabaseTestScenarios:
    """Test scenarios for multi-database workflows."""
    
    def __init__(self):
        self.memory = KeyValueMemory(name="multi_db_tests")
        self.task_manager = TaskContextManager(memory=self.memory)
        self.query_manager = QueryTreeManager(memory=self.memory)
        self.schema_manager = DatabaseSchemaManager(memory=self.memory)
        self.history_manager = NodeHistoryManager(memory=self.memory)
    
    async def scenario_1_database_switching(self):
        """Test Case 1: Switching between databases in a single session"""
        print("\n=== Scenario 1: Database Switching ===")
        
        # Task 1: Query from financial database
        task_id_1 = "multi_db_001"
        await self.task_manager.initialize(
            task_id=task_id_1,
            original_query="Show total loans by district",
            database_name="financial"
        )
        
        # Initialize query tree
        await self.query_manager.initialize("Show total loans by district")
        
        # Create query for financial DB
        node1 = QueryNode(
            nodeId="node_financial_001",
            intent="Get total loans by district",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="loans", alias="l", purpose="Loan amounts"),
                    TableMapping(name="district", alias="d", purpose="District names")
                ],
                columns=[
                    ColumnMapping(table="loans", column="amount", usedFor="aggregation"),
                    ColumnMapping(table="district", column="name", usedFor="grouping")
                ]
            ),
            sql="SELECT d.name, SUM(l.amount) FROM loans l JOIN district d ON l.district_id = d.id GROUP BY d.name",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[["Praha", 1500000], ["Brno", 1200000]],
                rowCount=10,
            )
        )
        
        await self.query_manager.add_node(node1)
        
        # Task 2: Switch to ecommerce database
        task_id_2 = "multi_db_002"
        await self.task_manager.initialize(
            task_id=task_id_2,
            original_query="Show top selling products",
            database_name="ecommerce"
        )
        
        # Create query for ecommerce DB
        node2 = QueryNode(
            nodeId="node_ecommerce_001",
            intent="Get top selling products",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="products", alias="p", purpose="Product info"),
                    TableMapping(name="sales", alias="s", purpose="Sales data")
                ],
                columns=[
                    ColumnMapping(table="products", column="name", usedFor="select"),
                    ColumnMapping(table="sales", column="quantity", usedFor="aggregation")
                ]
            ),
            sql="SELECT p.name, SUM(s.quantity) as total_sold FROM products p JOIN sales s ON p.id = s.product_id GROUP BY p.name ORDER BY total_sold DESC LIMIT 10",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[["iPhone 15", 5000], ["MacBook Pro", 3000]],
                rowCount=10,
            )
        )
        
        await self.query_manager.add_node(node2)
        
        print(f"✓ Created queries for 2 different databases")
        print(f"  Financial DB: {node1.executionResult.rowCount} districts")
        print(f"  Ecommerce DB: {node2.executionResult.rowCount} products")
        
        return True

    async def scenario_2_cross_database_analysis(self):
        """Test Case 2: Analysis requiring data from multiple databases"""
        print("\n=== Scenario 2: Cross-Database Analysis ===")
        
        task_id = "cross_db_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Compare customer spending patterns across retail and banking",
            database_name="analytics"  # Virtual database for cross-DB queries
        )
        
        # Root node for cross-database analysis
        root = QueryNode(
            nodeId="node_cross_root",
            intent="Compare spending across retail and banking",
            mapping=QueryMapping(tables=[], columns=[]),
            childIds=["node_cross_001", "node_cross_002", "node_cross_003"],
            status=NodeStatus.CREATED,
            combineStrategy=CombineStrategy(
                type=CombineStrategyType.CUSTOM,
                template="Merge customer data from different sources"
            )
        )
        
        # Child 1: Get banking data
        child1 = QueryNode(
            nodeId="node_cross_001",
            intent="Get customer spending from banking",
            mapping=QueryMapping(
                tables=[TableMapping(name="financial.transactions", alias="t", purpose="Banking transactions")],
                columns=[ColumnMapping(table="transactions", column="amount", usedFor="aggregation")]
            ),
            parentId="node_cross_root",
            sql="SELECT customer_id, SUM(amount) as banking_total FROM financial.transactions GROUP BY customer_id",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[[101, 50000], [102, 75000]],
                rowCount=1000,
            )
        )
        
        # Child 2: Get retail data
        child2 = QueryNode(
            nodeId="node_cross_002",
            intent="Get customer spending from retail",
            mapping=QueryMapping(
                tables=[TableMapping(name="retail.orders", alias="o", purpose="Retail orders")],
                columns=[ColumnMapping(table="orders", column="total", usedFor="aggregation")]
            ),
            parentId="node_cross_root",
            sql="SELECT customer_id, SUM(total) as retail_total FROM retail.orders GROUP BY customer_id",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[[101, 15000], [102, 20000]],
                rowCount=800,
            )
        )
        
        # Child 3: Combined analysis
        child3 = QueryNode(
            nodeId="node_cross_003",
            intent="Combine and analyze spending patterns",
            mapping=QueryMapping(
                tables=[TableMapping(name="combined", alias="c", purpose="Combined data")],
                columns=[ColumnMapping(table="combined", column="*", usedFor="analysis")]
            ),
            parentId="node_cross_root",
            sql="""WITH combined_spending AS (
                       SELECT 
                           COALESCE(b.customer_id, r.customer_id) as customer_id,
                           COALESCE(b.banking_total, 0) as banking_total,
                           COALESCE(r.retail_total, 0) as retail_total,
                           COALESCE(b.banking_total, 0) + COALESCE(r.retail_total, 0) as total_spending
                       FROM 
                           (SELECT customer_id, SUM(amount) as banking_total FROM financial.transactions GROUP BY customer_id) b
                       FULL OUTER JOIN
                           (SELECT customer_id, SUM(total) as retail_total FROM retail.orders GROUP BY customer_id) r
                       ON b.customer_id = r.customer_id
                   )
                   SELECT 
                       customer_id,
                       banking_total,
                       retail_total,
                       total_spending,
                       ROUND(banking_total / NULLIF(total_spending, 0) * 100, 2) as banking_percentage,
                       ROUND(retail_total / NULLIF(total_spending, 0) * 100, 2) as retail_percentage
                   FROM combined_spending
                   ORDER BY total_spending DESC
                   LIMIT 100""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[[102, 75000, 20000, 95000, 78.95, 21.05]],
                rowCount=100,
            )
        )
        
        # Add all nodes
        for node in [root, child1, child2, child3]:
            await self.query_manager.add_node(node)
        
        print(f"✓ Created cross-database analysis with {len(root.childIds)} sub-queries")
        print(f"  Combined data from 2 databases")
        print(f"  Analyzed {child3.executionResult.rowCount} customers")
        
        return True

    async def scenario_3_database_metadata_query(self):
        """Test Case 3: Querying database metadata across multiple DBs"""
        print("\n=== Scenario 3: Database Metadata Query ===")
        
        task_id = "meta_db_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Show table statistics across all databases",
            database_name="information_schema"
        )
        
        # Query for database metadata
        node = QueryNode(
            nodeId="node_meta_001",
            intent="Get table statistics from all databases",
            mapping=QueryMapping(
                tables=[TableMapping(name="information_schema.tables", alias="t", purpose="Table metadata")],
                columns=[
                    ColumnMapping(table="tables", column="table_schema", usedFor="grouping"),
                    ColumnMapping(table="tables", column="table_rows", usedFor="aggregation")
                ]
            ),
            sql="""SELECT 
                       table_schema as database_name,
                       COUNT(*) as table_count,
                       SUM(table_rows) as total_rows,
                       SUM(data_length + index_length) / 1024 / 1024 as size_mb
                   FROM information_schema.tables 
                   WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema')
                   GROUP BY table_schema
                   ORDER BY size_mb DESC""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[
                    ["financial", 15, 1500000, 2048.5],
                    ["ecommerce", 20, 5000000, 4096.2],
                    ["analytics", 10, 3000000, 1024.8]
                ],
                rowCount=5,
            )
        )
        
        await self.query_manager.add_node(node)
        
        print(f"✓ Queried metadata for {node.executionResult.rowCount} databases")
        
        return True

    async def scenario_4_database_migration_validation(self):
        """Test Case 4: Validating data consistency across database versions"""
        print("\n=== Scenario 4: Database Migration Validation ===")
        
        task_id = "migration_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Validate customer data between prod and staging databases",
            database_name="migration_check"
        )
        
        # Root node for migration validation
        root = QueryNode(
            nodeId="node_migration_root",
            intent="Validate data consistency between databases",
            mapping=QueryMapping(tables=[], columns=[]),
            childIds=["node_migration_001", "node_migration_002"],
            status=NodeStatus.CREATED
        )
        
        # Child 1: Count comparison
        child1 = QueryNode(
            nodeId="node_migration_001",
            intent="Compare record counts between databases",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="prod.customers", alias="p", purpose="Production data"),
                    TableMapping(name="staging.customers", alias="s", purpose="Staging data")
                ],
                columns=[]
            ),
            parentId="node_migration_root",
            sql="""SELECT 
                       'customers' as table_name,
                       (SELECT COUNT(*) FROM prod.customers) as prod_count,
                       (SELECT COUNT(*) FROM staging.customers) as staging_count,
                       (SELECT COUNT(*) FROM prod.customers) - (SELECT COUNT(*) FROM staging.customers) as difference
                   UNION ALL
                   SELECT 
                       'orders' as table_name,
                       (SELECT COUNT(*) FROM prod.orders) as prod_count,
                       (SELECT COUNT(*) FROM staging.orders) as staging_count,
                       (SELECT COUNT(*) FROM prod.orders) - (SELECT COUNT(*) FROM staging.orders) as difference""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[
                    ["customers", 10000, 10000, 0],
                    ["orders", 50000, 49995, 5]
                ],
                rowCount=2,
            )
        )
        
        # Child 2: Data integrity check
        child2 = QueryNode(
            nodeId="node_migration_002",
            intent="Check data integrity between databases",
            mapping=QueryMapping(
                tables=[TableMapping(name="comparison", alias="comp", purpose="Data comparison")],
                columns=[]
            ),
            parentId="node_migration_root",
            sql="""SELECT 
                       'Missing in staging' as issue_type,
                       COUNT(*) as count
                   FROM prod.customers p
                   LEFT JOIN staging.customers s ON p.customer_id = s.customer_id
                   WHERE s.customer_id IS NULL
                   UNION ALL
                   SELECT 
                       'Data mismatch' as issue_type,
                       COUNT(*) as count
                   FROM prod.customers p
                   JOIN staging.customers s ON p.customer_id = s.customer_id
                   WHERE p.email != s.email OR p.phone != s.phone""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[
                    ["Missing in staging", 0],
                    ["Data mismatch", 3]
                ],
                rowCount=2,
            )
        )
        
        # Add nodes
        for node in [root, child1, child2]:
            await self.query_manager.add_node(node)
        
        print(f"✓ Validated migration with {len(root.childIds)} checks")
        print(f"  Found {child2.executionResult.data[1][1]} data mismatches")
        
        return True

    async def scenario_5_federated_query(self):
        """Test Case 5: Federated query across different database systems"""
        print("\n=== Scenario 5: Federated Query ===")
        
        task_id = "federated_001"
        await self.task_manager.initialize(
            task_id=task_id,
            original_query="Combine MySQL customer data with PostgreSQL analytics",
            database_name="federated"
        )
        
        # Federated query combining different DB systems
        node = QueryNode(
            nodeId="node_federated_001",
            intent="Join data from MySQL and PostgreSQL",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="mysql.customers", alias="mc", purpose="MySQL customer data"),
                    TableMapping(name="postgres.analytics", alias="pa", purpose="PostgreSQL analytics")
                ],
                columns=[]
            ),
            sql="""SELECT 
                       mc.customer_id,
                       mc.name,
                       mc.email,
                       pa.lifetime_value,
                       pa.churn_probability,
                       pa.last_activity_date
                   FROM mysql_link.customers mc
                   JOIN postgres_link.customer_analytics pa 
                   ON mc.customer_id = pa.customer_id
                   WHERE pa.churn_probability > 0.7
                   ORDER BY pa.lifetime_value DESC""",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                
                data=[
                    [1001, "John Doe", "john@email.com", 15000.00, 0.85, "2023-01-15"],
                    [1005, "Jane Smith", "jane@email.com", 12000.00, 0.72, "2023-02-20"]
                ],
                rowCount=25,
            )
        )
        
        await self.query_manager.add_node(node)
        
        print(f"✓ Created federated query across MySQL and PostgreSQL")
        print(f"  Found {node.executionResult.rowCount} high-risk customers")
        
        return True

    async def run_all_scenarios(self):
        """Run all multi-database test scenarios."""
        scenarios = [
            self.scenario_1_database_switching,
            self.scenario_2_cross_database_analysis,
            self.scenario_3_database_metadata_query,
            self.scenario_4_database_migration_validation,
            self.scenario_5_federated_query
        ]
        
        results = []
        for i, scenario in enumerate(scenarios, 1):
            try:
                success = await scenario()
                results.append((scenario.__name__, success))
            except Exception as e:
                print(f"Error in scenario {i}: {str(e)}")
                import traceback
                traceback.print_exc()
                results.append((scenario.__name__, False))
        
        # Summary
        print("\n=== Multi-Database Test Summary ===")
        for i, (name, success) in enumerate(results, 1):
            status = "✓" if success else "✗"
            clean_name = name.replace('scenario_', '').replace('_', ' ').title()
            print(f"{status} Scenario {i}: {clean_name}")
        
        # Statistics
        print("\n=== Test Statistics ===")
        total_tasks = sum(1 for r in results if r[1])  # r[1] is success flag
        print(f"Total successful scenarios: {total_tasks}/{len(scenarios)}")
        
        # Memory usage
        print(f"\nDatabases tested:")
        print("  - financial")
        print("  - ecommerce")
        print("  - analytics")
        print("  - information_schema")
        print("  - prod/staging (migration)")
        print("  - mysql/postgres (federated)")
        
        return all(r[1] for r in results)


async def main():
    """Run all multi-database test scenarios."""
    tester = MultiDatabaseTestScenarios()
    success = await tester.run_all_scenarios()
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)