"""
Test cases for various workflow scenarios in the text-to-SQL system.
Tests different query types, decomposition patterns, and real-world use cases.
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
    TaskContext, QueryNode, NodeStatus, QueryMapping,
    TableSchema, ColumnInfo, NodeOperation, NodeOperationType
)


class WorkflowTestScenarios:
    """Collection of test scenarios for the text-to-SQL workflow."""
    
    def __init__(self):
        self.memory = KeyValueMemory(name="test_scenarios")
        self.task_manager = TaskContextManager(memory=self.memory)
        self.query_manager = QueryTreeManager(memory=self.memory)
        self.schema_manager = DatabaseSchemaManager(memory=self.memory)
        self.history_manager = NodeHistoryManager(memory=self.memory)
    
    async def setup_sample_database(self):
        """Create sample database schema for testing."""
        # Create a sample financial database schema
        tables = []
        
        # Client table
        client_columns = [
            ColumnInfo(name="client_id", data_type="INTEGER", is_primary_key=True, is_foreign_key=False, 
                      nullable=False, description="Unique client identifier", sample_values=[1, 2, 3, 4, 5]),
            ColumnInfo(name="name", data_type="VARCHAR", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Client name", sample_values=["John Doe", "Jane Smith"]),
            ColumnInfo(name="client_type", data_type="VARCHAR", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Client type", sample_values=["VIP", "Regular", "Premium"]),
            ColumnInfo(name="created_date", data_type="DATE", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Account creation date", sample_values=["2020-01-01", "2020-02-15"])
        ]
        
        client_table = TableSchema(
            database_name="financial",
            table_name="client",
            columns=client_columns,
            primary_keys=["client_id"],
            foreign_keys=[],
            description="Client information table"
        )
        tables.append(client_table)
        
        # Account table
        account_columns = [
            ColumnInfo(name="account_id", data_type="INTEGER", is_primary_key=True, is_foreign_key=False,
                      nullable=False, description="Unique account identifier", sample_values=[201, 202, 203]),
            ColumnInfo(name="client_id", data_type="INTEGER", is_primary_key=False, is_foreign_key=True,
                      nullable=False, description="Client owner", sample_values=[1, 2, 3]),
            ColumnInfo(name="account_type", data_type="VARCHAR", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Account type", sample_values=["Checking", "Savings", "Investment"]),
            ColumnInfo(name="balance", data_type="DECIMAL", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Current balance", sample_values=[1000.00, 5000.00, 10000.00]),
            ColumnInfo(name="district_id", data_type="INTEGER", is_primary_key=False, is_foreign_key=True,
                      nullable=False, description="District location", sample_values=[1, 2, 3])
        ]
        
        account_table = TableSchema(
            database_name="financial",
            table_name="account",
            columns=account_columns,
            primary_keys=["account_id"],
            foreign_keys=[["client_id", "client", "client_id"], ["district_id", "district", "district_id"]],
            description="Account information table"
        )
        tables.append(account_table)
        
        # Transaction table
        trans_columns = [
            ColumnInfo(name="trans_id", data_type="INTEGER", is_primary_key=True, is_foreign_key=False,
                      nullable=False, description="Transaction ID", sample_values=[5001, 5002, 5003]),
            ColumnInfo(name="account_id", data_type="INTEGER", is_primary_key=False, is_foreign_key=True,
                      nullable=False, description="Account ID", sample_values=[201, 202, 203]),
            ColumnInfo(name="date", data_type="DATE", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Transaction date", sample_values=["2020-03-15", "2020-06-20"]),
            ColumnInfo(name="amount", data_type="DECIMAL", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Transaction amount", sample_values=[100.00, 1500.00, 2000.00]),
            ColumnInfo(name="type", data_type="VARCHAR", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Transaction type", sample_values=["DEBIT", "CREDIT", "TRANSFER"])
        ]
        
        trans_table = TableSchema(
            database_name="financial",
            table_name="trans",
            columns=trans_columns,
            primary_keys=["trans_id"],
            foreign_keys=[["account_id", "account", "account_id"]],
            description="Transaction records"
        )
        tables.append(trans_table)
        
        # District table
        district_columns = [
            ColumnInfo(name="district_id", data_type="INTEGER", is_primary_key=True, is_foreign_key=False,
                      nullable=False, description="District ID", sample_values=[1, 2, 3]),
            ColumnInfo(name="name", data_type="VARCHAR", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="District name", sample_values=["Praha", "Brno", "Ostrava"])
        ]
        
        district_table = TableSchema(
            database_name="financial",
            table_name="district",
            columns=district_columns,
            primary_keys=["district_id"],
            foreign_keys=[],
            description="District information"
        )
        tables.append(district_table)
        
        # Loan table
        loan_columns = [
            ColumnInfo(name="loan_id", data_type="INTEGER", is_primary_key=True, is_foreign_key=False,
                      nullable=False, description="Loan ID", sample_values=[7001, 7002, 7003]),
            ColumnInfo(name="account_id", data_type="INTEGER", is_primary_key=False, is_foreign_key=True,
                      nullable=False, description="Account ID", sample_values=[201, 202, 203]),
            ColumnInfo(name="amount", data_type="DECIMAL", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Loan amount", sample_values=[50000.00, 100000.00, 150000.00]),
            ColumnInfo(name="status", data_type="VARCHAR", is_primary_key=False, is_foreign_key=False,
                      nullable=False, description="Loan status", sample_values=["A", "B", "C", "D"])
        ]
        
        loan_table = TableSchema(
            database_name="financial",
            table_name="loan",
            columns=loan_columns,
            primary_keys=["loan_id"],
            foreign_keys=[["account_id", "account", "account_id"]],
            description="Loan information"
        )
        tables.append(loan_table)
        
        # Store schemas
        for table in tables:
            await self.schema_manager.add_table(table)
        
        return tables

    async def scenario_1_simple_aggregation(self):
        """Test Case 1: Simple aggregation query - COUNT with GROUP BY"""
        print("\n=== Scenario 1: Simple Aggregation Query ===")
        
        # Setup database
        await self.setup_sample_database()
        
        # Create task context
        task_id = "task_001"
        task = TaskContext(
            task_id=task_id,
            original_question="How many accounts does each client have?",
            database_name="financial",
            selected_tables=["account", "client"],
            current_sql="",
            execution_result={},
            is_complete=False
        )
        await self.task_manager.initialize(task)
        
        # Create single node query (no decomposition needed)
        root_node = QueryNode(
            node_id="node_001",
            task_id=task_id,
            question="How many accounts does each client have?",
            sql="""SELECT c.client_id, COUNT(a.account_id) as account_count
                   FROM client c
                   LEFT JOIN account a ON c.client_id = a.client_id
                   GROUP BY c.client_id""",
            status=NodeStatus.SQL_GENERATED,
            parent_id=None,
            child_ids=[],
            dependencies=[],
            execution_result={
                "columns": ["client_id", "account_count"],
                "rows": [[1, 3], [2, 2], [3, 1]],
                "row_count": 3
            }
        )
        
        await self.query_manager.add_node(root_node)
        await self.query_manager.set_root_node_id(task_id, "node_001")
        
        # Add to history
        operation = NodeOperation(
            operation_id="op_001",
            node_id="node_001",
            operation_type=NodeOperationType.SQL_GENERATION,
            timestamp=datetime.now().isoformat(),
            details={"sql": root_node.sql}
        )
        await self.history_manager.add_operation(operation)
        
        # Verify
        stored_task = await self.task_manager.get_context()
        stored_tree = await self.query_manager.get_all_nodes(task_id)
        
        print(f"Task stored: {stored_task.task_id}")
        print(f"Query nodes: {len(stored_tree)}")
        print(f"SQL: {stored_tree[0].sql[:50]}...")
        
        return True

    async def scenario_2_complex_joins(self):
        """Test Case 2: Complex multi-table joins with filtering"""
        print("\n=== Scenario 2: Complex Multi-table Joins ===")
        
        # Setup database
        await self.setup_sample_database()
        
        # Create task context
        task_id = "task_002"
        task = TaskContext(
            task_id=task_id,
            original_question="Find all transactions over $1000 for VIP clients in 2020",
            database_name="financial",
            selected_tables=["client", "account", "trans"],
            current_sql="",
            execution_result={},
            is_complete=False
        )
        await self.task_manager.initialize(task)
        
        # Create decomposed query tree
        # Root node
        root_node = QueryNode(
            node_id="node_002_root",
            task_id=task_id,
            question="Find all transactions over $1000 for VIP clients in 2020",
            sql="",
            status=NodeStatus.PENDING,
            parent_id=None,
            child_ids=["node_002_1", "node_002_2"],
            dependencies=[]
        )
        
        # Sub-question 1: Find VIP clients
        node1 = QueryNode(
            node_id="node_002_1",
            task_id=task_id,
            question="Which clients are VIP clients?",
            sql="""SELECT client_id 
                   FROM client 
                   WHERE client_type = 'VIP'""",
            status=NodeStatus.SQL_GENERATED,
            parent_id="node_002_root",
            child_ids=[],
            dependencies=[],
            execution_result={
                "columns": ["client_id"],
                "rows": [[101], [105], [108]],
                "row_count": 3
            }
        )
        
        # Sub-question 2: Find transactions for these clients
        node2 = QueryNode(
            node_id="node_002_2",
            task_id=task_id,
            question="What are the transactions over $1000 for these VIP clients in 2020?",
            sql="""WITH vip_clients AS (
                       SELECT client_id 
                       FROM client 
                       WHERE client_type = 'VIP'
                   )
                   SELECT t.trans_id, t.account_id, t.date, t.amount, c.client_id
                   FROM trans t
                   JOIN account a ON t.account_id = a.account_id
                   JOIN vip_clients c ON a.client_id = c.client_id
                   WHERE t.amount > 1000 
                   AND YEAR(t.date) = 2020""",
            status=NodeStatus.SQL_GENERATED,
            parent_id="node_002_root",
            child_ids=[],
            dependencies=["node_002_1"],
            execution_result={
                "columns": ["trans_id", "account_id", "date", "amount", "client_id"],
                "rows": [
                    [5001, 201, "2020-03-15", 1500.00, 101],
                    [5002, 205, "2020-06-20", 2000.00, 105]
                ],
                "row_count": 2
            }
        )
        
        # Add nodes
        await self.query_manager.add_node(root_node)
        await self.query_manager.add_node(node1)
        await self.query_manager.add_node(node2)
        await self.query_manager.set_root_node_id(task_id, "node_002_root")
        
        # Add history
        for node in [node1, node2]:
            operation = NodeOperation(
                operation_id=f"op_{node.node_id}",
                node_id=node.node_id,
                operation_type=NodeOperationType.SQL_GENERATION,
                timestamp=datetime.now().isoformat(),
                details={"sql": node.sql}
            )
            await self.history_manager.add_operation(operation)
        
        # Verify
        tree = await self.query_manager.get_subtree("node_002_root")
        print(f"Query tree size: {len(tree)}")
        print(f"Root has {len(root_node.child_ids)} children")
        print(f"Node 2 depends on: {node2.dependencies}")
        
        return True

    async def scenario_3_nested_subqueries(self):
        """Test Case 3: Nested subqueries with window functions"""
        print("\n=== Scenario 3: Nested Subqueries with Window Functions ===")
        
        await self.setup_sample_database()
        
        task_id = "task_003"
        task = TaskContext(
            task_id=task_id,
            original_question="Find the top 3 clients by transaction volume for each month",
            database_name="financial",
            selected_tables=["client", "account", "trans"],
            current_sql="",
            execution_result={},
            is_complete=False
        )
        await self.task_manager.initialize(task)
        
        # Complex query with window functions
        root_node = QueryNode(
            node_id="node_003",
            task_id=task_id,
            question="Find the top 3 clients by transaction volume for each month",
            sql="""WITH monthly_volumes AS (
                       SELECT 
                           c.client_id,
                           DATE_FORMAT(t.date, '%Y-%m') as month,
                           SUM(t.amount) as total_volume,
                           COUNT(t.trans_id) as trans_count
                       FROM trans t
                       JOIN account a ON t.account_id = a.account_id
                       JOIN client c ON a.client_id = c.client_id
                       GROUP BY c.client_id, DATE_FORMAT(t.date, '%Y-%m')
                   ),
                   ranked_clients AS (
                       SELECT 
                           client_id,
                           month,
                           total_volume,
                           trans_count,
                           ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_volume DESC) as rank
                       FROM monthly_volumes
                   )
                   SELECT * FROM ranked_clients WHERE rank <= 3""",
            status=NodeStatus.SQL_GENERATED,
            parent_id=None,
            child_ids=[],
            dependencies=[],
            execution_result={
                "columns": ["client_id", "month", "total_volume", "trans_count", "rank"],
                "rows": [
                    [101, "2020-01", 50000.00, 15, 1],
                    [105, "2020-01", 45000.00, 12, 2],
                    [103, "2020-01", 40000.00, 10, 3]
                ],
                "row_count": 36  # 3 per month for 12 months
            }
        )
        
        await self.query_manager.add_node(root_node)
        await self.query_manager.set_root_node_id(task_id, "node_003")
        
        print(f"Created complex query with window functions")
        print(f"SQL length: {len(root_node.sql)} characters")
        
        return True

    async def scenario_4_schema_refinement(self):
        """Test Case 4: Schema selection and refinement workflow"""
        print("\n=== Scenario 4: Schema Selection and Refinement ===")
        
        await self.setup_sample_database()
        
        task_id = "task_004"
        task = TaskContext(
            task_id=task_id,
            original_question="What is the average loan amount by district?",
            database_name="financial",
            selected_tables=["loan"],  # Initially only loan table
            current_sql="",
            execution_result={},
            is_complete=False
        )
        await self.task_manager.initialize(task)
        
        # Step 1: Initial query with limited schema
        node1 = QueryNode(
            node_id="node_004_1",
            task_id=task_id,
            question="What is the average loan amount by district?",
            sql="SELECT district_id, AVG(amount) as avg_loan FROM loan GROUP BY district_id",
            status=NodeStatus.EXECUTION_FAILED,
            parent_id=None,
            child_ids=[],
            dependencies=[],
            error_message="Column 'district_id' not found in loan table"
        )
        
        await self.query_manager.add_node(node1)
        
        # Step 2: Schema refinement - add more tables
        task.selected_tables = ["loan", "account", "district"]
        await self.task_manager.update(task)
        
        # Step 3: Refined query with proper joins
        node2 = QueryNode(
            node_id="node_004_2",
            task_id=task_id,
            question="What is the average loan amount by district? (refined)",
            sql="""SELECT d.district_id, d.name as district_name, AVG(l.amount) as avg_loan
                   FROM loan l
                   JOIN account a ON l.account_id = a.account_id
                   JOIN district d ON a.district_id = d.district_id
                   GROUP BY d.district_id, d.name""",
            status=NodeStatus.EXECUTION_SUCCESS,
            parent_id=None,
            child_ids=[],
            dependencies=[],
            execution_result={
                "columns": ["district_id", "district_name", "avg_loan"],
                "rows": [[1, "Praha", 125000.00], [2, "Brno", 98000.00]],
                "row_count": 10
            }
        )
        
        await self.query_manager.add_node(node2)
        
        # Add refinement mapping
        mapping = QueryMapping(
            source_node_id="node_004_1",
            target_node_id="node_004_2",
            mapping_type="schema_refinement",
            confidence_score=0.95
        )
        await self.query_manager.add_mapping(task_id, mapping)
        
        print(f"Initial query failed: {node1.error_message}")
        print(f"Refined with tables: {task.selected_tables}")
        print(f"Refinement successful: {node2.status}")
        
        return True

    async def scenario_5_multi_step_analysis(self):
        """Test Case 5: Multi-step analytical query with intermediate results"""
        print("\n=== Scenario 5: Multi-step Analytical Query ===")
        
        await self.setup_sample_database()
        
        task_id = "task_005"
        task = TaskContext(
            task_id=task_id,
            original_question="Which districts have loan default rates above average?",
            database_name="financial",
            selected_tables=["loan", "account", "district"],
            current_sql="",
            execution_result={},
            is_complete=False
        )
        await self.task_manager.initialize(task)
        
        # Root analytical question
        root = QueryNode(
            node_id="node_005_root",
            task_id=task_id,
            question="Which districts have loan default rates above average?",
            sql="",
            status=NodeStatus.PENDING,
            parent_id=None,
            child_ids=["node_005_1", "node_005_2", "node_005_3"],
            dependencies=[]
        )
        
        # Step 1: Calculate default rate by district
        node1 = QueryNode(
            node_id="node_005_1",
            task_id=task_id,
            question="What is the loan default rate for each district?",
            sql="""SELECT 
                       d.district_id,
                       COUNT(l.loan_id) as total_loans,
                       SUM(CASE WHEN l.status = 'D' THEN 1 ELSE 0 END) as defaulted_loans,
                       CAST(SUM(CASE WHEN l.status = 'D' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(l.loan_id) as default_rate
                   FROM loan l
                   JOIN account a ON l.account_id = a.account_id
                   JOIN district d ON a.district_id = d.district_id
                   GROUP BY d.district_id""",
            status=NodeStatus.EXECUTION_SUCCESS,
            parent_id="node_005_root",
            child_ids=[],
            dependencies=[],
            execution_result={
                "columns": ["district_id", "total_loans", "defaulted_loans", "default_rate"],
                "rows": [[1, 100, 15, 0.15], [2, 80, 8, 0.10], [3, 120, 30, 0.25]],
                "row_count": 10
            }
        )
        
        # Step 2: Calculate average default rate
        node2 = QueryNode(
            node_id="node_005_2",
            task_id=task_id,
            question="What is the average default rate across all districts?",
            sql="""SELECT AVG(default_rate) as avg_default_rate
                   FROM (
                       SELECT 
                           d.district_id,
                           CAST(SUM(CASE WHEN l.status = 'D' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(l.loan_id) as default_rate
                       FROM loan l
                       JOIN account a ON l.account_id = a.account_id
                       JOIN district d ON a.district_id = d.district_id
                       GROUP BY d.district_id
                   ) district_rates""",
            status=NodeStatus.EXECUTION_SUCCESS,
            parent_id="node_005_root",
            child_ids=[],
            dependencies=["node_005_1"],
            execution_result={
                "columns": ["avg_default_rate"],
                "rows": [[0.166]],
                "row_count": 1
            }
        )
        
        # Step 3: Find districts above average
        node3 = QueryNode(
            node_id="node_005_3",
            task_id=task_id,
            question="Which districts have default rates above the average?",
            sql="""WITH district_rates AS (
                       SELECT 
                           d.district_id,
                           d.name as district_name,
                           CAST(SUM(CASE WHEN l.status = 'D' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(l.loan_id) as default_rate
                       FROM loan l
                       JOIN account a ON l.account_id = a.account_id
                       JOIN district d ON a.district_id = d.district_id
                       GROUP BY d.district_id, d.name
                   ),
                   avg_rate AS (
                       SELECT AVG(default_rate) as avg_default_rate
                       FROM district_rates
                   )
                   SELECT dr.district_id, dr.district_name, dr.default_rate, ar.avg_default_rate
                   FROM district_rates dr
                   CROSS JOIN avg_rate ar
                   WHERE dr.default_rate > ar.avg_default_rate
                   ORDER BY dr.default_rate DESC""",
            status=NodeStatus.EXECUTION_SUCCESS,
            parent_id="node_005_root",
            child_ids=[],
            dependencies=["node_005_1", "node_005_2"],
            execution_result={
                "columns": ["district_id", "district_name", "default_rate", "avg_default_rate"],
                "rows": [[3, "District C", 0.25, 0.166], [5, "District E", 0.20, 0.166]],
                "row_count": 2
            }
        )
        
        # Add all nodes
        for node in [root, node1, node2, node3]:
            await self.query_manager.add_node(node)
        await self.query_manager.set_root_node_id(task_id, "node_005_root")
        
        print(f"Created {len(root.child_ids)}-step analysis")
        print(f"Final result: {node3.execution_result['row_count']} districts above average")
        
        return True

    async def scenario_6_iterative_refinement(self):
        """Test Case 6: Iterative query refinement based on user feedback"""
        print("\n=== Scenario 6: Iterative Query Refinement ===")
        
        await self.setup_sample_database()
        
        task_id = "task_006"
        task = TaskContext(
            task_id=task_id,
            original_question="Show me client activity",
            database_name="financial",
            selected_tables=["client", "trans"],
            current_sql="",
            execution_result={},
            is_complete=False
        )
        await self.task_manager.initialize(task)
        
        # Iteration 1: Too broad
        node1 = QueryNode(
            node_id="node_006_1",
            task_id=task_id,
            question="Show me client activity",
            sql="SELECT * FROM client c JOIN trans t ON c.client_id = t.client_id",
            status=NodeStatus.USER_FEEDBACK,
            parent_id=None,
            child_ids=[],
            dependencies=[],
            feedback="Too much data. Need summary by client."
        )
        
        # Iteration 2: Add aggregation
        node2 = QueryNode(
            node_id="node_006_2",
            task_id=task_id,
            question="Show client activity summary",
            sql="""SELECT c.client_id, COUNT(t.trans_id) as transaction_count
                   FROM client c 
                   LEFT JOIN account a ON c.client_id = a.client_id
                   LEFT JOIN trans t ON a.account_id = t.account_id
                   GROUP BY c.client_id""",
            status=NodeStatus.USER_FEEDBACK,
            parent_id=None,
            child_ids=[],
            dependencies=[],
            feedback="Also need total amount and date range"
        )
        
        # Iteration 3: Final version
        node3 = QueryNode(
            node_id="node_006_3",
            task_id=task_id,
            question="Show comprehensive client activity summary",
            sql="""SELECT 
                       c.client_id,
                       COUNT(t.trans_id) as transaction_count,
                       SUM(t.amount) as total_amount,
                       MIN(t.date) as first_transaction,
                       MAX(t.date) as last_transaction,
                       DATEDIFF(MAX(t.date), MIN(t.date)) as active_days
                   FROM client c 
                   LEFT JOIN account a ON c.client_id = a.client_id
                   LEFT JOIN trans t ON a.account_id = t.account_id
                   GROUP BY c.client_id
                   HAVING transaction_count > 0
                   ORDER BY total_amount DESC""",
            status=NodeStatus.EXECUTION_SUCCESS,
            parent_id=None,
            child_ids=[],
            dependencies=[],
            execution_result={
                "columns": ["client_id", "transaction_count", "total_amount", "first_transaction", "last_transaction", "active_days"],
                "rows": [[101, 156, 1250000.00, "2019-01-05", "2021-12-20", 1080]],
                "row_count": 50
            }
        )
        
        # Add nodes and mappings
        for node in [node1, node2, node3]:
            await self.query_manager.add_node(node)
        
        # Add refinement mappings
        mapping1 = QueryMapping(
            source_node_id="node_006_1",
            target_node_id="node_006_2",
            mapping_type="user_refinement",
            confidence_score=0.8
        )
        mapping2 = QueryMapping(
            source_node_id="node_006_2",
            target_node_id="node_006_3",
            mapping_type="user_refinement",
            confidence_score=0.95
        )
        
        await self.query_manager.add_mapping(task_id, mapping1)
        await self.query_manager.add_mapping(task_id, mapping2)
        
        print(f"Refinement iterations: 3")
        print(f"Final query satisfied user requirements")
        
        return True

    async def export_memory_state(self):
        """Export all memory content to JSON file."""
        # Collect all memory data
        memory_data = {}
        
        # Get all memory items
        items = await self.memory.list()
        for item_id in items:
            item = await self.memory.get_with_details(item_id)
            if item:
                memory_data[item_id] = {
                    "content": item.content,
                    "mime_type": item.mime_type.value,
                    "metadata": item.metadata
                }
        
        return memory_data

    async def run_all_scenarios(self):
        """Run all test scenarios and generate summary."""
        scenarios = [
            self.scenario_1_simple_aggregation,
            self.scenario_2_complex_joins,
            self.scenario_3_nested_subqueries,
            self.scenario_4_schema_refinement,
            self.scenario_5_multi_step_analysis,
            self.scenario_6_iterative_refinement
        ]
        
        results = []
        for i, scenario in enumerate(scenarios, 1):
            try:
                success = await scenario()
                results.append((i, scenario.__name__, success))
            except Exception as e:
                print(f"Error in scenario {i}: {str(e)}")
                import traceback
                traceback.print_exc()
                results.append((i, scenario.__name__, False))
        
        # Export memory state
        memory_state = await self.export_memory_state()
        output_file = Path("test_scenarios_output.json")
        with open(output_file, 'w') as f:
            json.dump(memory_state, f, indent=2)
        
        # Summary
        print("\n=== Test Summary ===")
        for num, name, success in results:
            status = "✓" if success else "✗"
            print(f"{status} Scenario {num}: {name.replace('_', ' ').title()}")
        
        # Memory statistics
        print(f"\nMemory entries: {len(memory_state)}")
        print(f"Output saved to: {output_file}")
        
        return all(r[2] for r in results)


async def main():
    """Run all workflow test scenarios."""
    tester = WorkflowTestScenarios()
    success = await tester.run_all_scenarios()
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)