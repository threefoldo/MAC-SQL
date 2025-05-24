"""
Test cases for agent structure and workflow components

Tests focus on the data structures and workflow rather than the agents themselves.
"""

import src.asyncio as asyncio
import src.pytest as pytest
from src.datetime import datetime
from src.pathlib import Path
import src.sys as sys
from src.typing import Dict, Any, List

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.memory import KeyValueMemory
from src.memory_content_types import (
    TaskContext, QueryNode, NodeStatus, TaskStatus,
    CombineStrategy, CombineStrategyType, QueryMapping,
    TableMapping, ColumnMapping, JoinMapping,
    TableSchema, ColumnInfo, ExecutionResult
)
from src.task_context_manager import TaskContextManager
from src.query_tree_manager import QueryTreeManager
from src.database_schema_manager import DatabaseSchemaManager
from src.node_history_manager import NodeHistoryManager


async def setup_test_schema(schema_manager: DatabaseSchemaManager):
    """Setup test schema for all tests."""
    
    # Employees table
    employees = TableSchema(
        name="employees",
        columns={
            "id": ColumnInfo(
                dataType="INTEGER", nullable=False,
                isPrimaryKey=True, isForeignKey=False
            ),
            "name": ColumnInfo(
                dataType="VARCHAR(100)", nullable=False,
                isPrimaryKey=False, isForeignKey=False
            ),
            "email": ColumnInfo(
                dataType="VARCHAR(100)", nullable=True,
                isPrimaryKey=False, isForeignKey=False
            ),
            "department_id": ColumnInfo(
                dataType="INTEGER", nullable=True,
                isPrimaryKey=False, isForeignKey=True,
                references={"table": "departments", "column": "id"}
            ),
            "manager_id": ColumnInfo(
                dataType="INTEGER", nullable=True,
                isPrimaryKey=False, isForeignKey=True,
                references={"table": "employees", "column": "id"}
            ),
            "salary": ColumnInfo(
                dataType="DECIMAL(10,2)", nullable=True,
                isPrimaryKey=False, isForeignKey=False
            ),
            "hire_date": ColumnInfo(
                dataType="DATE", nullable=True,
                isPrimaryKey=False, isForeignKey=False
            )
        }
    )
    await schema_manager.add_table(employees)
    
    # Departments table
    departments = TableSchema(
        name="departments",
        columns={
            "id": ColumnInfo(
                dataType="INTEGER", nullable=False,
                isPrimaryKey=True, isForeignKey=False
            ),
            "name": ColumnInfo(
                dataType="VARCHAR(100)", nullable=False,
                isPrimaryKey=False, isForeignKey=False
            ),
            "location": ColumnInfo(
                dataType="VARCHAR(100)", nullable=True,
                isPrimaryKey=False, isForeignKey=False
            )
        }
    )
    await schema_manager.add_table(departments)
    
    # Sales table for aggregate testing
    sales = TableSchema(
        name="sales",
        columns={
            "id": ColumnInfo(
                dataType="INTEGER", nullable=False,
                isPrimaryKey=True, isForeignKey=False
            ),
            "product_id": ColumnInfo(
                dataType="INTEGER", nullable=False,
                isPrimaryKey=False, isForeignKey=True,
                references={"table": "products", "column": "id"}
            ),
            "region_id": ColumnInfo(
                dataType="INTEGER", nullable=False,
                isPrimaryKey=False, isForeignKey=True,
                references={"table": "regions", "column": "id"}
            ),
            "amount": ColumnInfo(
                dataType="DECIMAL(10,2)", nullable=False,
                isPrimaryKey=False, isForeignKey=False
            ),
            "date": ColumnInfo(
                dataType="DATE", nullable=False,
                isPrimaryKey=False, isForeignKey=False
            )
        }
    )
    await schema_manager.add_table(sales)


class TestQueryAnalysisWorkflow:
    """Test query analysis workflow patterns"""
    
    @pytest.mark.asyncio
    async def test_simple_select_workflow(self):
        """Test workflow for simple SELECT query"""
        memory = KeyValueMemory()
        
        # Initialize managers
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_001", "Show all employee names", "test_db")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Show all employee names")
        
        # Simulate query analysis result
        mapping = QueryMapping(
            tables=[TableMapping(name="employees", purpose="Main data source")],
            columns=[ColumnMapping(table="employees", column="name", usedFor="select")]
        )
        
        await tree_manager.update_node(root_id, {
            "mapping": mapping.to_dict(),
            "sql": "SELECT name FROM employees"
        })
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.intent == "Show all employee names"
        assert len(node.mapping.tables) == 1
        assert node.sql is not None
    
    @pytest.mark.asyncio
    async def test_complex_decomposition_workflow(self):
        """Test decomposition of complex queries"""
        memory = KeyValueMemory()
        
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_002", "Top 5 departments by average salary", "test_db")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Find top 5 departments by average salary")
        
        # Simulate decomposition into sub-queries
        # Sub-query 1: Calculate average salary per department
        child1 = QueryNode(
            nodeId="sq_001",
            intent="Calculate average salary per department",
            mapping=QueryMapping(
                tables=[TableMapping(name="employees"), TableMapping(name="departments")],
                columns=[
                    ColumnMapping(table="departments", column="name", usedFor="groupBy"),
                    ColumnMapping(table="employees", column="salary", usedFor="aggregate")
                ]
            )
        )
        await tree_manager.add_node(child1, root_id)
        
        # Sub-query 2: Count employees per department
        child2 = QueryNode(
            nodeId="sq_002",
            intent="Count employees per department",
            mapping=QueryMapping(
                tables=[TableMapping(name="employees")],
                columns=[
                    ColumnMapping(table="employees", column="department_id", usedFor="groupBy")
                ]
            )
        )
        await tree_manager.add_node(child2, root_id)
        
        # Update root with combination strategy
        strategy = CombineStrategy(
            type=CombineStrategyType.JOIN,
            template="Join average salaries with employee counts and rank"
        )
        await tree_manager.update_node(root_id, {
            "combineStrategy": strategy.to_dict()
        })
        
        # Verify structure
        root = await tree_manager.get_node(root_id)
        assert len(root.childIds) == 2
        assert root.combineStrategy.type == CombineStrategyType.JOIN
        
        children = await tree_manager.get_children(root_id)
        assert len(children) == 2
    
    @pytest.mark.asyncio
    async def test_join_query_workflow(self):
        """Test workflow for JOIN queries"""
        memory = KeyValueMemory()
        
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_003", "Employees with department names", "test_db")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Show employees with their department names")
        
        # Create join mapping
        mapping = QueryMapping(
            tables=[
                TableMapping(name="employees", alias="e", purpose="Employee data"),
                TableMapping(name="departments", alias="d", purpose="Department names")
            ],
            columns=[
                ColumnMapping(table="employees", column="name", usedFor="select"),
                ColumnMapping(table="departments", column="name", usedFor="select")
            ],
            joins=[
                JoinMapping(
                    from_table="employees",
                    to="departments",
                    on="e.department_id = d.id"
                )
            ]
        )
        
        sql = """
        SELECT e.name AS employee_name, d.name AS department_name
        FROM employees e
        LEFT JOIN departments d ON e.department_id = d.id
        """
        
        await tree_manager.update_node(root_id, {
            "mapping": mapping.to_dict(),
            "sql": sql.strip()
        })
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert len(node.mapping.tables) == 2
        assert len(node.mapping.joins) == 1
        assert "JOIN" in node.sql
    
    @pytest.mark.asyncio
    async def test_aggregate_query_workflow(self):
        """Test workflow for aggregate queries"""
        memory = KeyValueMemory()
        
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_004", "Total sales by region", "test_db")
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await setup_test_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Calculate total sales by region for Q4 2023")
        
        # Create aggregate mapping
        mapping = QueryMapping(
            tables=[
                TableMapping(name="sales", alias="s"),
                TableMapping(name="regions", alias="r")  
            ],
            columns=[
                ColumnMapping(table="regions", column="name", usedFor="select"),
                ColumnMapping(table="sales", column="amount", usedFor="aggregate"),
                ColumnMapping(table="sales", column="date", usedFor="filter"),
                ColumnMapping(table="sales", column="region_id", usedFor="groupBy")
            ],
            joins=[
                JoinMapping(
                    from_table="sales",
                    to="regions",
                    on="s.region_id = r.id"
                )
            ]
        )
        
        sql = """
        SELECT r.name AS region, SUM(s.amount) AS total_sales
        FROM sales s
        JOIN regions r ON s.region_id = r.id
        WHERE s.date >= '2023-10-01' AND s.date <= '2023-12-31'
        GROUP BY r.id, r.name
        ORDER BY total_sales DESC
        """
        
        await tree_manager.update_node(root_id, {
            "mapping": mapping.to_dict(),
            "sql": sql.strip()
        })
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert "SUM" in node.sql
        assert "GROUP BY" in node.sql
        assert any(col.usedFor == "aggregate" for col in node.mapping.columns)
    
    @pytest.mark.asyncio
    async def test_subquery_workflow(self):
        """Test workflow with subqueries"""
        memory = KeyValueMemory()
        
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_005", "Above average earners", "test_db")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Find employees earning above department average")
        
        # This could be one complex query or decomposed
        # Option 1: Single complex query with subquery
        sql_with_subquery = """
        SELECT e.name, e.salary, e.department_id
        FROM employees e
        WHERE e.salary > (
            SELECT AVG(e2.salary)
            FROM employees e2
            WHERE e2.department_id = e.department_id
        )
        """
        
        # Option 2: Decomposed approach
        # Child: Calculate department averages
        child = QueryNode(
            nodeId="avg_calc",
            intent="Calculate average salary per department",
            mapping=QueryMapping(
                tables=[TableMapping(name="employees")],
                columns=[
                    ColumnMapping(table="employees", column="department_id", usedFor="groupBy"),
                    ColumnMapping(table="employees", column="salary", usedFor="aggregate")
                ]
            ),
            sql="SELECT department_id, AVG(salary) as avg_salary FROM employees GROUP BY department_id"
        )
        await tree_manager.add_node(child, root_id)
        
        # Root uses child results
        root_sql = """
        SELECT e.name, e.salary, e.department_id
        FROM employees e
        JOIN ({child_result}) avg ON e.department_id = avg.department_id
        WHERE e.salary > avg.avg_salary
        """
        
        await tree_manager.update_node(root_id, {"sql": root_sql})
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert "{child_result}" in node.sql or "SELECT AVG" in sql_with_subquery
    
    @pytest.mark.asyncio
    async def test_union_query_workflow(self):
        """Test workflow for UNION queries"""
        memory = KeyValueMemory()
        
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_006", "All managers", "test_db")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Find all managers (department heads and team leads)")
        
        # Create child queries for UNION
        child1 = QueryNode(
            nodeId="dept_heads",
            intent="Get department heads",
            mapping=QueryMapping(
                tables=[TableMapping(name="departments"), TableMapping(name="employees")],
                columns=[ColumnMapping(table="employees", column="name", usedFor="select")]
            ),
            sql="SELECT e.name, 'Department Head' as role FROM employees e JOIN departments d ON e.id = d.manager_id"
        )
        await tree_manager.add_node(child1, root_id)
        
        child2 = QueryNode(
            nodeId="team_leads",
            intent="Get team leads",
            mapping=QueryMapping(
                tables=[TableMapping(name="employees")],
                columns=[ColumnMapping(table="employees", column="name", usedFor="select")]
            ),
            sql="SELECT name, 'Team Lead' as role FROM employees WHERE is_team_lead = true"
        )
        await tree_manager.add_node(child2, root_id)
        
        # Set UNION strategy
        strategy = CombineStrategy(
            type=CombineStrategyType.UNION,
            unionType="UNION",
            template="Combine all manager types"
        )
        
        root_sql = """
        {child_dept_heads}
        UNION
        {child_team_leads}
        ORDER BY name
        """
        
        await tree_manager.update_node(root_id, {
            "combineStrategy": strategy.to_dict(),
            "sql": root_sql
        })
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.combineStrategy.type == CombineStrategyType.UNION
        assert "UNION" in node.sql


class TestSchemaLinkingWorkflow:
    """Test schema linking workflow patterns"""
    
    @pytest.mark.asyncio
    async def test_direct_table_mapping(self):
        """Test direct table name matching"""
        memory = KeyValueMemory()
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await setup_test_schema(schema_manager)
        
        # Verify schema is loaded
        tables = await schema_manager.get_all_tables()
        table_names = list(tables.keys())
        assert "employees" in table_names
        assert "departments" in table_names
    
    @pytest.mark.asyncio
    async def test_foreign_key_detection(self):
        """Test foreign key relationship detection"""
        memory = KeyValueMemory()
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await setup_test_schema(schema_manager)
        
        # Get employees table
        emp_table = await schema_manager.get_table("employees")
        
        # Check foreign keys
        dept_id_col = emp_table.columns["department_id"]
        assert dept_id_col.isForeignKey
        assert dept_id_col.references["table"] == "departments"
        assert dept_id_col.references["column"] == "id"
        
        # Self-referencing foreign key
        mgr_id_col = emp_table.columns["manager_id"]
        assert mgr_id_col.isForeignKey
        assert mgr_id_col.references["table"] == "employees"
    
    @pytest.mark.asyncio
    async def test_multi_hop_join_path(self):
        """Test finding join paths through multiple tables"""
        memory = KeyValueMemory()
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        
        # Add more tables for complex joins
        products = TableSchema(
            name="products",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "name": ColumnInfo(dataType="VARCHAR(100)", nullable=False, isPrimaryKey=False, isForeignKey=False),
                "category_id": ColumnInfo(
                    dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=True,
                    references={"table": "categories", "column": "id"}
                )
            }
        )
        await schema_manager.add_table(products)
        
        orders = TableSchema(
            name="orders",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "customer_id": ColumnInfo(
                    dataType="INTEGER", nullable=False, isPrimaryKey=False, isForeignKey=True,
                    references={"table": "customers", "column": "id"}
                ),
                "product_id": ColumnInfo(
                    dataType="INTEGER", nullable=False, isPrimaryKey=False, isForeignKey=True,
                    references={"table": "products", "column": "id"}
                )
            }
        )
        await schema_manager.add_table(orders)
        
        # Can now join: customers -> orders -> products -> categories
        orders_table = await schema_manager.get_table("orders")
        assert orders_table.columns["customer_id"].references["table"] == "customers"
        assert orders_table.columns["product_id"].references["table"] == "products"


class TestSQLExecutionWorkflow:
    """Test SQL execution workflow patterns"""
    
    @pytest.mark.asyncio
    async def test_successful_execution_flow(self):
        """Test successful query execution workflow"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Test query")
        
        # Set SQL
        await tree_manager.update_node_sql(root_id, "SELECT COUNT(*) FROM employees")
        
        # Simulate execution
        result = ExecutionResult(
            data=[[150]],
            rowCount=1
        )
        await tree_manager.update_node_result(root_id, result, success=True)
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.status == NodeStatus.EXECUTED_SUCCESS
        assert node.executionResult.rowCount == 1
        assert node.executionResult.data[0][0] == 150
    
    @pytest.mark.asyncio
    async def test_execution_error_flow(self):
        """Test query execution error handling"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        history_manager = NodeHistoryManager(memory)
        
        root_id = await tree_manager.initialize("Bad query")
        
        # Set bad SQL
        await tree_manager.update_node_sql(root_id, "SELECT * FROM non_existent_table")
        
        # Simulate execution error
        result = ExecutionResult(
            data=[],
            rowCount=0,
            error="Table 'non_existent_table' doesn't exist"
        )
        await tree_manager.update_node_result(root_id, result, success=False)
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.status == NodeStatus.EXECUTED_FAILED
        assert node.executionResult.error is not None
        assert "non_existent_table" in node.executionResult.error
    
    @pytest.mark.asyncio
    async def test_query_revision_flow(self):
        """Test query revision after error"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Query with typo")
        
        # First attempt with typo
        await tree_manager.update_node_sql(root_id, "SELCT * FROM employees")  # Typo
        
        # Execution fails
        result1 = ExecutionResult(data=[], rowCount=0, error="Syntax error near 'SELCT'")
        await tree_manager.update_node_result(root_id, result1, success=False)
        
        # Revise query
        await tree_manager.update_node(root_id, {
            "status": NodeStatus.REVISED.value,
            "sql": "SELECT * FROM employees"  # Fixed
        })
        
        # Second execution succeeds
        result2 = ExecutionResult(
            data=[["John", "john@example.com"], ["Jane", "jane@example.com"]],
            rowCount=2
        )
        await tree_manager.update_node_result(root_id, result2, success=True)
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.status == NodeStatus.EXECUTED_SUCCESS
        assert node.executionResult.rowCount == 2
    
    @pytest.mark.asyncio
    async def test_partial_results_handling(self):
        """Test handling of partial results (e.g., timeout)"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Large query")
        
        await tree_manager.update_node_sql(root_id, "SELECT * FROM huge_table")
        
        # Simulate partial results due to timeout
        result = ExecutionResult(
            data=[["row1"], ["row2"], ["row3"]],  # Only got 3 rows before timeout
            rowCount=3,
            error="Query timed out after 30 seconds. Partial results returned."
        )
        
        # Could be marked as success with warning or failed
        await tree_manager.update_node_result(root_id, result, success=True)
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.executionResult.rowCount == 3
        assert "timed out" in node.executionResult.error


class TestCompleteWorkflow:
    """Test complete end-to-end workflows"""
    
    @pytest.mark.asyncio
    async def test_simple_end_to_end(self):
        """Test simple query through all stages"""
        memory = KeyValueMemory()
        
        # Initialize task
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("e2e_simple", "Show all employee names", "test_db")
        
        # Initialize schema
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await setup_test_schema(schema_manager)
        
        # Stage 1: Query Analysis
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Show all employee names")
        
        # Stage 2: Schema Linking
        mapping = QueryMapping(
            tables=[TableMapping(name="employees")],
            columns=[ColumnMapping(table="employees", column="name", usedFor="select")]
        )
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Stage 3: SQL Generation
        await tree_manager.update_node_sql(root_id, "SELECT name FROM employees ORDER BY name")
        
        # Stage 4: Execution
        result = ExecutionResult(
            data=[["Alice"], ["Bob"], ["Charlie"]],
            rowCount=3
        )
        await tree_manager.update_node_result(root_id, result, success=True)
        
        # Verify complete workflow
        final_node = await tree_manager.get_node(root_id)
        assert final_node.status == NodeStatus.EXECUTED_SUCCESS
        assert final_node.mapping is not None
        assert final_node.sql is not None
        assert final_node.executionResult is not None
        assert final_node.executionResult.rowCount == 3
        
        # Verify task status
        task = await task_manager.get()
        await task_manager.update_status(TaskStatus.COMPLETED)
        task = await task_manager.get()
        assert task.status == TaskStatus.COMPLETED


if __name__ == "__main__":
    asyncio.run(pytest.main([__file__, "-v"]))