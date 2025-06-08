"""
Test cases for all agent tools using the actual implementation patterns

Tests the agent tools based on how they work in the codebase.
"""

import asyncio
import pytest
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, Any, List

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from keyvalue_memory import KeyValueMemory
from memory_content_types import (
    TaskContext, QueryNode, NodeStatus, TaskStatus,
    CombineStrategy, CombineStrategyType, QueryMapping,
    TableMapping, ColumnMapping, JoinMapping,
    TableSchema, ColumnInfo, ExecutionResult
)
from task_context_manager import TaskContextManager
from query_tree_manager import QueryTreeManager
from database_schema_manager import DatabaseSchemaManager
from node_history_manager import NodeHistoryManager
from query_analyzer_agent import QueryAnalyzerAgent
from schema_linker_agent import SchemaLinkerAgent
from sql_generator_agent import SQLGeneratorAgent
from sql_evaluator_agent import SQLEvaluatorAgent


async def setup_test_schema(schema_manager: DatabaseSchemaManager):
    """Setup test schema for all agent tests."""
    
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


class TestQueryTreeStructure:
    """Test query tree creation and management"""
    
    @pytest.mark.asyncio
    async def test_simple_query_tree(self):
        """Test creating a simple query tree"""
        memory = KeyValueMemory()
        
        # Initialize managers
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_001", "Show all employees", "test_db")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Show all employees")
        
        # Verify root node
        root_node = await tree_manager.get_node(root_id)
        assert root_node is not None
        assert root_node.intent == "Show all employees"
        assert root_node.status == NodeStatus.CREATED
        assert len(root_node.childIds) == 0
    
    @pytest.mark.asyncio
    async def test_complex_query_tree(self):
        """Test creating a complex query tree with children"""
        memory = KeyValueMemory()
        
        # Initialize
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_002", "Complex query", "test_db")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Find top departments by salary")
        
        # Add child nodes
        child1 = QueryNode(
            nodeId="child_1",
            intent="Calculate average salary per department",
            mapping=QueryMapping()
        )
        await tree_manager.add_node(child1, root_id)
        
        child2 = QueryNode(
            nodeId="child_2",
            intent="Count employees per department",
            mapping=QueryMapping()
        )
        await tree_manager.add_node(child2, root_id)
        
        # Update root with combine strategy
        root_node = await tree_manager.get_node(root_id)
        strategy = CombineStrategy(
            type=CombineStrategyType.JOIN,
            template="Combine salary and count data"
        )
        await tree_manager.update_node(root_id, {
            "combineStrategy": strategy.to_dict()
        })
        
        # Verify structure
        root_node = await tree_manager.get_node(root_id)
        assert len(root_node.childIds) == 2
        assert "child_1" in root_node.childIds
        assert "child_2" in root_node.childIds
        
        # Verify parent-child relationships
        child = await tree_manager.get_node("child_1")
        assert child.parentId == root_id
    
    @pytest.mark.asyncio
    async def test_node_status_updates(self):
        """Test updating node statuses through workflow"""
        memory = KeyValueMemory()
        
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("test_003", "Test query", "test_db")
        
        tree_manager = QueryTreeManager(memory)
        history_manager = NodeHistoryManager(memory)
        
        root_id = await tree_manager.initialize("Test query")
        
        # Add schema mapping
        mapping = QueryMapping(
            tables=[TableMapping(name="employees")],
            columns=[ColumnMapping(table="employees", column="name", usedFor="select")]
        )
        await tree_manager.update_node(root_id, {
            "mapping": mapping.to_dict()
        })
        
        # Update to SQL generated
        await tree_manager.update_node_sql(root_id, "SELECT name FROM employees")
        
        node = await tree_manager.get_node(root_id)
        assert node.status == NodeStatus.SQL_GENERATED
        assert node.sql == "SELECT name FROM employees"
        
        # Update with execution result
        result = ExecutionResult(
            data=[["John"], ["Jane"]],
            rowCount=2
        )
        await tree_manager.update_node_result(root_id, result, success=True)
        
        node = await tree_manager.get_node(root_id)
        assert node.status == NodeStatus.EXECUTED_SUCCESS
        assert node.executionResult.rowCount == 2


class TestSchemaMapping:
    """Test schema mapping functionality"""
    
    @pytest.mark.asyncio
    async def test_simple_table_mapping(self):
        """Test mapping a simple table reference"""
        memory = KeyValueMemory()
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await setup_test_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Show all employees")
        
        # Create mapping
        mapping = QueryMapping(
            tables=[TableMapping(name="employees", purpose="Main data source")],
            columns=[
                ColumnMapping(table="employees", column="id", usedFor="select"),
                ColumnMapping(table="employees", column="name", usedFor="select"),
                ColumnMapping(table="employees", column="email", usedFor="select")
            ]
        )
        
        await tree_manager.update_node(root_id, {
            "mapping": mapping.to_dict()
        })
        
        # Verify mapping
        node = await tree_manager.get_node(root_id)
        assert len(node.mapping.tables) == 1
        assert node.mapping.tables[0].name == "employees"
        assert len(node.mapping.columns) == 3
    
    @pytest.mark.asyncio
    async def test_join_mapping(self):
        """Test mapping with joins"""
        memory = KeyValueMemory()
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await setup_test_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Show employees with department names")
        
        # Create mapping with join
        mapping = QueryMapping(
            tables=[
                TableMapping(name="employees", alias="e"),
                TableMapping(name="departments", alias="d")
            ],
            columns=[
                ColumnMapping(table="employees", column="name", usedFor="select"),
                ColumnMapping(table="departments", column="name", usedFor="select")
            ],
            joins=[
                JoinMapping(
                    from_table="employees",
                    to="departments",
                    on="employees.department_id = departments.id"
                )
            ]
        )
        
        await tree_manager.update_node(root_id, {
            "mapping": mapping.to_dict()
        })
        
        # Verify mapping
        node = await tree_manager.get_node(root_id)
        assert len(node.mapping.tables) == 2
        assert len(node.mapping.joins) == 1
        assert node.mapping.joins[0].from_table == "employees"
        assert node.mapping.joins[0].to == "departments"


class TestSQLGeneration:
    """Test SQL generation patterns"""
    
    @pytest.mark.asyncio
    async def test_simple_sql_generation(self):
        """Test generating simple SQL"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Select all employees")
        
        # Add mapping
        mapping = QueryMapping(
            tables=[TableMapping(name="employees")],
            columns=[ColumnMapping(table="employees", column="*", usedFor="select")]
        )
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Simulate SQL generation
        sql = "SELECT * FROM employees"
        await tree_manager.update_node_sql(root_id, sql)
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.sql == sql
        assert node.status == NodeStatus.SQL_GENERATED
    
    @pytest.mark.asyncio
    async def test_complex_sql_with_children(self):
        """Test SQL generation with child results"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Complex aggregation")
        
        # Create child with results
        child = QueryNode(
            nodeId="child_1",
            intent="Get department averages",
            mapping=QueryMapping(),
            sql="SELECT dept_id, AVG(salary) FROM employees GROUP BY dept_id",
            executionResult=ExecutionResult(
                data=[[1, 75000], [2, 82000]],
                rowCount=2
            ),
            status=NodeStatus.EXECUTED_SUCCESS
        )
        await tree_manager.add_node(child, root_id)
        
        # Root SQL can reference child results
        root_sql = """
        WITH dept_avgs AS (
            -- Results from child_1
            SELECT * FROM (VALUES (1, 75000), (2, 82000)) AS t(dept_id, avg_salary)
        )
        SELECT * FROM dept_avgs ORDER BY avg_salary DESC
        """
        await tree_manager.update_node_sql(root_id, root_sql)
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert "WITH dept_avgs" in node.sql
        assert len(node.childIds) == 1


class TestExecutionResults:
    """Test execution result handling"""
    
    @pytest.mark.asyncio
    async def test_successful_execution(self):
        """Test handling successful execution"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Test query")
        
        # Set SQL
        await tree_manager.update_node_sql(root_id, "SELECT COUNT(*) FROM employees")
        
        # Execute with success
        result = ExecutionResult(
            data=[[42]],
            rowCount=1
        )
        await tree_manager.update_node_result(root_id, result, success=True)
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.status == NodeStatus.EXECUTED_SUCCESS
        assert node.executionResult.rowCount == 1
        assert node.executionResult.data[0][0] == 42
        assert node.executionResult.error is None
    
    @pytest.mark.asyncio
    async def test_failed_execution(self):
        """Test handling failed execution"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize("Bad query")
        
        # Set SQL
        await tree_manager.update_node_sql(root_id, "SELECT * FROM non_existent_table")
        
        # Execute with failure
        result = ExecutionResult(
            data=[],
            rowCount=0,
            error="Table 'non_existent_table' does not exist"
        )
        await tree_manager.update_node_result(root_id, result, success=False)
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.status == NodeStatus.EXECUTED_FAILED
        assert node.executionResult.error is not None
        assert "non_existent_table" in node.executionResult.error
    
    @pytest.mark.asyncio
    async def test_revision_after_failure(self):
        """Test revising a query after failure"""
        memory = KeyValueMemory()
        
        tree_manager = QueryTreeManager(memory)
        history_manager = NodeHistoryManager(memory)
        
        root_id = await tree_manager.initialize("Query needing revision")
        
        # First attempt fails
        await tree_manager.update_node_sql(root_id, "SELECT * FROM employes")  # Typo
        result = ExecutionResult(data=[], rowCount=0, error="Table 'employes' not found")
        await tree_manager.update_node_result(root_id, result, success=False)
        
        # Revise
        await tree_manager.update_node(root_id, {
            "status": NodeStatus.REVISED.value,
            "sql": "SELECT * FROM employees"  # Fixed typo
        })
        
        # Second attempt succeeds
        result2 = ExecutionResult(data=[["John"], ["Jane"]], rowCount=2)
        await tree_manager.update_node_result(root_id, result2, success=True)
        
        # Verify final state
        node = await tree_manager.get_node(root_id)
        assert node.status == NodeStatus.EXECUTED_SUCCESS
        assert node.sql == "SELECT * FROM employees"
        assert node.executionResult.rowCount == 2


class TestAgentIntegration:
    """Test agent integration and workflow"""
    
    @pytest.mark.asyncio
    async def test_workflow_data_flow(self):
        """Test data flow through workflow stages"""
        memory = KeyValueMemory()
        
        # Setup
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("workflow_test", "Find all employees in IT", "test_db")
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await setup_test_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        history_manager = NodeHistoryManager(memory)
        
        # Stage 1: Query analysis (simulated)
        root_id = await tree_manager.initialize("Find all employees in IT department")
        
        # Stage 2: Schema linking (simulated)
        it_dept = await schema_manager.get_all_tables()
        mapping = QueryMapping(
            tables=[
                TableMapping(name="employees", alias="e"),
                TableMapping(name="departments", alias="d")
            ],
            columns=[
                ColumnMapping(table="employees", column="name", usedFor="select"),
                ColumnMapping(table="departments", column="name", usedFor="filter")
            ],
            joins=[
                JoinMapping(
                    from_table="employees",
                    to="departments",
                    on="e.department_id = d.id"
                )
            ]
        )
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Stage 3: SQL generation (simulated)
        sql = """
        SELECT e.name
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE d.name = 'IT'
        """
        await tree_manager.update_node_sql(root_id, sql)
        
        # Stage 4: Execution (simulated)
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


if __name__ == "__main__":
    # Run all tests
    asyncio.run(pytest.main([__file__, "-v"]))