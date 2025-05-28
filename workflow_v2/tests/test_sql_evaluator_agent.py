"""
Test cases for SQLEvaluatorAgent using real LLM and BIRD dataset.

Tests the actual run method and internal implementation.
"""

import asyncio
import pytest
import os
from pathlib import Path
import sys
import logging
from typing import Dict, List, Optional, Any, Tuple
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from keyvalue_memory import KeyValueMemory
from memory_content_types import (
    TaskContext, QueryNode, NodeStatus, TaskStatus,
    TableSchema, ColumnInfo, ExecutionResult
)
from task_context_manager import TaskContextManager
from query_tree_manager import QueryTreeManager
from database_schema_manager import DatabaseSchemaManager
from node_history_manager import NodeHistoryManager
from sql_evaluator_agent import SQLEvaluatorAgent
from sql_executor import SQLExecutor
from schema_reader import SchemaReader


class TestSQLEvaluatorAgent:
    """Test cases for SQLEvaluatorAgent"""
    
    async def setup_test_environment(self, query: str, task_id: str, db_name: str = "california_schools"):
        """Setup test environment with schema loaded"""
        memory = KeyValueMemory()
        
        # Initialize task
        task_manager = TaskContextManager(memory)
        await task_manager.initialize(task_id, query, db_name)
        
        # Load schema
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        
        # Load real schema from BIRD dataset
        data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
        tables_json_path = Path(data_path) / "dev_tables.json"
        
        if tables_json_path.exists():
            schema_reader = SchemaReader(
                data_path=data_path,
                tables_json_path=str(tables_json_path),
                dataset_name="bird",
                lazy=False
            )
            await schema_manager.load_from_schema_reader(schema_reader, db_name)
        else:
            # Fallback to manual schema for testing
            await self._setup_manual_schema(schema_manager)
        
        return memory
    
    async def _setup_manual_schema(self, schema_manager: DatabaseSchemaManager):
        """Setup basic test schema"""
        # schools table
        schools_schema = TableSchema(
            name="schools",
            columns={
                "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "School": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "County": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "City": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(schools_schema)
        
        # frpm table
        frpm_schema = TableSchema(
            name="frpm",
            columns={
                "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=True,
                                    references={"table": "schools", "column": "CDSCode"}),
                "Eligible Free Rate (K-12)": ColumnInfo(dataType="REAL", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Free Meal Count (K-12)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(frpm_schema)
    
    async def create_test_node_with_sql(self, memory: KeyValueMemory, intent: str, sql: str) -> str:
        """Create a test node with SQL"""
        tree_manager = QueryTreeManager(memory)
        
        # Create node
        node_id = await tree_manager.initialize(intent)
        
        # Update node with SQL and generation field
        await tree_manager.update_node_sql(node_id, sql)
        # Also update generation field as SQLGeneratorAgent would
        await tree_manager.update_node(node_id, {"generation": {"sql": sql, "query_type": "simple"}})
        
        return node_id
    
    def create_mock_execution_result(self, data: List[Dict] = None, 
                                   error: str = None, row_count: int = None) -> ExecutionResult:
        """Create a mock execution result"""
        if data is None:
            data = []
        if row_count is None:
            row_count = len(data)
            
        return ExecutionResult(
            data=data,
            rowCount=row_count,
            error=error
        )
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_successful_execution(self):
        """Test evaluating a successful SQL execution"""
        query = "What is the highest eligible free rate in Alameda County?"
        memory = await self.setup_test_environment(query, "test_success")
        
        # Create a node with SQL
        sql = """
        SELECT MAX(f."Eligible Free Rate (K-12)") as max_rate
        FROM schools s
        JOIN frpm f ON s.CDSCode = f.CDSCode
        WHERE s.County = 'Alameda'
        """
        node_id = await self.create_test_node_with_sql(memory, query, sql)
        
        # Create mock execution result
        execution_result = self.create_mock_execution_result(
            data=[{"max_rate": 0.95}],
            row_count=1
        )
        
        # Store execution result in node's evaluation field
        tree_manager = QueryTreeManager(memory)
        await tree_manager.update_node_result(node_id, execution_result, success=True)
        # Also store in evaluation field as SQLEvaluatorAgent expects
        await tree_manager.update_node(node_id, {
            "evaluation": {
                "execution_result": execution_result.to_dict()
            }
        })
        
        # Create evaluator
        agent = SQLEvaluatorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        }, debug=True)
        
        # Run the agent - SQLEvaluator uses current_node_id from tree manager
        result = await agent.run("Evaluate SQL execution results")
        
        # Verify the agent ran
        assert result is not None
        assert hasattr(result, 'messages')
        assert len(result.messages) > 0
        
        # Check evaluation was stored
        evaluation = await memory.get("execution_analysis")
        assert evaluation is not None
        
        print(f"\nEvaluation result: {evaluation}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_failed_execution(self):
        """Test evaluating a failed SQL execution"""
        query = "Find schools with invalid data"
        memory = await self.setup_test_environment(query, "test_failure")
        
        # Create a node with invalid SQL
        sql = "SELECT * FROM non_existent_table"
        node_id = await self.create_test_node_with_sql(memory, query, sql)
        
        # Create mock error result
        execution_result = self.create_mock_execution_result(
            error="no such table: non_existent_table"
        )
        
        # Store execution result
        tree_manager = QueryTreeManager(memory)
        await tree_manager.update_node_result(node_id, execution_result, success=False)
        # Also store in evaluation field
        await tree_manager.update_node(node_id, {
            "evaluation": {
                "execution_result": execution_result.to_dict()
            }
        })
        
        # Create evaluator
        agent = SQLEvaluatorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        # Run the agent - SQLEvaluator uses current_node_id from tree manager
        result = await agent.run("Evaluate SQL execution results")
        
        assert result is not None
        assert len(result.messages) > 0
        
        # Check evaluation
        evaluation = await memory.get("execution_analysis")
        assert evaluation is not None
        # For errors, the agent should identify issues
        assert "issues" in evaluation or "result_quality" in evaluation
        
        print(f"\nError evaluation: {evaluation}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_empty_results(self):
        """Test evaluating SQL that returns no results"""
        query = "Find schools in a non-existent county"
        memory = await self.setup_test_environment(query, "test_empty")
        
        sql = "SELECT * FROM schools WHERE County = 'NonExistentCounty'"
        node_id = await self.create_test_node_with_sql(memory, query, sql)
        
        # Create empty result
        execution_result = self.create_mock_execution_result(
            data=[],
            row_count=0
        )
        
        tree_manager = QueryTreeManager(memory)
        await tree_manager.update_node_result(node_id, execution_result, success=True)
        # Also store in evaluation field
        await tree_manager.update_node(node_id, {
            "evaluation": {
                "execution_result": execution_result.to_dict()
            }
        })
        
        agent = SQLEvaluatorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        result = await agent.run("Evaluate SQL execution results")
        
        assert result is not None
        evaluation = await memory.get("execution_analysis")
        assert evaluation is not None
        
        print(f"\nEmpty result evaluation: {evaluation}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_reader_callback(self):
        """Test the _reader_callback method"""
        query = "Test query"
        memory = await self.setup_test_environment(query, "test_reader")
        
        sql = "SELECT COUNT(*) FROM schools"
        node_id = await self.create_test_node_with_sql(memory, query, sql)
        
        # Add execution result
        execution_result = self.create_mock_execution_result(
            data=[{"count": 100}]
        )
        tree_manager = QueryTreeManager(memory)
        await tree_manager.update_node_result(node_id, execution_result, success=True)
        # Also store in evaluation field
        await tree_manager.update_node(node_id, {
            "evaluation": {
                "execution_result": execution_result.to_dict()
            }
        })
        
        agent = SQLEvaluatorAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test reader callback
        context = await agent._reader_callback(memory, "task", None)
        
        assert context is not None
        assert "node_id" in context
        assert "intent" in context
        assert "sql" in context
        assert "execution_result" in context
        
        print(f"\nReader callback context keys: {list(context.keys())}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_parse_evaluation_xml(self):
        """Test XML parsing of evaluation results"""
        memory = KeyValueMemory()
        agent = SQLEvaluatorAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test evaluation XML
        eval_xml = """
        <evaluation>
          <answers_intent>yes</answers_intent>
          <result_quality>good</result_quality>
          <result_summary>The query returns the maximum eligible free rate for schools in Alameda County</result_summary>
          <issues>
            <issue>
              <type>performance</type>
              <description>Query might be slow on large datasets</description>
              <severity>low</severity>
            </issue>
          </issues>
          <suggestions>
            <suggestion>Consider adding an index on County column</suggestion>
          </suggestions>
          <confidence_score>0.9</confidence_score>
        </evaluation>
        """
        
        result = agent._parse_evaluation_xml(eval_xml)
        
        assert result is not None
        assert result["answers_intent"] == "yes"
        assert result["result_quality"] == "good"
        
        # Handle both single issue and list of issues
        issues = result.get("issues", {})
        if isinstance(issues, dict) and "issue" in issues:
            # Single issue case - convert to list
            issue_list = [issues["issue"]] if not isinstance(issues["issue"], list) else issues["issue"]
        else:
            issue_list = []
        
        assert len(issue_list) == 1
        assert issue_list[0]["type"] == "performance"
        
        # Handle suggestions similarly
        suggestions = result.get("suggestions", {})
        if isinstance(suggestions, dict) and "suggestion" in suggestions:
            suggestion_list = [suggestions["suggestion"]] if not isinstance(suggestions["suggestion"], list) else suggestions["suggestion"]
        else:
            suggestion_list = []
            
        assert len(suggestion_list) == 1
        assert float(result["confidence_score"]) == 0.9
        
        print(f"\nParsed evaluation: {result}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_real_bird_queries(self):
        """Test with real BIRD dataset queries"""
        test_cases = [
            {
                "query": "How many schools are in Los Angeles?",
                "sql": "SELECT COUNT(*) as school_count FROM schools WHERE City = 'Los Angeles'",
                "mock_result": [{"school_count": 150}]
            },
            {
                "query": "What is the average SAT math score?",
                "sql": "SELECT AVG(AvgScrMath) as avg_math FROM satscores WHERE AvgScrMath IS NOT NULL",
                "mock_result": [{"avg_math": 520.5}]
            }
        ]
        
        for i, test_case in enumerate(test_cases):
            print(f"\n--- Testing BIRD Query {i+1} ---")
            print(f"Query: {test_case['query']}")
            
            memory = await self.setup_test_environment(test_case['query'], f"bird_test_{i}")
            
            # Create node with SQL
            node_id = await self.create_test_node_with_sql(
                memory,
                test_case['query'],
                test_case['sql']
            )
            
            # Add execution result
            execution_result = self.create_mock_execution_result(
                data=test_case['mock_result']
            )
            tree_manager = QueryTreeManager(memory)
            await tree_manager.update_node_result(node_id, execution_result, success=True)
            # Also store in evaluation field
            await tree_manager.update_node(node_id, {
                "evaluation": {
                    "execution_result": execution_result.to_dict()
                }
            })
            
            # Create and run evaluator
            agent = SQLEvaluatorAgent(memory, llm_config={
                "model_name": "gpt-4o",
                "temperature": 0.1,
                "timeout": 60
            })
            
            result = await agent.run("Evaluate SQL execution results")
            
            assert result is not None
            assert len(result.messages) > 0
            
            evaluation = await memory.get("execution_analysis")
            assert evaluation is not None
            
            print(f"Result quality: {evaluation.get('result_quality')}")
            print(f"Answers intent: {evaluation.get('answers_intent')}")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))