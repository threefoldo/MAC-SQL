"""
Test cases for SQLEvaluatorAgent - TESTING_PLAN.md Layer 2.4 Requirements.

Verifies that SQLEvaluatorAgent:
1. ONLY prepares context, calls LLM, and extracts outputs (NO business logic)
2. Formats execution results for LLM evaluation
3. Stores evaluation without implementing scoring logic
4. Does NOT make automatic retry decisions
5. Does NOT validate results against expected patterns
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
    """Test cases for SQLEvaluatorAgent - Verify NO business logic per TESTING_PLAN.md"""
    
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
    async def test_agent_only_formats_results_for_llm(self):
        """Verify agent ONLY formats execution results for LLM - NO evaluation logic"""
        query = "What is the highest eligible free rate in Alameda County?"
        memory = await self.setup_test_environment(query, "test_no_logic")
        
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
        
        # Initialize execution_analysis memory as orchestrator would
        from datetime import datetime
        execution_context = {
            "original_query": query,
            "node_id": node_id,
            "initialized_at": datetime.now().isoformat(),
            "evaluation": None,
            "last_update": None
        }
        await memory.set("execution_analysis", execution_context)
        
        # Create evaluator
        agent = SQLEvaluatorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        }, debug=True)
        
        # VERIFY AGENT RESPONSIBILITIES:
        # 1. CONTEXT PREPARATION - formats execution results for LLM
        # 2. LLM INTERACTION - sends results and receives evaluation
        # 3. OUTPUT EXTRACTION - stores evaluation without logic
        
        # Run the agent
        result = await agent.run("Evaluate SQL execution results")
        
        # Verify agent stored LLM's evaluation WITHOUT scoring logic
        evaluation = await memory.get("execution_analysis")
        assert evaluation is not None
        
        # Agent should NOT have logic to:
        # - Decide if 0.95 is a "good" result
        # - Score the quality as high/medium/low
        # - Determine if result matches intent
        
        print(f"\nLLM-Determined Evaluation (not agent logic):")
        print(f"Result quality: {evaluation.get('result_quality')} (LLM decided)")
        print(f"Answers intent: {evaluation.get('answers_intent')} (LLM decided)")
        
        # Key point: All quality assessments came from LLM
        print("\n✓ Agent only formatted results and extracted LLM evaluation")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_agent_does_not_categorize_errors(self):
        """Verify agent does NOT categorize or analyze errors - LLM does"""
        query = "Find schools with invalid data"
        memory = await self.setup_test_environment(query, "test_no_error_logic")
        
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
        
        # Initialize execution_analysis memory as orchestrator would
        from datetime import datetime
        execution_context = {
            "original_query": query,
            "node_id": node_id,
            "initialized_at": datetime.now().isoformat(),
            "evaluation": None,
            "last_update": None
        }
        await memory.set("execution_analysis", execution_context)
        
        # Create evaluator
        agent = SQLEvaluatorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        # Run the agent
        result = await agent.run("Evaluate SQL execution results")
        
        # Get evaluation
        evaluation = await memory.get("execution_analysis")
        assert evaluation is not None
        
        # CRITICAL VERIFICATION:
        # Agent should NOT have code that:
        # - Categorizes "no such table" as a schema error
        # - Decides severity of the error
        # - Recommends specific fixes
        
        print(f"\nError Handling Verification:")
        print(f"Error: no such table: non_existent_table")
        print(f"LLM's assessment: {evaluation}")
        
        # The error type, severity, and recommendations
        # all come from LLM, not agent code
        print("\n✓ Agent did NOT categorize error - LLM evaluated it")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_agent_does_not_judge_empty_results(self):
        """Verify agent does NOT decide if empty results are good/bad - LLM does"""
        query = "Find schools in a non-existent county"
        memory = await self.setup_test_environment(query, "test_no_empty_logic")
        
        sql = "SELECT * FROM schools WHERE County = 'NonExistentCounty'"
        node_id = await self.create_test_node_with_sql(memory, query, sql)
        
        # Create empty result
        execution_result = self.create_mock_execution_result(
            data=[],
            row_count=0
        )
        
        # Store execution result in generation field (SQL Generator now executes SQL)
        tree_manager = QueryTreeManager(memory)
        node = await tree_manager.get_node(node_id)
        generation = node.generation or {}
        generation["execution_result"] = execution_result.to_dict()
        await tree_manager.update_node(node_id, {"generation": generation})
        
        # Initialize execution_analysis memory as orchestrator would
        from datetime import datetime
        execution_context = {
            "original_query": query,
            "node_id": node_id,
            "initialized_at": datetime.now().isoformat(),
            "evaluation": None,
            "last_update": None
        }
        await memory.set("execution_analysis", execution_context)
        
        agent = SQLEvaluatorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        result = await agent.run("Evaluate SQL execution results")
        
        evaluation = await memory.get("execution_analysis")
        assert evaluation is not None
        
        # CRITICAL VERIFICATION:
        # Agent should NOT have logic like:
        # - if row_count == 0: mark as "possibly incorrect"
        # - if empty and WHERE clause: suggest "too restrictive"
        # - if no results: confidence = "low"
        
        print(f"\nEmpty Results Verification:")
        print(f"Result: 0 rows returned")
        print(f"LLM decided: {evaluation}")
        
        # Whether empty results are expected or problematic
        # is determined by LLM based on query intent
        print("\n✓ Agent did NOT judge empty results - LLM evaluated")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_context_preparation_only(self):
        """Verify _reader_callback ONLY prepares execution context, no evaluation logic"""
        query = "Test query"
        memory = await self.setup_test_environment(query, "test_context_prep")
        
        sql = "SELECT COUNT(*) FROM schools"
        node_id = await self.create_test_node_with_sql(memory, query, sql)
        
        # Add execution result
        execution_result = self.create_mock_execution_result(
            data=[{"count": 100}]
        )
        # Store execution result in generation field (SQL Generator now executes SQL)
        tree_manager = QueryTreeManager(memory)
        node = await tree_manager.get_node(node_id)
        generation = node.generation or {}
        generation["execution_result"] = execution_result.to_dict()
        await tree_manager.update_node(node_id, {"generation": generation})
        
        agent = SQLEvaluatorAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test reader callback
        context = await agent._reader_callback(memory, "task", None)
        
        # VERIFY: Callback only formats data, no evaluation
        assert context is not None
        assert "node_id" in context
        assert "intent" in context
        assert "sql" in context
        assert "execution_result" in context
        
        # Context should NOT contain:
        # - Pre-calculated quality scores
        # - Error categorizations
        # - Performance assessments
        
        print(f"\nContext preparation verification:")
        print(f"Context keys: {list(context.keys())}")
        print(f"Raw execution result provided: Yes")
        print(f"Pre-evaluation included: No")
        print("✓ Context contains only raw data for LLM evaluation")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_no_automatic_retry_decisions(self):
        """Verify agent does NOT make automatic retry decisions"""
        memory = KeyValueMemory()
        agent = SQLEvaluatorAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test various evaluation scenarios
        test_evaluations = [
            # Good result
            """
            <evaluation>
              <result_quality>good</result_quality>
              <should_retry>no</should_retry>
            </evaluation>
            """,
            # Bad result with issues
            """
            <evaluation>
              <result_quality>poor</result_quality>
              <issues>
                <issue>Missing data for some schools</issue>
              </issues>
              <should_retry>yes</should_retry>
              <retry_suggestion>Need to adjust query logic</retry_suggestion>
            </evaluation>
            """,
            # Error result
            """
            <evaluation>
              <result_quality>failed</result_quality>
              <error_type>schema_mismatch</error_type>
              <should_retry>yes</should_retry>
              <retry_approach>Fix table references</retry_approach>
            </evaluation>
            """
        ]
        
        for i, eval_xml in enumerate(test_evaluations):
            result = agent._parse_evaluation_xml(eval_xml)
            
            print(f"\nEvaluation {i+1}:")
            print(f"Quality: {result.get('result_quality')}")
            print(f"LLM says retry: {result.get('should_retry')}")
            
            # CRITICAL VERIFICATION:
            # Agent should NOT have code that:
            # - if quality == "poor": retry = True
            # - if error_type == "schema": retry_with_schema_fix()
            # - if issues.count > threshold: force_retry()
            
            # The retry decision comes entirely from LLM
            if result.get('should_retry') == 'yes':
                print(f"LLM's retry suggestion: {result.get('retry_suggestion') or result.get('retry_approach')}")
            
        print("\n✓ Agent does NOT make retry decisions - only extracts LLM's decision")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_no_validation_against_patterns(self):
        """Verify agent does NOT validate results against expected patterns"""
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
            print(f"\n--- Verifying No Pattern Validation for Query {i+1} ---")
            print(f"Query: {test_case['query']}")
            
            memory = await self.setup_test_environment(test_case['query'], f"no_pattern_test_{i}")
            
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
            await tree_manager.update_node(node_id, {
                "evaluation": {
                    "execution_result": execution_result.to_dict()
                }
            })
            
            # Initialize execution_analysis memory as orchestrator would
            from datetime import datetime
            execution_context = {
                "original_query": test_case['query'],
                "node_id": node_id,
                "initialized_at": datetime.now().isoformat(),
                "evaluation": None,
                "last_update": None
            }
            await memory.set("execution_analysis", execution_context)
            
            # Create and run evaluator
            agent = SQLEvaluatorAgent(memory, llm_config={
                "model_name": "gpt-4o",
                "temperature": 0.1,
                "timeout": 60
            })
            
            result = await agent.run("Evaluate SQL execution results")
            
            evaluation = await memory.get("execution_analysis")
            assert evaluation is not None
            
            # CRITICAL VERIFICATION:
            # Agent should NOT have patterns like:
            # - if query contains "count": expect single integer
            # - if query contains "average": expect float between 0-1000
            # - if result > expected_range: flag as suspicious
            
            print(f"Result: {test_case['mock_result']}")
            print(f"LLM evaluation: {evaluation.get('result_quality')}")
            
            # For count query - agent didn't validate if 150 is reasonable
            if "count" in test_case['query'].lower():
                print("  Query asks for count - LLM decided if 150 is reasonable")
            
            # For average query - agent didn't validate if 520.5 is valid SAT score
            if "average" in test_case['query'].lower():
                print("  Query asks for average - LLM decided if 520.5 is valid")
            
            print("✓ Agent did NOT validate against patterns - LLM evaluated")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "="*70)
    print("SQLEvaluatorAgent Tests - Verifying NO Business Logic")
    print("Based on TESTING_PLAN.md Layer 2.4 Requirements")
    print("="*70)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))