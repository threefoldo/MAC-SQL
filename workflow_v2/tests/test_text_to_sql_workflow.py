"""
Tests for the complete text-to-SQL workflow system.

This module contains comprehensive tests for the TextToSQLWorkflow class
and the run_text_to_sql convenience function.
"""

import os
import sys
import pytest
import asyncio
from pathlib import Path
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from text_to_sql_workflow import TextToSQLWorkflow, run_text_to_sql


class TestTextToSQLWorkflow:
    """Test cases for the TextToSQLWorkflow class."""
    
    @pytest.fixture
    def data_path(self):
        """Test data path."""
        return "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
    
    @pytest.fixture
    def tables_json_path(self, data_path):
        """Test tables JSON path."""
        return str(Path(data_path) / "dev_tables.json")
    
    @pytest.fixture
    def workflow(self, data_path, tables_json_path):
        """Create a test workflow instance."""
        return TextToSQLWorkflow(
            data_path=data_path,
            tables_json_path=tables_json_path,
            dataset_name="bird"
        )
    
    def test_workflow_initialization(self, workflow):
        """Test that workflow initializes correctly."""
        assert workflow.data_path is not None
        assert workflow.tables_json_path is not None
        assert workflow.dataset_name == "bird"
        assert workflow.memory is not None
        assert workflow.task_manager is not None
        assert workflow.tree_manager is not None
        assert workflow.schema_manager is not None
        assert workflow.history_manager is not None
        assert workflow.query_analyzer is not None
        assert workflow.schema_linker is not None
        assert workflow.sql_generator is not None
        assert workflow.sql_evaluator is not None
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="No OpenAI API key")
    async def test_database_initialization(self, workflow):
        """Test database schema initialization."""
        db_name = "california_schools"
        
        await workflow.initialize_database(db_name)
        
        # Check that schema was loaded
        summary = await workflow.schema_manager.get_schema_summary()
        assert summary['table_count'] > 0
        assert summary['total_columns'] > 0
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="No OpenAI API key")
    async def test_simple_query_processing(self, workflow):
        """Test processing a simple query."""
        query = "How many schools are there?"
        db_name = "california_schools"
        
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            use_coordinator=False  # Use sequential for testing
        )
        
        # Check basic structure
        assert "query_tree" in results
        assert "nodes" in results
        assert "final_results" in results
        
        # Check that we have at least one node
        assert len(results["nodes"]) > 0
        
        # Check that root node exists
        tree = results["query_tree"]
        assert "rootId" in tree
        assert tree["rootId"] in results["nodes"]
        
        # Check node structure
        root_node = results["nodes"][tree["rootId"]]
        assert root_node["node_id"] == tree["rootId"]
        assert root_node["intent"] is not None
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="No OpenAI API key")
    async def test_complex_query_processing(self, workflow):
        """Test processing a more complex query."""
        query = "What is the highest eligible free rate for K-12 students in schools located in Alameda County?"
        db_name = "california_schools"
        
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            use_coordinator=False
        )
        
        # Check that we got results
        assert len(results["final_results"]) > 0
        
        final_result = results["final_results"][0]
        
        # Check that SQL was generated
        assert final_result["sql"] is not None
        assert "SELECT" in final_result["sql"].upper()
        
        # Check that execution happened
        assert final_result["execution_result"] is not None
        exec_result = final_result["execution_result"]
        assert "rowCount" in exec_result
    
    @pytest.mark.asyncio
    async def test_display_functions(self, workflow):
        """Test the display functions don't crash."""
        # These should not crash even with empty tree
        await workflow.display_query_tree()
        await workflow.display_final_results()
    
    def test_coordinator_creation(self, workflow):
        """Test coordinator agent creation."""
        coordinator = workflow._create_coordinator()
        assert coordinator is not None
        assert coordinator.name == "coordinator"
        assert len(coordinator.tools) == 4  # Should have all 4 agents


class TestRunTextToSQL:
    """Test cases for the run_text_to_sql convenience function."""
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="No OpenAI API key")
    async def test_run_text_to_sql_basic(self):
        """Test the basic run_text_to_sql function."""
        query = "How many schools are there?"
        db_name = "california_schools"
        
        results = await run_text_to_sql(
            query=query,
            db_name=db_name,
            use_coordinator=False,
            display_results=False
        )
        
        # Check basic structure
        assert "query_tree" in results
        assert "nodes" in results
        assert "final_results" in results
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="No OpenAI API key")
    async def test_run_text_to_sql_with_parameters(self):
        """Test run_text_to_sql with custom parameters."""
        query = "What schools are in Alameda County?"
        db_name = "california_schools"
        custom_data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
        
        results = await run_text_to_sql(
            query=query,
            db_name=db_name,
            data_path=custom_data_path,
            dataset_name="bird",
            use_coordinator=False,
            display_results=False
        )
        
        assert results is not None
        assert "final_results" in results


class TestWorkflowIntegration:
    """Integration tests for the complete workflow."""
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="No OpenAI API key")
    async def test_end_to_end_workflow(self):
        """Test complete end-to-end workflow."""
        test_cases = [
            {
                "name": "Simple count query",
                "query": "How many schools are there?",
                "expected_sql_keywords": ["SELECT", "COUNT", "schools"]
            },
            {
                "name": "Filtering query",
                "query": "What schools are in Alameda County?",
                "expected_sql_keywords": ["SELECT", "FROM", "schools", "WHERE", "County", "Alameda"]
            }
        ]
        
        for test_case in test_cases:
            print(f"\nTesting: {test_case['name']}")
            
            results = await run_text_to_sql(
                query=test_case["query"],
                db_name="california_schools",
                use_coordinator=False,
                display_results=False
            )
            
            # Check that we got results
            assert len(results["final_results"]) > 0, f"No results for: {test_case['name']}"
            
            final_result = results["final_results"][0]
            
            # Check SQL generation
            assert final_result["sql"] is not None, f"No SQL generated for: {test_case['name']}"
            
            sql_upper = final_result["sql"].upper()
            for keyword in test_case["expected_sql_keywords"]:
                assert keyword.upper() in sql_upper, f"Missing keyword '{keyword}' in SQL for: {test_case['name']}"
            
            # Check execution
            assert final_result["execution_result"] is not None, f"No execution result for: {test_case['name']}"
            exec_result = final_result["execution_result"]
            assert "rowCount" in exec_result, f"No row count in execution result for: {test_case['name']}"
            
            print(f"✓ {test_case['name']} completed successfully")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="No OpenAI API key")
    async def test_error_handling(self):
        """Test error handling in the workflow."""
        # Test with invalid database name
        with pytest.raises(Exception):
            await run_text_to_sql(
                query="Test query",
                db_name="nonexistent_database",
                use_coordinator=False,
                display_results=False
            )
    
    @pytest.mark.asyncio
    async def test_workflow_without_api_key(self):
        """Test workflow behavior without API key."""
        # Temporarily remove API key
        original_key = os.environ.get("OPENAI_API_KEY")
        if original_key:
            del os.environ["OPENAI_API_KEY"]
        
        try:
            # This should either skip gracefully or handle the missing key
            workflow = TextToSQLWorkflow(
                data_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird",
                tables_json_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird/dev_tables.json",
                dataset_name="bird"
            )
            
            # Just test initialization, not actual processing
            assert workflow is not None
            
        finally:
            # Restore API key
            if original_key:
                os.environ["OPENAI_API_KEY"] = original_key


# Performance and stress tests
class TestWorkflowPerformance:
    """Performance tests for the workflow."""
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="No OpenAI API key")
    @pytest.mark.slow
    async def test_multiple_queries_sequentially(self):
        """Test processing multiple queries in sequence."""
        queries = [
            "How many schools are there?",
            "What schools are in Alameda County?",
            "What is the average SAT score?"
        ]
        
        workflow = TextToSQLWorkflow(
            data_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird",
            tables_json_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird/dev_tables.json",
            dataset_name="bird"
        )
        
        # Initialize database once
        await workflow.initialize_database("california_schools")
        
        results_list = []
        for i, query in enumerate(queries):
            results = await workflow.process_query(
                query=query,
                db_name="california_schools",
                task_id=f"perf_test_{i}",
                use_coordinator=False
            )
            results_list.append(results)
        
        # Check that all queries were processed
        assert len(results_list) == len(queries)
        for results in results_list:
            assert len(results["final_results"]) > 0


# Utility functions for testing
def check_sql_validity(sql: str) -> bool:
    """Check if SQL string looks valid."""
    if not sql:
        return False
    
    sql_upper = sql.upper().strip()
    
    # Must start with SELECT, WITH, or similar
    valid_starts = ["SELECT", "WITH"]
    if not any(sql_upper.startswith(start) for start in valid_starts):
        return False
    
    # Must contain FROM (for most queries)
    if "SELECT" in sql_upper and "FROM" not in sql_upper:
        return False
    
    return True


def check_execution_result_validity(exec_result: dict) -> bool:
    """Check if execution result looks valid."""
    if not exec_result:
        return False
    
    required_keys = ["rowCount"]
    for key in required_keys:
        if key not in exec_result:
            return False
    
    # Row count should be non-negative
    if exec_result["rowCount"] < 0:
        return False
    
    return True


# Test configuration
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )