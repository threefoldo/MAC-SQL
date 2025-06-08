"""
Integration tests for the text-to-SQL workflow using BIRD dataset.

This module tests the complete workflow functionality without strict SQL matching.
"""

import asyncio
import pytest
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator

# Load environment variables
load_dotenv()

# Test configuration
DATA_PATH = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
TABLES_JSON_PATH = str(Path(DATA_PATH) / "dev_tables.json")

# Set up logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('autogen_core').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)


class TestWorkflowIntegration:
    """Integration tests for the text-to-SQL workflow."""
    
    @pytest.fixture
    async def workflow(self):
        """Create a workflow instance for testing."""
        workflow = TextToSQLTreeOrchestrator(
            data_path=DATA_PATH,
            tables_json_path=TABLES_JSON_PATH,
            dataset_name="bird"
        )
        yield workflow
        await workflow.memory.clear()
    
    @pytest.mark.asyncio
    async def test_simple_query_workflow(self, workflow):
        """Test workflow with a simple query."""
        db_name = "california_schools"
        query = "What is the highest eligible free rate for K-12 students in Alameda County?"
        
        # Initialize database
        await workflow.initialize_database(db_name)
        
        # Process query
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Verify basic workflow execution
        assert "nodes" in results
        assert len(results["nodes"]) > 0
        
        # Get first node
        first_node = list(results["nodes"].values())[0]
        assert first_node["sql"] is not None
        assert first_node["execution_result"] is not None
        
        # Check if we got good results (tree_complete is only True for good quality)
        if results["tree_complete"]:
            assert len(results["final_results"]) > 0
            first_result = results["final_results"][0]
            assert first_result["analysis"]["result_quality"] in ["excellent", "good"]
        else:
            # Log why workflow didn't complete
            logging.warning(f"Workflow incomplete - quality: {first_node.get('analysis', {}).get('result_quality', 'unknown')}")
    
    @pytest.mark.asyncio
    async def test_query_with_join(self, workflow):
        """Test workflow with a query requiring joins."""
        db_name = "california_schools"
        query = "List the school names with SAT test takers over 500 that are magnet schools"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Verify results
        assert "final_results" in results
        if results["final_results"]:
            first_result = results["final_results"][0]
            assert "JOIN" in first_result["sql"].upper()
            assert first_result["analysis"]["answers_intent"].upper() == "YES"
    
    @pytest.mark.asyncio
    async def test_complex_query_decomposition(self, workflow):
        """Test workflow with a complex query that might need decomposition."""
        db_name = "california_schools"
        query = "What are the top 3 counties by number of schools, and what is the average SAT score in each?"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Verify tree structure
        assert "query_tree" in results
        tree = results["query_tree"]
        assert "nodes" in tree
        
        # Check if query was decomposed (multiple nodes) or handled as single
        node_count = len(tree["nodes"])
        assert node_count >= 1
        
        # If decomposed, verify parent-child relationships
        if node_count > 1:
            assert "root" in tree
            root_node = tree["nodes"][tree["root"]]
            if "children" in root_node:
                assert len(root_node["children"]) > 0
    
    @pytest.mark.asyncio 
    async def test_coordinator_workflow(self, workflow):
        """Test workflow using the coordinator agent."""
        db_name = "california_schools"
        query = "How many schools are there in Los Angeles County?"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Verify results
        assert results is not None
        assert "nodes" in results
        
        # Check for successful execution
        if results.get("final_results"):
            first_result = results["final_results"][0]
            assert first_result["sql"] is not None
            assert "COUNT" in first_result["sql"].upper()
    
    @pytest.mark.asyncio
    async def test_error_handling(self, workflow):
        """Test workflow error handling with invalid database."""
        db_name = "non_existent_db"
        query = "SELECT * FROM test"
        
        with pytest.raises(Exception):
            await workflow.initialize_database(db_name)
    
    @pytest.mark.asyncio
    async def test_quality_assessment(self, workflow):
        """Test that quality assessment works correctly."""
        db_name = "california_schools"
        query = "What is the average SAT math score?"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Check analysis exists
        if results.get("final_results"):
            for result in results["final_results"]:
                assert "analysis" in result
                assert "result_quality" in result["analysis"]
                assert "answers_intent" in result["analysis"]
                assert "result_summary" in result["analysis"]


# Quick test runner
if __name__ == "__main__":
    async def run_tests():
        """Run a few quick tests."""
        workflow = TextToSQLTreeOrchestrator(
            data_path=DATA_PATH,
            tables_json_path=TABLES_JSON_PATH,
            dataset_name="bird"
        )
        
        # Test 1: Simple query
        print("\n" + "="*60)
        print("TEST 1: Simple Query")
        print("="*60)
        
        await workflow.initialize_database("california_schools")
        results = await workflow.process_query(
            query="What is the highest eligible free rate for K-12 students in Alameda County?",
            db_name="california_schools",
            
        )
        
        await workflow.display_query_tree()
        await workflow.display_final_results()
        
        print(f"\nWorkflow complete: {results['tree_complete']}")
        print(f"Final results count: {len(results['final_results'])}")
        
        # Test 2: Query with join
        print("\n" + "="*60)
        print("TEST 2: Query with Join")
        print("="*60)
        
        await workflow.memory.clear()
        results = await workflow.process_query(
            query="List school names with over 500 SAT test takers",
            db_name="california_schools",
            
        )
        
        if results.get("final_results"):
            print(f"Generated SQL: {results['final_results'][0]['sql']}")
            print(f"Row count: {results['final_results'][0]['execution_result']['rowCount']}")
    
    asyncio.run(run_tests())