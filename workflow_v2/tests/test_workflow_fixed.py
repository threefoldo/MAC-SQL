"""
Fixed integration tests for the text-to-SQL workflow.

This module contains working tests that handle real-world data issues.
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


class TestWorkflowFixed:
    """Fixed integration tests for the text-to-SQL workflow."""
    
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
    async def test_simple_query_success(self, workflow):
        """Test a simple query that should succeed."""
        db_name = "california_schools"
        query = "What is the highest eligible free rate for K-12 students in Alameda County?"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Basic verification
        assert "nodes" in results
        assert len(results["nodes"]) > 0
        
        # Check execution
        first_node = list(results["nodes"].values())[0]
        assert first_node["sql"] is not None
        assert first_node["execution_result"] is not None
        
        # Print results for debugging
        print(f"Results: {results}")
        print(f"Tree complete: {results.get('tree_complete')}")
        print(f"Final result: {results.get('final_result')}")
        
        # This query should complete successfully
        assert results["tree_complete"] == True
        assert results["final_result"] is not None
    
    @pytest.mark.asyncio
    async def test_count_query(self, workflow):
        """Test a count query."""
        db_name = "california_schools"
        query = "How many schools are there in total?"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Verify SQL generation
        first_node = list(results["nodes"].values())[0]
        assert "COUNT" in first_node["sql"].upper()
        assert first_node["execution_result"]["rowCount"] == 1
    
    @pytest.mark.asyncio
    async def test_query_with_filtering(self, workflow):
        """Test a query with WHERE clause."""
        db_name = "california_schools"
        query = "List all magnet schools"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Verify filtering
        first_node = list(results["nodes"].values())[0]
        sql = first_node["sql"].upper()
        assert "WHERE" in sql or "MAGNET" in sql
    
    @pytest.mark.asyncio
    async def test_aggregation_query(self, workflow):
        """Test an aggregation query."""
        db_name = "california_schools"
        query = "What is the average SAT math score?"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Verify aggregation
        first_node = list(results["nodes"].values())[0]
        assert "AVG" in first_node["sql"].upper()
    
    @pytest.mark.asyncio
    async def test_workflow_with_poor_quality(self, workflow):
        """Test workflow behavior when SQL quality is poor."""
        db_name = "california_schools"
        # This query might result in poor quality due to data issues
        query = "Show school names with SAT test takers where school name is available"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Workflow should execute even if quality is poor
        assert "nodes" in results
        first_node = list(results["nodes"].values())[0]
        
        # Check analysis exists
        if first_node.get("analysis"):
            quality = first_node["analysis"].get("result_quality", "").lower()
            logging.info(f"Query quality: {quality}")
            
            # Workflow complete only if quality is good
            if quality in ["excellent", "good"]:
                assert results["tree_complete"] == True
            else:
                assert results["tree_complete"] == False
    
    @pytest.mark.asyncio
    async def test_complex_query_handling(self, workflow):
        """Test how workflow handles potentially complex queries."""
        db_name = "california_schools"
        query = "What are the top 5 schools by SAT math scores and their counties?"
        
        await workflow.initialize_database(db_name)
        results = await workflow.process_query(
            query=query,
            db_name=db_name,
            
        )
        
        # Check tree structure
        tree = results["query_tree"]
        node_count = len(tree["nodes"])
        
        # Log complexity handling
        if node_count > 1:
            logging.info(f"Query decomposed into {node_count} nodes")
        else:
            logging.info("Query handled as single node")
        
        # Verify at least one node was processed
        assert node_count >= 1


# Main test runner
if __name__ == "__main__":
    async def run_tests():
        """Run all tests with detailed output."""
        workflow = TextToSQLTreeOrchestrator(
            data_path=DATA_PATH,
            tables_json_path=TABLES_JSON_PATH,
            dataset_name="bird"
        )
        
        test_cases = [
            ("What is the highest eligible free rate for K-12 students in Alameda County?", "california_schools"),
            ("How many schools are there in total?", "california_schools"),
            ("What is the average SAT math score?", "california_schools"),
            ("List the top 5 schools by enrollment", "california_schools"),
        ]
        
        for i, (query, db_name) in enumerate(test_cases):
            print(f"\n{'='*60}")
            print(f"TEST {i+1}: {query}")
            print('='*60)
            
            try:
                await workflow.initialize_database(db_name)
                results = await workflow.process_query(
                    query=query,
                    db_name=db_name,
                    
                )
                
                # Display results
                await workflow.display_query_tree()
                print(f"\nWorkflow complete: {results['tree_complete']}")
                print(f"Nodes processed: {len(results['nodes'])}")
                
                if results['tree_complete']:
                    print(f"Final results: {len(results['final_results'])}")
                    
            except Exception as e:
                print(f"Error: {e}")
                import traceback
                traceback.print_exc()
            
            # Clear memory for next test
            await workflow.memory.clear()
    
    asyncio.run(run_tests())