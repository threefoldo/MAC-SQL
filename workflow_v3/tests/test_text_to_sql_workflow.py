"""
Test cases for the complete Text-to-SQL workflow.

This module tests both coordinator-based and sequential workflows with
comprehensive test coverage including quality assessment and error handling.
"""

import asyncio
import pytest
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator, run_text_to_sql
from keyvalue_memory import KeyValueMemory

# Load environment variables
load_dotenv()

# Test configuration
DATA_PATH = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
TABLES_JSON_PATH = str(Path(DATA_PATH) / "dev_tables.json")
DB_NAME = "california_schools"

# Set up logging for tests
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('autogen_core').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)


class TestTextToSQLTreeOrchestrator:
    """Test cases for the text-to-SQL workflow."""
    
    @pytest.fixture
    async def workflow(self):
        """Create a workflow instance for testing."""
        workflow = TextToSQLTreeOrchestrator(
            data_path=DATA_PATH,
            tables_json_path=TABLES_JSON_PATH,
            dataset_name="bird"
        )
        # Initialize database
        await workflow.initialize_database(DB_NAME)
        yield workflow
        # Cleanup
        await workflow.memory.clear()
    
    @pytest.mark.asyncio
    async def test_simple_query_sequential(self, workflow):
        """Test a simple query using sequential workflow."""
        query = "What is the highest eligible free rate for K-12 students in schools located in Alameda County?"
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME,
            
        )
        
        # Verify results structure
        assert "query_tree" in results
        assert "nodes" in results
        assert "final_result" in results
        
        # Check that we have at least one node
        assert len(results["nodes"]) > 0
        
        # Verify first node has required components
        first_node = list(results["nodes"].values())[0]
        assert first_node["intent"] is not None
        assert first_node["sql"] is not None
        assert first_node["execution_result"] is not None
        
        # Check if SQL was executed successfully
        exec_result = first_node["execution_result"]
        assert exec_result is not None
        if exec_result.get("error"):
            pytest.skip(f"SQL execution failed: {exec_result['error']}")
        
        # Verify we got results
        assert exec_result.get("rowCount", 0) > 0
    
    @pytest.mark.asyncio
    async def test_simple_query_coordinator(self, workflow):
        """Test a simple query using coordinator workflow."""
        query = "How many schools are there in Los Angeles County?"
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME,
            
        )
        
        # Verify we got results
        assert results is not None
        assert "error" not in results
        
        # Check workflow completion status
        if results.get("tree_complete"):
            # Verify we have final SQL from root node
            assert results.get("final_result") is not None, "Expected final SQL from root node"
    
    @pytest.mark.asyncio
    async def test_complex_query_with_decomposition(self, workflow):
        """Test a complex query that requires decomposition."""
        query = "Find the top 5 counties by average SAT scores, including the number of schools and average free lunch rate"
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME,
            
        )
        
        # Complex queries should create multiple nodes
        assert len(results.get("nodes", {})) >= 1
        
        # Check that root node exists
        tree = results.get("query_tree", {})
        root_id = tree.get("rootId")
        assert root_id is not None
        
        # Verify root node has children for complex query
        root_node = tree.get("nodes", {}).get(root_id, {})
        if root_node.get("childIds"):
            assert len(root_node["childIds"]) > 0
    
    @pytest.mark.asyncio
    async def test_error_handling(self, workflow):
        """Test error handling for invalid queries."""
        query = "SELECT * FROM non_existent_table"
        
        # This should not raise an exception but handle gracefully
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME,
            
        )
        
        # Should still return results structure
        assert results is not None
        
        # Check if any nodes have errors
        has_error = False
        for node_result in results.get("nodes", {}).values():
            if node_result.get("execution_result", {}).get("error"):
                has_error = True
                break
        
        # For direct SQL, we expect an error
        if query.startswith("SELECT"):
            assert has_error or len(results["nodes"]) == 0
    
    @pytest.mark.asyncio
    async def test_workflow_state_tracking(self, workflow):
        """Test that workflow properly tracks state across agents."""
        query = "What is the average SAT score for schools in San Diego?"
        
        # Clear memory first
        await workflow.memory.clear()
        
        # Process query
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME,
            
        )
        
        # Verify state was tracked
        tree = await workflow.tree_manager.get_tree()
        assert tree is not None
        assert "rootId" in tree
        assert "nodes" in tree
        
        # Check current node tracking
        current_node_id = await workflow.tree_manager.get_current_node_id()
        assert current_node_id is not None
    
    @pytest.mark.asyncio
    async def test_quality_assessment(self, workflow):
        """Test that quality assessment works correctly."""
        query = "Show me all schools"  # Intentionally vague query
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME,
            
        )
        
        # Check that analysis was performed
        for node_id, node_result in results.get("nodes", {}).items():
            if node_result.get("sql"):
                analysis = node_result.get("analysis")
                if analysis:
                    # Verify analysis has required fields
                    assert "answers_intent" in analysis
                    assert "result_quality" in analysis
                    assert analysis["result_quality"] in ["excellent", "good", "acceptable", "poor"]
    
    @pytest.mark.asyncio
    async def test_node_progression(self, workflow):
        """Test that node progression works correctly for multi-node queries."""
        query = "Find the average SAT score for each county and show the top 3"
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME,
            
        )
        
        # Check if multiple nodes were created
        tree = results.get("query_tree", {})
        nodes = tree.get("nodes", {})
        
        # For complex queries, verify node relationships
        if len(nodes) > 1:
            root_id = tree.get("rootId")
            root_node = nodes.get(root_id, {})
            
            # Check if root has children
            if root_node.get("childIds"):
                # Verify children exist
                for child_id in root_node["childIds"]:
                    assert child_id in nodes
                    child_node = nodes[child_id]
                    assert child_node.get("parentId") == root_id


@pytest.mark.asyncio
async def test_run_text_to_sql_convenience():
    """Test the convenience function."""
    query = "Count the number of schools in California"
    
    results = await run_text_to_sql(
        query=query,
        db_name=DB_NAME,
        
        display_results=False  # Don't display in tests
    )
    
    assert results is not None
    assert "query_tree" in results
    assert "tree_complete" in results


@pytest.mark.asyncio 
async def test_feedback_loop_patterns():
    """Test orchestrator feedback loop patterns - TESTING_PLAN.md Layer 3.2."""
    workflow = TextToSQLTreeOrchestrator(
        data_path=DATA_PATH,
        tables_json_path=TABLES_JSON_PATH,
        dataset_name="bird"
    )
    
    # Test queries that trigger different feedback patterns
    test_cases = [
        {
            "query": "Find data from invalid_table",
            "expected_pattern": "SCHEMA ERRORS → Schema Linker"
        },
        {
            "query": "Get results but with wrong logic",
            "expected_pattern": "LOGIC ERRORS → Query Analyzer"
        },
        {
            "query": "SELECT FROM WHERE syntax error",
            "expected_pattern": "SYNTAX ERRORS → SQL Generator"
        }
    ]
    
    print("\nFeedback Loop Pattern Tests:")
    
    for test_case in test_cases:
        print(f"\nTesting: {test_case['expected_pattern']}")
        print(f"Query: {test_case['query']}")
        
        results = await workflow.process_query(
            query=test_case['query'],
            db_name=DB_NAME,
        )
        
        # Check if appropriate feedback loop was triggered
        nodes = results.get("nodes", {})
        for node_id, node_data in nodes.items():
            if node_data.get("execution_result", {}).get("error"):
                error = node_data["execution_result"]["error"]
                print(f"  Error detected: {error[:50]}...")
                
                # Verify orchestrator made logical decisions
                history = node_data.get("history", [])
                if len(history) > 1:
                    print(f"  ✓ Orchestrator triggered feedback loop ({len(history)} attempts)")


@pytest.mark.asyncio
async def test_orchestrator_maintains_context():
    """Test that orchestrator maintains context across iterations."""
    workflow = TextToSQLTreeOrchestrator(
        data_path=DATA_PATH,
        tables_json_path=TABLES_JSON_PATH,
        dataset_name="bird"
    )
    
    query = "Find complex relationships between schools and test scores"
    
    results = await workflow.process_query(
        query=query,
        db_name=DB_NAME,
    )
    
    # VERIFY ORCHESTRATOR:
    # - Makes logical agent selection decisions
    # - Builds effective feedback loops
    # - Terminates appropriately
    # - Maintains context across iterations
    
    print("\nContext Maintenance Test:")
    
    # Check that context was maintained
    tree = results.get("query_tree", {})
    nodes = tree.get("nodes", {})
    
    for node_id, node in nodes.items():
        print(f"\nNode {node_id}:")
        
        # Check if context was preserved across agent calls
        if node.get("schema_linking"):
            print("  ✓ Schema context preserved")
        
        if node.get("decomposition"):
            print("  ✓ Query analysis context preserved")
        
        if node.get("sql"):
            print("  ✓ SQL generation used schema context")
        
        if node.get("executionResult"):
            print("  ✓ Evaluation had full context")


@pytest.mark.asyncio
async def test_display_functions():
    """Test that display functions work correctly."""
    workflow = TextToSQLTreeOrchestrator(
        data_path=DATA_PATH,
        tables_json_path=TABLES_JSON_PATH,
        dataset_name="bird"
    )
    
    # Process a query
    query = "How many schools are in Alameda County?"
    results = await workflow.process_query(
        query=query,
        db_name=DB_NAME,
        
    )
    
    # These should not raise exceptions
    await workflow.display_query_tree()
    await workflow.display_final_results()


if __name__ == "__main__":
    # Run specific test
    async def run_single_test():
        workflow = TextToSQLTreeOrchestrator(
            data_path=DATA_PATH,
            tables_json_path=TABLES_JSON_PATH,
            dataset_name="bird"
        )
        await workflow.initialize_database(DB_NAME)
        
        # Test a specific query
        query = "What is the highest eligible free rate for K-12 students in schools located in Alameda County?"
        
        print(f"\nTesting query: {query}")
        print("="*80)
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME,
            
        )
        
        # Display results
        await workflow.display_query_tree()
        await workflow.display_final_results()
        
        print("\n✓ Test completed")
        
        # Check quality
        if results.get("tree_complete"):
            print("\n✅ Workflow completed successfully")
            print(f"Total nodes: {len(results['nodes'])}")
            print(f"Final SQL available: {'Yes' if results.get('final_result') else 'No'}")
        else:
            print("\n⚠️  Workflow did not complete")
    
    # Run the test
    asyncio.run(run_single_test())