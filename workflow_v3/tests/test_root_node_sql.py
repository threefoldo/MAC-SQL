"""
Test cases specifically for root node SQL generation in complex queries.

This module tests that the TextToSQLTreeOrchestrator properly generates SQL
for root nodes that combine results from child nodes.
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

from text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator
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


class TestRootNodeSQL:
    """Test cases for root node SQL generation."""
    
    @pytest.fixture
    async def workflow(self):
        """Create a workflow instance for testing."""
        workflow = TextToSQLTreeOrchestrator(
            data_path=DATA_PATH,
            tables_json_path=TABLES_JSON_PATH,
            dataset_name="bird",
            max_steps=150  # Increase for complex queries
        )
        # Initialize database
        await workflow.initialize_database(DB_NAME)
        yield workflow
        # Cleanup
        await workflow.memory.clear()
    
    @pytest.mark.asyncio
    async def test_complex_query_root_node_sql(self, workflow):
        """Test that complex queries generate SQL for the root node."""
        query = "Find the top 5 counties by average SAT scores, including the number of schools and average free lunch rate"
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME
        )
        
        # Get tree structure
        tree = results.get("query_tree", {})
        root_id = tree.get("rootId")
        assert root_id is not None, "Root node ID should exist"
        
        # Get root node data
        root_node = tree.get("nodes", {}).get(root_id)
        assert root_node is not None, "Root node should exist"
        
        # Check if this is a complex query (has children)
        if root_node.get("childIds") and len(root_node["childIds"]) > 0:
            print(f"\n✓ Complex query detected with {len(root_node['childIds'])} child nodes")
            
            # Verify all child nodes have SQL
            for child_id in root_node["childIds"]:
                child_node = tree["nodes"].get(child_id)
                assert child_node is not None, f"Child node {child_id} should exist"
                assert child_node.get("sql") is not None, f"Child node {child_id} should have SQL"
                print(f"  ✓ Child node {child_id[-8:]}: {child_node.get('intent', '')[:50]}...")
            
            # CRITICAL: Check that root node also has SQL
            assert root_node.get("sql") is not None, "Root node should have SQL that combines child results"
            print(f"\n✓ Root node has SQL: {root_node['sql'][:100]}...")
            
            # Verify root SQL references child results (CTEs or subqueries)
            root_sql = root_node.get("sql", "").upper()
            assert "JOIN" in root_sql or "WITH" in root_sql, "Root SQL should combine results using JOIN or CTE"
            
            # Check execution result
            assert root_node.get("executionResult") is not None, "Root node should have execution result"
            exec_result = root_node["executionResult"]
            assert exec_result.get("status") == "success", "Root node SQL should execute successfully"
            assert exec_result.get("rowCount", 0) > 0, "Root node should return results"
            
            print(f"✓ Root node execution: {exec_result.get('rowCount')} rows returned")
        else:
            print("\n✓ Simple query - only root node needs SQL")
            assert root_node.get("sql") is not None, "Root node should have SQL"
    
    @pytest.mark.asyncio
    async def test_multi_table_join_query(self, workflow):
        """Test that queries requiring joins generate proper root SQL."""
        query = "Show me schools with their SAT scores and free lunch rates"
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME
        )
        
        # Get root node
        tree = results.get("query_tree", {})
        root_id = tree.get("rootId")
        root_node = tree.get("nodes", {}).get(root_id)
        
        assert root_node is not None
        assert root_node.get("sql") is not None, "Root node should have SQL"
        
        # Check if SQL contains necessary joins
        root_sql = root_node.get("sql", "")
        if root_node.get("childIds"):
            # Complex query - check for combination logic
            print(f"\nComplex query with {len(root_node['childIds'])} children")
        else:
            # Simple query - check for direct joins
            print(f"\nSimple query SQL: {root_sql[:150]}...")
        
        # Verify execution
        exec_result = root_node.get("executionResult", {})
        if exec_result.get("error"):
            print(f"⚠️  SQL execution error: {exec_result['error']}")
        else:
            print(f"✓ SQL executed successfully: {exec_result.get('rowCount', 0)} rows")
    
    @pytest.mark.asyncio
    async def test_aggregation_query_root_sql(self, workflow):
        """Test aggregation queries generate proper root SQL."""
        query = "What is the average SAT score by county, showing top 3 counties?"
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME
        )
        
        # Get root node
        tree = results.get("query_tree", {})
        root_id = tree.get("rootId")
        root_node = tree.get("nodes", {}).get(root_id)
        
        assert root_node is not None
        assert root_node.get("sql") is not None, "Root node should have SQL"
        
        # Check SQL contains aggregation
        root_sql = root_node.get("sql", "").upper()
        assert "AVG" in root_sql or "GROUP BY" in root_sql, "Should contain aggregation"
        assert "ORDER BY" in root_sql, "Should have ordering for top results"
        assert "LIMIT" in root_sql, "Should limit to top 3"
        
        print(f"\n✓ Aggregation SQL generated: {root_node['sql'][:100]}...")
    
    @pytest.mark.asyncio 
    async def test_verify_tree_completion(self, workflow):
        """Test that tree is only marked complete when all nodes have SQL."""
        query = "Find counties with average SAT scores above 500 and show their school count"
        
        results = await workflow.process_query(
            query=query,
            db_name=DB_NAME
        )
        
        tree = results.get("query_tree", {})
        tree_complete = results.get("tree_complete", False)
        
        # If tree is marked complete, verify all nodes have SQL
        if tree_complete:
            print("\n✓ Tree marked as complete")
            for node_id, node in tree.get("nodes", {}).items():
                assert node.get("sql") is not None, f"Node {node_id} missing SQL but tree marked complete"
                assert node.get("executionResult") is not None, f"Node {node_id} not executed but tree marked complete"
                
                # Check quality
                analysis = results["nodes"][node_id].get("analysis")
                if analysis:
                    quality = analysis.get("result_quality", "").lower()
                    assert quality in ["excellent", "good"], f"Node {node_id} has {quality} quality but tree marked complete"
            print(f"✓ All {len(tree['nodes'])} nodes have SQL and good quality")
        else:
            print("\n⚠️  Tree not marked as complete")


@pytest.mark.asyncio
async def test_display_complex_query_tree():
    """Standalone test to visualize a complex query tree."""
    workflow = TextToSQLTreeOrchestrator(
        data_path=DATA_PATH,
        tables_json_path=TABLES_JSON_PATH,
        dataset_name="bird"
    )
    
    # Complex query that should create multiple nodes
    query = "Find the top 5 counties by average SAT scores, including the number of schools and average free lunch rate"
    
    print(f"\nTesting complex query: {query}")
    print("="*80)
    
    results = await workflow.process_query(
        query=query,
        db_name=DB_NAME
    )
    
    # Display the tree structure
    await workflow.display_query_tree()
    
    # Display final results
    await workflow.display_final_results()
    
    # Verify root node has SQL
    tree = results.get("query_tree", {})
    root_id = tree.get("rootId")
    root_node = tree.get("nodes", {}).get(root_id)
    
    if root_node and root_node.get("childIds"):
        print(f"\n{'='*60}")
        print("ROOT NODE SQL CHECK")
        print('='*60)
        print(f"Root has {len(root_node['childIds'])} children")
        if root_node.get("sql"):
            print(f"✅ Root node HAS SQL that combines child results")
            print(f"\nRoot SQL preview:")
            print(root_node['sql'][:500] + "..." if len(root_node['sql']) > 500 else root_node['sql'])
        else:
            print(f"❌ Root node is MISSING SQL!")
    
    return results


if __name__ == "__main__":
    # Run the visual test
    asyncio.run(test_display_complex_query_tree())