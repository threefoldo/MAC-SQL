"""
Simple workflow tests that can be run without pytest.

This module contains basic tests for the text-to-SQL workflow that can be
run directly with Python for quick testing and validation.
"""

import os
import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator, run_text_to_sql


async def test_workflow_initialization():
    """Test that the workflow initializes correctly."""
    print("Testing workflow initialization...")
    
    try:
        workflow = TextToSQLTreeOrchestrator(
            data_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird",
            tables_json_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird/dev_tables.json",
            dataset_name="bird"
        )
        
        # Check components exist
        assert workflow.memory is not None
        assert workflow.query_analyzer is not None
        assert workflow.schema_linker is not None
        assert workflow.sql_generator is not None
        assert workflow.sql_evaluator is not None
        
        print("✓ Workflow initialization test passed")
        return True
        
    except Exception as e:
        print(f"✗ Workflow initialization test failed: {e}")
        return False


async def test_database_loading():
    """Test that database schema loads correctly."""
    print("Testing database loading...")
    
    try:
        workflow = TextToSQLTreeOrchestrator(
            data_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird",
            tables_json_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird/dev_tables.json",
            dataset_name="bird"
        )
        
        await workflow.initialize_database("california_schools")
        
        # Check schema was loaded
        summary = await workflow.schema_manager.get_schema_summary()
        assert summary['table_count'] > 0
        assert summary['total_columns'] > 0
        
        print(f"✓ Database loading test passed - {summary['table_count']} tables loaded")
        return True
        
    except Exception as e:
        print(f"✗ Database loading test failed: {e}")
        return False


async def test_simple_query():
    """Test processing a simple query."""
    if not os.getenv("OPENAI_API_KEY"):
        print("⚠ Skipping simple query test - no API key")
        return True
    
    print("Testing simple query processing...")
    
    try:
        query = "How many schools are there?"
        
        # Add timeout to prevent hanging
        results = await asyncio.wait_for(
            run_text_to_sql(
                query=query,
                db_name="california_schools",
                
                display_results=False
            ),
            timeout=300.0  # 300 second timeout
        )
        
        # Basic validation
        assert "final_result" in results
        assert results["final_result"] is not None
        
        print("✓ Simple query test passed")
        print(f"  Final result: {results['final_result']}")
        
        # Check if we have nodes with results
        if "nodes" in results and results["nodes"]:
            # Get the first node's results
            first_node = list(results["nodes"].values())[0]
            
            if first_node.get("sql"):
                print(f"  Generated SQL: {first_node['sql'].strip()}")
            
            if first_node.get("execution_result"):
                exec_result = first_node["execution_result"]
                print(f"  Execution: {exec_result.get('rowCount', 0)} rows")
                
                # Check if execution was successful
                if exec_result.get('error'):
                    print(f"  ⚠ Execution error: {exec_result['error']}")
            
            # Check evaluation results
            if first_node.get("analysis"):
                analysis = first_node["analysis"]
                answers_intent = analysis.get('answers_intent', 'unknown')
                result_quality = analysis.get('result_quality', 'unknown')
                print(f"  Evaluation: {answers_intent} intent, {result_quality} quality")
                
                # Validate evaluation
                if answers_intent in ['yes', 'partially'] and result_quality in ['excellent', 'good', 'acceptable']:
                    print("  🎉 Query answered successfully!")
                else:
                    print("  ⚠ Query may not be fully answered")
        
        return True
        
    except Exception as e:
        print(f"✗ Simple query test failed: {e}")
        import traceback
        traceback.print_exc()
        return False






async def run_all_tests():
    """Run all simple tests."""
    print("="*80)
    print("RUNNING SIMPLE WORKFLOW TESTS")
    print("="*80)
    
    # Check environment
    if os.getenv("OPENAI_API_KEY"):
        print("✓ OpenAI API key found")
    else:
        print("⚠ OpenAI API key not found - some tests will be skipped")
    
    print("-"*80)
    
    # Run tests
    tests = [
        ("Workflow Initialization", test_workflow_initialization),
        ("Database Loading", test_database_loading),
        ("Simple Query", test_simple_query),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n[{test_name}]")
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:<8} {test_name}")
    
    print("-"*80)
    print(f"TOTAL: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print("⚠ Some tests failed")
        return False


def main():
    """Main function for running tests."""
    try:
        success = asyncio.run(run_all_tests())
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        return 1
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
