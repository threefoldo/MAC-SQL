"""
Simple workflow tests that can be run without pytest.

This module contains basic tests for the text-to-SQL workflow that can be
run directly with Python for quick testing and validation.
"""

import os
import sys
import asyncio
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from text_to_sql_workflow import TextToSQLWorkflow, run_text_to_sql


async def test_workflow_initialization():
    """Test that the workflow initializes correctly."""
    print("Testing workflow initialization...")
    
    try:
        workflow = TextToSQLWorkflow(
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
        workflow = TextToSQLWorkflow(
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
        
        results = await run_text_to_sql(
            query=query,
            db_name="california_schools",
            use_coordinator=False,
            display_results=False
        )
        
        # Basic validation
        assert "final_results" in results
        assert len(results["final_results"]) > 0
        
        final_result = results["final_results"][0]
        assert final_result["sql"] is not None
        assert "SELECT" in final_result["sql"].upper()
        
        print("✓ Simple query test passed")
        print(f"  Generated SQL: {final_result['sql'].strip()}")
        
        if final_result.get("execution_result"):
            exec_result = final_result["execution_result"]
            print(f"  Execution: {exec_result.get('rowCount', 0)} rows")
            
            # Check if execution was successful
            if exec_result.get('error'):
                print(f"  ⚠ Execution error: {exec_result['error']}")
        
        # Check evaluation results
        if final_result.get("analysis"):
            analysis = final_result["analysis"]
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


async def test_workflow_components():
    """Test individual workflow components."""
    print("Testing workflow components...")
    
    try:
        workflow = TextToSQLWorkflow(
            data_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird",
            tables_json_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird/dev_tables.json",
            dataset_name="bird"
        )
        
        # Test display functions (should not crash)
        await workflow.display_query_tree()
        await workflow.display_final_results()
        
        # Test coordinator creation
        coordinator = workflow._create_coordinator()
        assert coordinator is not None
        assert len(coordinator.tools) == 4
        
        print("✓ Workflow components test passed")
        return True
        
    except Exception as e:
        print(f"✗ Workflow components test failed: {e}")
        return False


async def test_validation_functions():
    """Test the validation functions."""
    print("Testing validation functions...")
    
    try:
        workflow = TextToSQLWorkflow(
            data_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird",
            tables_json_path="/home/norman/work/text-to-sql/MAC-SQL/data/bird/dev_tables.json",
            dataset_name="bird"
        )
        
        # Test SQL validation
        good_sql = "SELECT COUNT(*) FROM schools WHERE County = 'Alameda'"
        bad_sql = "SELECT COUNT(*) WHERE"
        empty_sql = ""
        
        good_validation = workflow._validate_sql(good_sql)
        bad_validation = workflow._validate_sql(bad_sql)
        empty_validation = workflow._validate_sql(empty_sql)
        
        assert good_validation["is_valid"] == True
        assert bad_validation["is_valid"] == False
        assert empty_validation["is_valid"] == False
        
        print("  ✓ SQL validation works correctly")
        
        # Test evaluation validation
        from memory_content_types import ExecutionResult
        
        good_analysis = {
            "answers_intent": "yes",
            "result_quality": "good",
            "result_summary": "Query answered correctly",
            "confidence_score": 0.9
        }
        
        bad_analysis = {
            "answers_intent": "no",
            "result_quality": "poor"
        }
        
        exec_result = ExecutionResult(data=[[1392]], rowCount=1, error=None)
        
        good_eval = workflow._validate_evaluation(good_analysis, exec_result)
        bad_eval = workflow._validate_evaluation(bad_analysis, exec_result)
        none_eval = workflow._validate_evaluation(None, exec_result)
        
        assert good_eval["is_valid"] == True
        assert bad_eval["is_valid"] == True  # Structure is valid even if content indicates failure
        assert none_eval["is_valid"] == False
        
        print("  ✓ Evaluation validation works correctly")
        print("✓ Validation functions test passed")
        return True
        
    except Exception as e:
        print(f"✗ Validation functions test failed: {e}")
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
        ("Workflow Components", test_workflow_components),
        ("Validation Functions", test_validation_functions),
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
        print("🎉 All tests passed\!")
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
EOF < /dev/null
