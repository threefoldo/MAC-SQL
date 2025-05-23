"""
Execution helpers for text-to-SQL workflow.

This module contains runner functions for executing the text-to-SQL workflow
in different modes: step-by-step, combined, and batch.
"""

import json
import traceback
from typing import Dict, List, Any

from workflow_utils import (
    display_task_info, display_sql_result,
    select_schema, generate_sql, refine_sql, process_text_to_sql
)

# Default constants
DEFAULT_TIMEOUT = 120  # seconds
MAX_REFINEMENT_ATTEMPTS = 3  # Maximum number of refinement attempts for SQL

# Step-by-step execution
async def run_text_to_sql_step_by_step(
    selector_agent, 
    decomposer_agent, 
    refiner_agent,
    current_test: Dict,
    timeout: int = DEFAULT_TIMEOUT,
    max_refinement_attempts: int = MAX_REFINEMENT_ATTEMPTS
):
    """
    Run the text-to-SQL process step by step for a test case with detailed output.
    
    Args:
        selector_agent: The schema selector agent
        decomposer_agent: The SQL decomposer agent
        refiner_agent: The SQL refiner agent
        current_test: Dictionary with test case information
        timeout: Timeout in seconds for each step
        max_refinement_attempts: Maximum refinement iterations
        
    Returns:
        Dictionary with the final result
    """
    try:
        print("\n" + "="*60)
        print("STEP-BY-STEP TEXT-TO-SQL EXECUTION")
        print("="*60)
        
        # Display task info
        display_task_info(current_test)
        
        # Step 1: Schema Selection
        print("\n" + "="*50)
        print("STEP 1: SCHEMA SELECTION")
        print("="*50)
        task, selector_content = await select_schema(
            selector_agent=selector_agent,
            task_json=json.dumps(current_test),
            timeout=timeout
        )
        
        print("\nSchema selection result summary:")
        print("-" * 40)
        # Verify if we got schema information
        if "<database_schema>" in selector_content or "schema_str" in selector_content:
            print("✓ Schema information successfully extracted")
        else:
            print("⚠ Schema information may be missing or malformed")
        
        # Step 2: SQL Generation
        print("\n" + "="*50)
        print("STEP 2: SQL GENERATION")
        print("="*50)
        decomposer_content, sql = await generate_sql(
            decomposer_agent=decomposer_agent,
            selector_content=selector_content,
            task=task,
            timeout=timeout
        )
        
        print("\nSQL generation result summary:")
        print("-" * 40)
        if sql:
            print(f"✓ SQL query generated ({len(sql)} chars)")
            print("\nSQL Query:")
            print(sql[:300] + "..." if len(sql) > 300 else sql)
        else:
            print("⚠ No SQL query was generated")
        
        # Step 3: SQL Refinement
        print("\n" + "="*50)
        print("STEP 3: SQL REFINEMENT")
        print("="*50)
        result = await refine_sql(
            refiner_agent=refiner_agent,
            decomposer_content=decomposer_content,
            sql=sql,
            task=task,
            max_refinement_attempts=max_refinement_attempts,
            timeout=timeout
        )
        
        # Display final result
        display_sql_result(result)
        return result
            
    except Exception as e:
        print(f"\nERROR IN EXECUTION: {str(e)}")
        print(traceback.format_exc())
        return {"error": str(e)}

# Combined execution (more concise)
async def run_complete_text_to_sql(
    selector_agent, 
    decomposer_agent, 
    refiner_agent,
    current_test: Dict,
    timeout: int = DEFAULT_TIMEOUT,
    max_refinement_attempts: int = MAX_REFINEMENT_ATTEMPTS
):
    """
    Run the complete text-to-SQL process in a single function call with minimal output.
    
    Args:
        selector_agent: The schema selector agent
        decomposer_agent: The SQL decomposer agent
        refiner_agent: The SQL refiner agent
        current_test: Dictionary with test case information
        timeout: Timeout in seconds for each step
        max_refinement_attempts: Maximum refinement iterations
        
    Returns:
        Dictionary with the final result
    """
    try:
        print("\n" + "="*60)
        print("COMPLETE TEXT-TO-SQL EXECUTION")
        print("="*60)
        
        # Display task info
        display_task_info(current_test)
        
        print("Processing query... (this may take a minute)")
        
        # Execute the complete pipeline
        result = await process_text_to_sql(
            selector_agent=selector_agent,
            decomposer_agent=decomposer_agent,
            refiner_agent=refiner_agent,
            task_json=json.dumps(current_test),
            max_refinement_attempts=max_refinement_attempts,
            timeout=timeout
        )
        
        # Display the result
        display_sql_result(result)
        return result
        
    except Exception as e:
        print(f"\nERROR IN EXECUTION: {str(e)}")
        print(traceback.format_exc())
        return {"error": str(e)}

# Batch testing
async def run_all_tests(
    selector_agent, 
    decomposer_agent, 
    refiner_agent,
    test_cases: List[Dict],
    timeout: int = DEFAULT_TIMEOUT,
    max_refinement_attempts: int = MAX_REFINEMENT_ATTEMPTS
):
    """
    Run text-to-SQL process on multiple test cases and collect results.
    
    Args:
        selector_agent: The schema selector agent
        decomposer_agent: The SQL decomposer agent
        refiner_agent: The SQL refiner agent
        test_cases: List of test case dictionaries
        timeout: Timeout in seconds for each step
        max_refinement_attempts: Maximum refinement iterations
        
    Returns:
        List of result dictionaries
    """
    results = []
    for i, test_case in enumerate(test_cases):
        print(f"\n\n{'='*80}")
        print(f"Test {i+1}/{len(test_cases)}: {test_case['db_id']} - {test_case['query']}")
        print(f"{'='*80}\n")
        
        # Display current test case info
        display_task_info(test_case)
        
        # Run the test
        try:
            result = await process_text_to_sql(
                selector_agent=selector_agent,
                decomposer_agent=decomposer_agent,
                refiner_agent=refiner_agent,
                task_json=json.dumps(test_case),
                max_refinement_attempts=max_refinement_attempts,
                timeout=timeout
            )
            results.append(result)
            
            # Display result summary
            display_sql_result(result)
            
            print(f"\n{'-'*40}")
            print(f"Test {i+1}/{len(test_cases)} completed")
            print(f"{'-'*40}\n")
            
        except Exception as e:
            print(f"❌ Error in test {i+1}: {str(e)}")
            results.append({"error": str(e)})
    
    print("\nAll tests completed!")
    return results