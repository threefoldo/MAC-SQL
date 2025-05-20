"""
SQL utilities for the Text-to-SQL workflow.

This module contains helper functions and workflow components for the Text-to-SQL task.
It's designed to be used in Jupyter notebooks and interactive environments.

Note: For production use, prefer the TextToSQLProcessor class in text_to_sql_processor.py
"""

import json
import re
import time
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Union

# Import more comprehensive functionality from the processor module
from text_to_sql_processor import (
    parse_json, extract_sql_from_text, format_sql, format_json_result,
    select_schema, generate_sql, refine_sql, process_text_to_sql
)

# --- Notebook-specific utility functions for display and visualization ---

def display_task_info(task: Dict):
    """
    Print formatted task information for better notebook visibility.
    
    Args:
        task: Dictionary with task information
    """
    print(f"\n{'='*80}")
    print(f"DATABASE: {task.get('db_id', 'Unknown')}")
    print(f"QUERY: {task.get('query', 'No query')}")
    if task.get('evidence'):
        print(f"EVIDENCE: {task.get('evidence', '')}")
    print(f"{'='*80}\n")

def display_sql_result(result: Dict):
    """
    Print formatted SQL result for better notebook visibility.
    
    Args:
        result: Dictionary with SQL execution result
    """
    print(f"\n{'='*80}")
    print("EXECUTION RESULT")
    print(f"{'='*80}\n")
    
    # Display status if available
    if "status" in result:
        status = result["status"]
        status_symbol = "✅" if status in ["EXECUTION_SUCCESSFUL", "NO_CHANGE_NEEDED"] else "❌"
        print(f"{status_symbol} Status: {status}")
    
    # Display SQL if available
    if "final_sql" in result:
        print(f"\nSQL Query:")
        print(f"```sql\n{result['final_sql']}\n```")
    
    # Display execution result if available
    if "final_output" in result:
        try:
            output = parse_json(result["final_output"])
            if "execution_result" in output:
                print("\nQuery Output:")
                exec_result = output["execution_result"]
                if isinstance(exec_result, dict):
                    for key, value in exec_result.items():
                        print(f"  {key}: {value}")
                else:
                    print(f"  {exec_result}")
        except Exception:
            pass
    
    # Display error if present
    if "error" in result:
        print(f"\n❌ Error: {result['error']}")

# --- Step-by-step execution functions ---

async def run_text_to_sql_step_by_step(
    selector_agent, 
    decomposer_agent, 
    refiner_agent,
    current_test: Dict,
    timeout: int = 120,
    max_refinement_attempts: int = 3
):
    """
    Run the text-to-SQL process step by step for a test case, with detailed output.
    
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
        import traceback
        print(traceback.format_exc())
        return {"error": str(e)}

# --- Combined execution function ---

async def run_complete_text_to_sql(
    selector_agent, 
    decomposer_agent, 
    refiner_agent,
    current_test: Dict,
    timeout: int = 120,
    max_refinement_attempts: int = 3
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
        
        print("Processing... (this may take a minute)")
        
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
        import traceback
        print(traceback.format_exc())
        return {"error": str(e)}

# --- Batch test function ---

async def run_all_tests(
    selector_agent,
    decomposer_agent,
    refiner_agent,
    test_cases: List[Dict],
    timeout: int = 120,
    max_refinement_attempts: int = 3
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
    success_count = 0
    error_count = 0
    
    for i, test_case in enumerate(test_cases):
        print(f"\n\n{'='*80}")
        print(f"Test {i+1}/{len(test_cases)}: {test_case['db_id']} - {test_case['query']}")
        print(f"{'='*80}\n")
        
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
            
            # Track success/failure
            if "error" in result:
                error_count += 1
                print(f"❌ Test {i+1} failed with error: {result['error']}")
            else:
                status = result.get("status", "UNKNOWN")
                if status in ['EXECUTION_SUCCESSFUL', 'NO_CHANGE_NEEDED', 'EXECUTION_CONFIRMED']:
                    success_count += 1
                    print(f"✅ Test {i+1} succeeded with status: {status}")
                else:
                    print(f"⚠️ Test {i+1} completed with non-success status: {status}")
            
            # Display the SQL if available
            if "final_sql" in result:
                print(f"\nFinal SQL Query:")
                print(f"```sql\n{result['final_sql']}\n```")
            
            print(f"\n{'-'*40}")
            print(f"Test {i+1}/{len(test_cases)} completed")
            print(f"{'-'*40}\n")
            
        except Exception as e:
            print(f"❌ Error in test {i+1}: {str(e)}")
            results.append({"error": str(e)})
            error_count += 1
    
    # Print summary
    print(f"\n{'='*60}")
    print("BATCH TESTING SUMMARY")
    print(f"{'='*60}")
    print(f"Total tests: {len(test_cases)}")
    print(f"Successful: {success_count} ({success_count / len(test_cases):.1%})")
    print(f"Failed: {error_count} ({error_count / len(test_cases):.1%})")
    
    return results