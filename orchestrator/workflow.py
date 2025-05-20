"""
Workflow functions for the Text-to-SQL orchestrator.
These functions handle the three main steps of the Text-to-SQL pipeline:
1. Schema Selection
2. SQL Generation
3. SQL Refinement
"""
import asyncio
import json
import time
import re
from typing import Dict, Any, Tuple, List, Optional

from autogen_core import CancellationToken
from autogen_agentchat.messages import TextMessage

from utils import parse_json, extract_sql_from_text


# Default timeout for each step
DEFAULT_TIMEOUT = 120  # seconds
MAX_REFINEMENT_ATTEMPTS = 3  # Maximum number of refinement attempts for SQL


async def select_schema(task_json: str, selector_agent, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Dict[str, Any], str]:
    """
    First workflow step: Schema selection with selector_agent
    
    Args:
        task_json: JSON string containing the task information
        selector_agent: The agent responsible for schema selection
        timeout: Maximum time to wait for response in seconds
        
    Returns:
        Tuple of (task dict, selector response content)
    """
    # Create cancellation token with timeout
    cancellation_token = CancellationToken()
    
    # Parse task
    try:
        task = json.loads(task_json)
    except json.JSONDecodeError as e:
        print(f"[Step 1] Error: Invalid JSON input: {str(e)}")
        raise ValueError(f"Invalid task JSON: {str(e)}")
        
    db_id = task.get('db_id', '')
    query = task.get('query', '')
    evidence = task.get('evidence', '')
    
    if not db_id or not query:
        raise ValueError("Task must contain db_id and query")
        
    print(f"[Step 1] Starting schema selection for database '{db_id}'")
    print(f"[Step 1] Query: {query}")
    print(f"[Step 1] Evidence: {evidence}")
    
    # Ensure task has the expected format for selector_agent
    task_content = task_json
    # If task_json isn't properly formatted, create a well-structured message
    if not '"db_id"' in task_json or not task_json.strip().startswith('{'):
        task_content = json.dumps({
            "db_id": db_id,
            "query": query,
            "evidence": evidence
        })
    
    # Create proper message object
    user_message = TextMessage(content=task_content, source="user")
    
    # Execute schema selection with timeout
    start_time = time.time()
    try:
        print(f"[Step 1] Requesting schema selection (timeout: {timeout}s)")
        selector_response = await selector_agent.on_messages([user_message], cancellation_token)
        selector_content = selector_response.chat_message.content.strip()
        
        # Verify selector output contains schema information
        if "<database_schema>" not in selector_content and "schema_str" not in selector_content:
            print(f"[Step 1] Warning: Selector response may not contain valid schema information")
            
        elapsed = time.time() - start_time
        print(f"[Step 1] Schema selected successfully (took {elapsed:.1f}s)")
        return task, selector_content
    except asyncio.TimeoutError:
        print(f"[Step 1] Error: Schema selection timed out after {timeout}s")
        raise
    except Exception as e:
        print(f"[Step 1] Error in schema selection: {str(e)}")
        raise


async def generate_sql(selector_content: str, task: Dict[str, Any], decomposer_agent, 
                       timeout: int = DEFAULT_TIMEOUT) -> Tuple[str, str]:
    """
    Second workflow step: SQL generation with decomposer_agent
    
    Args:
        selector_content: Output from the schema selection step
        task: Task dictionary containing query information
        decomposer_agent: The agent responsible for SQL generation
        timeout: Maximum time to wait for response in seconds
        
    Returns:
        Tuple of (decomposer content, extracted SQL)
    """
    # Create cancellation token with timeout
    cancellation_token = CancellationToken()
    
    print(f"\n[Step 2] Starting SQL generation")
    print(f"[Step 2] Query: {task.get('query', '')}")
    
    # Extract schema from selector content if possible
    schema_str = ""
    try:
        # Try to extract schema from JSON format
        data = parse_json(selector_content)
        if 'schema_str' in data:
            schema_str = data['schema_str']
            print(f"[Step 2] Found schema information in JSON format")
    except Exception:
        # If JSON parsing fails, try to extract schema directly
        schema_match = re.search(r'<database_schema>.*?</database_schema>', selector_content, re.DOTALL)
        if schema_match:
            schema_str = schema_match.group()
            print(f"[Step 2] Found schema information in XML format")
    
    if not schema_str:
        print(f"[Step 2] Warning: Could not extract schema from selector output")
    
    # Ensure selector_content is properly formatted for the decomposer agent
    if not schema_str and "<database_schema>" not in selector_content:
        print(f"[Step 2] Warning: Reformatting selector content to ensure schema is included")
        # Try to reformat it as a proper JSON
        try:
            data = parse_json(selector_content)
            if not 'schema_str' in data:
                # If we parsed JSON but it doesn't have schema_str, try to extract it directly
                schema_match = re.search(r'<database_schema>.*?</database_schema>', selector_content, re.DOTALL)
                if schema_match:
                    data['schema_str'] = schema_match.group()
                    selector_content = json.dumps(data)
        except Exception:
            # If all fails, just use the original content
            pass
    
    # Get the agent's name to use as source
    selector_name = getattr(selector_agent, 'name', 'schema_selector')
    
    # Create proper message object
    selector_message = TextMessage(content=selector_content, source=selector_name)
    
    # Execute SQL generation with timeout
    start_time = time.time()
    try:
        print(f"[Step 2] Requesting SQL generation (timeout: {timeout}s)")
        decomposer_response = await decomposer_agent.on_messages([selector_message], cancellation_token)
        decomposer_content = decomposer_response.chat_message.content.strip()
        
        # Extract SQL from decomposer output
        sql = extract_sql_from_text(decomposer_content)
        
        elapsed = time.time() - start_time
        print(f"[Step 2] SQL generation completed (took {elapsed:.1f}s)")
        
        if not sql:
            print(f"[Step 2] Warning: No SQL found in decomposer output")
        else:
            print(f"[Step 2] SQL generated: {sql[:100]}...")
            
        return decomposer_content, sql
    except asyncio.TimeoutError:
        print(f"[Step 2] Error: SQL generation timed out after {timeout}s")
        raise
    except Exception as e:
        print(f"[Step 2] Error in SQL generation: {str(e)}")
        raise


async def refine_sql(decomposer_content: str, sql: str, task: Dict[str, Any], refiner_agent,
                     max_refinement_attempts: int = MAX_REFINEMENT_ATTEMPTS,
                     timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Third workflow step: SQL refinement with refiner_agent
    
    Args:
        decomposer_content: Output from the SQL generation step
        sql: Initial SQL query extracted from decomposer_content
        task: Task dictionary containing query information
        refiner_agent: The agent responsible for SQL refinement
        max_refinement_attempts: Maximum number of refinement iterations
        timeout: Maximum time to wait for each response in seconds
        
    Returns:
        Refinement results dictionary
    """
    # Handle case where no SQL was generated
    if not sql:
        print(f"[Step 3] No SQL to refine, skipping refinement step")
        return {
            "db_id": task.get('db_id', ''),
            "query": task.get('query', ''),
            "evidence": task.get('evidence', ''),
            "final_output": decomposer_content,
            "error": "No SQL generated",
            "refinement_attempts": 0,
            "status": "ERROR_NO_SQL"
        }
    
    print(f"\n[Step 3] Starting SQL refinement")
    print(f"[Step 3] Initial SQL: {sql[:100]}...")
    
    # Extract schema information from decomposer content or selector content
    schema_info = ""
    try:
        # Look for schema in decomposer content
        schema_match = re.search(r'<database_schema>.*?</database_schema>', decomposer_content, re.DOTALL)
        if schema_match:
            schema_info = schema_match.group()
        
        # If not found, try parsing JSON
        if not schema_info:
            data = parse_json(decomposer_content)
            if 'schema_str' in data:
                schema_info = data['schema_str']
    except Exception as e:
        print(f"[Step 3] Warning: Could not extract schema information: {str(e)}")
    
    # Create a structured input for the refiner with all necessary context
    refiner_input = {
        "db_id": task.get('db_id', ''),
        "query": task.get('query', ''),
        "evidence": task.get('evidence', ''),
        "sql": sql,
        "schema_info": schema_info,  # Include schema information when available
        "instructions": "Please execute this SQL against the database and return the results in a structured JSON format with the fields: status, final_sql, and execution_result."
    }
    
    refiner_content = json.dumps(refiner_input)
    last_sql = sql
    refinement_attempts = 0
    
    # Get agent names to use as sources
    decomposer_name = getattr(decomposer_agent, 'name', 'decomposer')
    refiner_name = getattr(refiner_agent, 'name', 'refiner')
    
    while refinement_attempts < max_refinement_attempts:
        attempt_number = refinement_attempts + 1
        print(f"[Step 3] Starting refinement attempt {attempt_number}/{max_refinement_attempts}")
        
        # Create cancellation token with timeout
        cancellation_token = CancellationToken()
        
        # Create proper message object with the appropriate source
        message = TextMessage(
            content=refiner_content, 
            source=decomposer_name if refinement_attempts == 0 else refiner_name
        )
        
        # Execute refinement with timeout
        start_time = time.time()
        try:
            print(f"[Step 3] Requesting refinement (timeout: {timeout}s)")
            refiner_response = await refiner_agent.on_messages([message], cancellation_token)
            refiner_content = refiner_response.chat_message.content.strip()
            
            elapsed = time.time() - start_time
            print(f"[Step 3] Refinement response received (took {elapsed:.1f}s)")
            
            # Parse refiner output (with fallback)
            data = {}
            try:
                data = parse_json(refiner_content)
            except Exception as e:
                print(f"[Step 3] Warning: Could not parse refiner response as JSON: {str(e)}")
                # Create a simple data structure if parsing failed
                data = {"error": str(e)}
            
            status = data.get('status', '')
            print(f"[Step 3] Refinement attempt {attempt_number}, status: {status}")
            
            # Check for termination conditions
            if status in ['EXECUTION_SUCCESSFUL', 'NO_CHANGE_NEEDED', 'EXECUTION_CONFIRMED']:
                print(f"[Step 3] SQL execution successful: {status}")
                break
                
            # Extract the refined SQL
            new_sql = extract_sql_from_text(refiner_content)
            
            if new_sql:
                if new_sql == last_sql:
                    # SQL didn't change
                    print(f"[Step 3] SQL unchanged in attempt {attempt_number}")
                else:
                    # SQL changed
                    print(f"[Step 3] New SQL detected: {new_sql[:100]}...")
                    last_sql = new_sql
            else:
                # No SQL found in response, force a better input format for next attempt
                print(f"[Step 3] No SQL found in refinement output, attempt {attempt_number}")
                
                refiner_input = {
                    "db_id": task.get('db_id', ''),
                    "query": task.get('query', ''),
                    "evidence": task.get('evidence', ''),
                    "sql": last_sql,
                    "schema_info": schema_info,
                    "refiner_instructions": "Please execute this SQL query against the database and provide the following in your response as a JSON object:\n - status: 'EXECUTION_SUCCESSFUL', 'REFINEMENT_NEEDED', or 'NO_CHANGE_NEEDED'\n - final_sql: The final SQL query (same as input if no changes needed)\n - execution_result: The result of executing the query",
                    "attempt": attempt_number
                }
                refiner_content = json.dumps(refiner_input)
                
            # Increment refinement attempts and check if max reached
            refinement_attempts += 1
            if refinement_attempts >= max_refinement_attempts:
                print(f"[Step 3] Max refinements ({max_refinement_attempts}) reached")
                break
                
        except asyncio.TimeoutError:
            print(f"[Step 3] Error: Refinement attempt {attempt_number} timed out after {timeout}s")
            refinement_attempts += 1
            
            # Try a simplified input for next attempt
            refiner_input = {
                "db_id": task.get('db_id', ''),
                "query": task.get('query', ''),
                "sql": last_sql,
                "timeout_error": f"Previous attempt timed out after {timeout}s",
                "instructions": "Please execute this SQL against the database and return the results."
            }
            refiner_content = json.dumps(refiner_input)
            
        except Exception as e:
            print(f"[Step 3] Error in refinement attempt {attempt_number}: {str(e)}")
            refinement_attempts += 1
            
            # Try a simplified input for next attempt
            refiner_input = {
                "db_id": task.get('db_id', ''),
                "query": task.get('query', ''),
                "sql": last_sql,
                "error": str(e),
                "instructions": "Please execute this SQL against the database and return the results."
            }
            refiner_content = json.dumps(refiner_input)
    
    # Extract final SQL and status from refiner output 
    final_sql = extract_sql_from_text(refiner_content) or last_sql
    
    # Prepare final result with fallback
    try:
        data = parse_json(refiner_content)
        status = data.get('status', 'UNKNOWN')
        
        # If data doesn't have a final_sql field but we extracted one, add it
        if final_sql and not data.get('final_sql'):
            data['final_sql'] = final_sql
            refiner_content = json.dumps(data)
    except Exception as e:
        print(f"[Step 3] Error parsing final result: {str(e)}")
        # Create a minimal result if parsing failed
        data = {
            "status": "ERROR_PARSING_RESULT",
            "final_sql": final_sql,
            "error": str(e)
        }
        refiner_content = json.dumps(data)
        status = "ERROR_PARSING_RESULT"
    
    print(f"[Step 3] Refinement complete - Final status: {status}")
    if final_sql:
        print(f"[Step 3] Final SQL: {final_sql[:100]}...")
    
    return {
        "db_id": task.get('db_id', ''),
        "query": task.get('query', ''),
        "evidence": task.get('evidence', ''),
        "final_output": refiner_content,
        "refinement_attempts": refinement_attempts,
        "status": status,
        "final_sql": final_sql
    }


async def execute_text_to_sql_pipeline(task_json: str, selector_agent, decomposer_agent, refiner_agent,
                                       timeout: int = DEFAULT_TIMEOUT,
                                       max_refinement_attempts: int = MAX_REFINEMENT_ATTEMPTS) -> Dict[str, Any]:
    """
    Execute the complete Text-to-SQL pipeline.
    
    Args:
        task_json: JSON string containing the task information
        selector_agent: The agent responsible for schema selection
        decomposer_agent: The agent responsible for SQL generation 
        refiner_agent: The agent responsible for SQL refinement
        timeout: Maximum time to wait for each step in seconds
        max_refinement_attempts: Maximum number of refinement iterations
        
    Returns:
        Dictionary with the final results
    """
    try:
        print("\n" + "="*60)
        print("TEXT-TO-SQL PIPELINE EXECUTION")
        print("="*60)
        
        # Step 1: Schema Selection
        print("\n" + "="*50)
        print("STEP 1: SCHEMA SELECTION")
        print("="*50)
        task, selector_content = await select_schema(task_json, selector_agent, timeout)
        
        print("\nSchema selection result summary:")
        print("-" * 40)
        # Verify if we got schema information
        if "<database_schema>" in selector_content or "schema_str" in selector_content:
            print("✓ Schema information successfully extracted")
        else:
            print("⚠ Schema information may be missing or malformed")
        # Print a preview of the content
        print("\nPreview:")
        preview = selector_content[:200] + "..." if len(selector_content) > 200 else selector_content
        print(preview)

        # Step 2: SQL Generation
        print("\n" + "="*50)
        print("STEP 2: SQL GENERATION")
        print("="*50)
        decomposer_content, sql = await generate_sql(selector_content, task, decomposer_agent, timeout)
        
        print("\nSQL generation result summary:")
        print("-" * 40)
        if sql:
            print(f"✓ SQL query generated ({len(sql)} chars)")
            print("\nSQL Query:")
            print(sql[:300] + "..." if len(sql) > 300 else sql)
        else:
            print("⚠ No SQL query was generated")
            print("\nDecomposer output preview:")
            print(decomposer_content[:200] + "..." if len(decomposer_content) > 200 else decomposer_content)

        # Step 3: SQL Refinement
        print("\n" + "="*50)
        print("STEP 3: SQL REFINEMENT")
        print("="*50)
        if not sql:
            print("\n⚠ Skipping refinement because no SQL was generated")
            result = {
                "db_id": task.get('db_id', ''),
                "query": task.get('query', ''),
                "final_output": decomposer_content,
                "error": "No SQL generated",
                "status": "ERROR_NO_SQL"
            }
        else:
            result = await refine_sql(
                decomposer_content, 
                sql, 
                task, 
                refiner_agent, 
                max_refinement_attempts,
                timeout
            )
        
        # Prepare final result
        final_result = {
            "db_id": task.get('db_id', ''),
            "query": task.get('query', ''),
            "evidence": task.get('evidence', ''),
            "selector_output": selector_content,
            "decomposer_output": decomposer_content,
            "final_output": result.get("final_output", ""),
            "status": result.get("status", "UNKNOWN"),
            "final_sql": result.get("final_sql", sql),
            "refinement_attempts": result.get("refinement_attempts", 0),
            "error": result.get("error", None)
        }
        
        return final_result
        
    except Exception as e:
        print(f"\nCRITICAL ERROR IN EXECUTION: {str(e)}")
        import traceback
        error_traceback = traceback.format_exc()
        print(error_traceback)
        
        return {
            "db_id": task.get('db_id', '') if 'task' in locals() else "",
            "query": task.get('query', '') if 'task' in locals() else "",
            "error": str(e),
            "error_traceback": error_traceback,
            "status": "CRITICAL_ERROR"
        }