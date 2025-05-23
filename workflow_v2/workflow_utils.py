"""
Workflow utility functions for text-to-SQL processing.

This module contains functions specifically for the text-to-SQL workflow,
including processing, parsing, and execution utilities.
"""

import json
import re
import time
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Union

from utils import parse_json  # Import from the main utils module


def extract_sql_from_text(text: str) -> str:
    """
    Extract SQL query from text.
    
    Args:
        text: Text that might contain SQL
        
    Returns:
        Extracted SQL query or empty string if no SQL found
    """
    try:
        # Try to extract SQL from JSON
        data = parse_json(text)
        if 'sql' in data:
            return data['sql']
        if 'final_sql' in data:
            return data['final_sql']
            
        # Try to extract SQL with regex patterns
        sql_patterns = [
            r'```sql\s*(.*?)\s*```',  # SQL in code blocks
            r'```\s*SELECT.*?```',    # SELECT in generic code blocks
            r'SELECT.*?(?:;|$)',      # Simple SELECT statements
            r'WITH.*?(?:;|$)',        # WITH queries
        ]
        
        for pattern in sql_patterns:
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            if matches:
                # Clean up the matched SQL
                sql = matches[0].strip()
                # Remove any trailing backticks or spaces
                if sql.endswith('```'):
                    sql = sql[:sql.rfind('```')].strip()
                return sql
        
        # If no clear SQL pattern, look for any content between backticks
        code_block_pattern = r'```(.*?)```'
        code_blocks = re.findall(code_block_pattern, text, re.DOTALL)
        for block in code_blocks:
            if 'SELECT' in block.upper() or 'WITH' in block.upper():
                return block.strip()
                
        return ""
    except Exception as e:
        print(f"Error extracting SQL: {str(e)}")
        return ""


def format_sql(sql: str, max_length: int = 200) -> str:
    """
    Formats SQL query for display, with truncation if needed.
    
    Args:
        sql: SQL query to format
        max_length: Maximum length before truncation
        
    Returns:
        Formatted SQL string
    """
    if not sql:
        return "No SQL found"
    
    if len(sql) <= max_length:
        return sql
    else:
        return sql[:max_length] + "..."


def format_json_result(json_str: str) -> str:
    """
    Attempts to parse and format JSON result for better display.
    
    Args:
        json_str: JSON string to format
        
    Returns:
        Formatted result string
    """
    try:
        data = parse_json(json_str)
        result = ""
        
        if "status" in data:
            result += f"Status: {data['status']}\n\n"
            
        if "final_sql" in data:
            result += f"SQL: {format_sql(data['final_sql'])}\n\n"
            
        if "execution_result" in data:
            exec_result = data["execution_result"]
            if isinstance(exec_result, str) and len(exec_result) > 200:
                exec_result = exec_result[:200] + "..."
            result += f"Result: {exec_result}\n"
            
        return result
    except Exception:
        # Return truncated raw string if parsing fails
        return json_str[:200] + "..." if len(json_str) > 200 else json_str


# --- Text-to-SQL Workflow Steps ---

async def select_schema(selector_agent, task_json: str, timeout: int = 120):
    """
    First workflow step: Schema selection with selector_agent
    
    Args:
        selector_agent: The schema selector agent
        task_json: JSON string containing the task information
        timeout: Maximum time to wait for response in seconds
        
    Returns:
        Tuple of (task dict, selector response content)
    """
    # Create cancellation token
    from autogen_core import CancellationToken
    from autogen_agentchat.messages import TextMessage
    
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


async def generate_sql(decomposer_agent, selector_content: str, task: dict, timeout: int = 120):
    """
    Second workflow step: SQL generation with decomposer_agent
    
    Args:
        decomposer_agent: The SQL decomposer agent
        selector_content: Output from the schema selection step
        task: Task dictionary containing query information
        timeout: Maximum time to wait for response in seconds
        
    Returns:
        Tuple of (decomposer content, extracted SQL)
    """
    # Create cancellation token
    from autogen_core import CancellationToken
    from autogen_agentchat.messages import TextMessage
    from const import SELECTOR_NAME
    
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
    
    # Create proper message object with a source name that matches the selector agent's name
    selector_message = TextMessage(content=selector_content, source=SELECTOR_NAME)
    
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


async def refine_sql(refiner_agent, decomposer_content: str, sql: str, task: dict, 
                    max_refinement_attempts: int = 3,
                    timeout: int = 120):
    """
    Third workflow step: SQL refinement with refiner_agent
    
    Args:
        refiner_agent: The SQL refiner agent
        decomposer_content: Output from the SQL generation step
        sql: Initial SQL query extracted from decomposer_content
        task: Task dictionary containing query information
        max_refinement_attempts: Maximum number of refinement iterations
        timeout: Maximum time to wait for each response in seconds
        
    Returns:
        Refinement results dictionary
    """
    # Create cancellation token
    from autogen_core import CancellationToken
    from autogen_agentchat.messages import TextMessage
    from const import DECOMPOSER_NAME, REFINER_NAME
    
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
    
    while refinement_attempts < max_refinement_attempts:
        attempt_number = refinement_attempts + 1
        print(f"[Step 3] Starting refinement attempt {attempt_number}/{max_refinement_attempts}")
        
        # Create cancellation token with timeout
        cancellation_token = CancellationToken()
        
        # Create proper message object with the appropriate source
        message = TextMessage(
            content=refiner_content, 
            source=DECOMPOSER_NAME if refinement_attempts == 0 else REFINER_NAME
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


async def process_text_to_sql(
    selector_agent, 
    decomposer_agent, 
    refiner_agent, 
    task_json: str,
    max_refinement_attempts: int = 3,
    timeout: int = 120
) -> Dict:
    """
    Process a text-to-SQL task through the complete pipeline.
    
    Args:
        selector_agent: The schema selector agent
        decomposer_agent: The SQL decomposer agent  
        refiner_agent: The SQL refiner agent
        task_json: JSON string with task details
        max_refinement_attempts: Maximum refinement iterations
        timeout: Timeout in seconds for each step
        
    Returns:
        Dictionary with the final result
    """
    try:
        # Step 1: Schema Selection
        task, selector_content = await select_schema(
            selector_agent=selector_agent,
            task_json=task_json,
            timeout=timeout
        )
        
        # Step 2: SQL Generation
        decomposer_content, sql = await generate_sql(
            decomposer_agent=decomposer_agent,
            selector_content=selector_content,
            task=task,
            timeout=timeout
        )
        
        # Step 3: SQL Refinement
        result = await refine_sql(
            refiner_agent=refiner_agent,
            decomposer_content=decomposer_content,
            sql=sql,
            task=task,
            max_refinement_attempts=max_refinement_attempts,
            timeout=timeout
        )
        
        return result
        
    except Exception as e:
        print(f"Error in text-to-SQL processing: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        # Return error information
        return {
            "error": str(e),
            "db_id": task.get('db_id', '') if 'task' in locals() else "",
            "query": task.get('query', '') if 'task' in locals() else "",
            "evidence": task.get('evidence', '') if 'task' in locals() else "",
            "status": "ERROR"
        }


# --- Display and visualization functions ---

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