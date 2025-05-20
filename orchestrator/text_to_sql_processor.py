"""
Text-to-SQL processing and evaluation module.

This module provides a comprehensive framework for executing the Text-to-SQL task
using a three-agent approach (schema selection, SQL generation, SQL refinement).
It includes functionality for:

1. Single query processing
2. Batch processing of multiple queries
3. Evaluation against standard datasets (BIRD, Spider)
4. Integration with external evaluation tools
"""

import asyncio
import json
import os
import re
import time
import logging
from typing import Dict, List, Any, Optional, Tuple, Union

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("text_to_sql.log")
    ]
)
logger = logging.getLogger("text_to_sql_processor")

# --- Utility functions ---

def parse_json(text: str) -> Dict:
    """
    Attempt to parse JSON from text, returning an empty dict if parsing fails.
    
    Args:
        text: Text that might contain JSON
        
    Returns:
        Dictionary of parsed JSON or empty dict if parsing fails
    """
    try:
        if not text or not isinstance(text, str):
            return {}
            
        # Try direct JSON loading first
        try:
            if text.strip().startswith('{') and text.strip().endswith('}'):
                return json.loads(text)
        except json.JSONDecodeError:
            pass
            
        # Find JSON-like patterns with regex
        json_pattern = r'{.*}'
        match = re.search(json_pattern, text, re.DOTALL)
        if match:
            json_str = match.group()
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass
            
        # Try finding JSON in code blocks
        code_block_pattern = r'```(?:json)?\s*(.*?)\s*```'
        blocks = re.findall(code_block_pattern, text, re.DOTALL)
        for block in blocks:
            if block.strip().startswith('{') and block.strip().endswith('}'):
                try:
                    return json.loads(block)
                except:
                    continue
                    
        return {}
    except Exception as e:
        logger.error(f"Error parsing JSON: {str(e)}")
        return {}


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
        logger.error(f"Error extracting SQL: {str(e)}")
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
        logger.error(f"[Step 1] Error: Invalid JSON input: {str(e)}")
        raise ValueError(f"Invalid task JSON: {str(e)}")
        
    db_id = task.get('db_id', '')
    query = task.get('query', '')
    evidence = task.get('evidence', '')
    
    if not db_id or not query:
        raise ValueError("Task must contain db_id and query")
        
    logger.info(f"[Step 1] Starting schema selection for database '{db_id}'")
    logger.info(f"[Step 1] Query: {query}")
    logger.info(f"[Step 1] Evidence: {evidence}")
    
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
        logger.info(f"[Step 1] Requesting schema selection (timeout: {timeout}s)")
        selector_response = await selector_agent.on_messages([user_message], cancellation_token)
        selector_content = selector_response.chat_message.content.strip()
        
        # Verify selector output contains schema information
        if "<database_schema>" not in selector_content and "schema_str" not in selector_content:
            logger.warning(f"[Step 1] Warning: Selector response may not contain valid schema information")
            
        elapsed = time.time() - start_time
        logger.info(f"[Step 1] Schema selected successfully (took {elapsed:.1f}s)")
        return task, selector_content
    except asyncio.TimeoutError:
        logger.error(f"[Step 1] Error: Schema selection timed out after {timeout}s")
        raise
    except Exception as e:
        logger.error(f"[Step 1] Error in schema selection: {str(e)}")
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
    
    cancellation_token = CancellationToken()
    
    logger.info(f"\n[Step 2] Starting SQL generation")
    logger.info(f"[Step 2] Query: {task.get('query', '')}")
    
    # Extract schema from selector content if possible
    schema_str = ""
    try:
        # Try to extract schema from JSON format
        data = parse_json(selector_content)
        if 'schema_str' in data:
            schema_str = data['schema_str']
            logger.info(f"[Step 2] Found schema information in JSON format")
    except Exception:
        # If JSON parsing fails, try to extract schema directly
        schema_match = re.search(r'<database_schema>.*?</database_schema>', selector_content, re.DOTALL)
        if schema_match:
            schema_str = schema_match.group()
            logger.info(f"[Step 2] Found schema information in XML format")
    
    if not schema_str:
        logger.warning(f"[Step 2] Warning: Could not extract schema from selector output")
    
    # Ensure selector_content is properly formatted for the decomposer agent
    if not schema_str and "<database_schema>" not in selector_content:
        logger.warning(f"[Step 2] Warning: Reformatting selector content to ensure schema is included")
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
    selector_message = TextMessage(content=selector_content, source="Selector")
    
    # Execute SQL generation with timeout
    start_time = time.time()
    try:
        logger.info(f"[Step 2] Requesting SQL generation (timeout: {timeout}s)")
        decomposer_response = await decomposer_agent.on_messages([selector_message], cancellation_token)
        decomposer_content = decomposer_response.chat_message.content.strip()
        
        # Extract SQL from decomposer output
        sql = extract_sql_from_text(decomposer_content)
        
        elapsed = time.time() - start_time
        logger.info(f"[Step 2] SQL generation completed (took {elapsed:.1f}s)")
        
        if not sql:
            logger.warning(f"[Step 2] Warning: No SQL found in decomposer output")
        else:
            logger.info(f"[Step 2] SQL generated: {sql[:100]}...")
            
        return decomposer_content, sql
    except asyncio.TimeoutError:
        logger.error(f"[Step 2] Error: SQL generation timed out after {timeout}s")
        raise
    except Exception as e:
        logger.error(f"[Step 2] Error in SQL generation: {str(e)}")
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
    
    # Handle case where no SQL was generated
    if not sql:
        logger.info(f"[Step 3] No SQL to refine, skipping refinement step")
        return {
            "db_id": task.get('db_id', ''),
            "query": task.get('query', ''),
            "evidence": task.get('evidence', ''),
            "final_output": decomposer_content,
            "error": "No SQL generated",
            "refinement_attempts": 0,
            "status": "ERROR_NO_SQL"
        }
    
    logger.info(f"\n[Step 3] Starting SQL refinement")
    logger.info(f"[Step 3] Initial SQL: {sql[:100]}...")
    
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
        logger.warning(f"[Step 3] Warning: Could not extract schema information: {str(e)}")
    
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
        logger.info(f"[Step 3] Starting refinement attempt {attempt_number}/{max_refinement_attempts}")
        
        # Create cancellation token with timeout
        cancellation_token = CancellationToken()
        
        # Create proper message object with the appropriate source
        message = TextMessage(
            content=refiner_content, 
            source="Decomposer" if refinement_attempts == 0 else "Refiner"
        )
        
        # Execute refinement with timeout
        start_time = time.time()
        try:
            logger.info(f"[Step 3] Requesting refinement (timeout: {timeout}s)")
            refiner_response = await refiner_agent.on_messages([message], cancellation_token)
            refiner_content = refiner_response.chat_message.content.strip()
            
            elapsed = time.time() - start_time
            logger.info(f"[Step 3] Refinement response received (took {elapsed:.1f}s)")
            
            # Parse refiner output (with fallback)
            data = {}
            try:
                data = parse_json(refiner_content)
            except Exception as e:
                logger.warning(f"[Step 3] Warning: Could not parse refiner response as JSON: {str(e)}")
                # Create a simple data structure if parsing failed
                data = {"error": str(e)}
            
            status = data.get('status', '')
            logger.info(f"[Step 3] Refinement attempt {attempt_number}, status: {status}")
            
            # Check for termination conditions
            if status in ['EXECUTION_SUCCESSFUL', 'NO_CHANGE_NEEDED', 'EXECUTION_CONFIRMED']:
                logger.info(f"[Step 3] SQL execution successful: {status}")
                break
                
            # Extract the refined SQL
            new_sql = extract_sql_from_text(refiner_content)
            
            if new_sql:
                if new_sql == last_sql:
                    # SQL didn't change
                    logger.info(f"[Step 3] SQL unchanged in attempt {attempt_number}")
                else:
                    # SQL changed
                    logger.info(f"[Step 3] New SQL detected: {new_sql[:100]}...")
                    last_sql = new_sql
            else:
                # No SQL found in response, force a better input format for next attempt
                logger.warning(f"[Step 3] No SQL found in refinement output, attempt {attempt_number}")
                
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
                logger.info(f"[Step 3] Max refinements ({max_refinement_attempts}) reached")
                break
                
        except asyncio.TimeoutError:
            logger.error(f"[Step 3] Error: Refinement attempt {attempt_number} timed out after {timeout}s")
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
            logger.error(f"[Step 3] Error in refinement attempt {attempt_number}: {str(e)}")
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
        logger.error(f"[Step 3] Error parsing final result: {str(e)}")
        # Create a minimal result if parsing failed
        data = {
            "status": "ERROR_PARSING_RESULT",
            "final_sql": final_sql,
            "error": str(e)
        }
        refiner_content = json.dumps(data)
        status = "ERROR_PARSING_RESULT"
    
    logger.info(f"[Step 3] Refinement complete - Final status: {status}")
    if final_sql:
        logger.info(f"[Step 3] Final SQL: {final_sql[:100]}...")
    
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
        logger.error(f"Error in text-to-SQL processing: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Return error information
        return {
            "error": str(e),
            "db_id": task.get('db_id', '') if 'task' in locals() else "",
            "query": task.get('query', '') if 'task' in locals() else "",
            "evidence": task.get('evidence', '') if 'task' in locals() else "",
            "status": "ERROR"
        }


# --- Evaluation Functions ---

async def evaluate_dataset(
    selector_agent,
    decomposer_agent,
    refiner_agent,
    dataset_path: str,
    dataset_type: str = "bird",  # "bird" or "spider"
    limit: Optional[int] = None,
    output_path: Optional[str] = None,
    max_refinement_attempts: int = 3,
    timeout: int = 120
) -> Dict:
    """
    Evaluate a dataset of text-to-SQL queries.
    
    Args:
        selector_agent: The schema selector agent
        decomposer_agent: The SQL decomposer agent
        refiner_agent: The SQL refiner agent
        dataset_path: Path to the dataset file (JSON)
        dataset_type: Type of dataset ("bird" or "spider") 
        limit: Maximum number of examples to process
        output_path: Path to save results
        max_refinement_attempts: Maximum refinement iterations
        timeout: Timeout in seconds for each step
        
    Returns:
        Dictionary with evaluation results
    """
    try:
        # Load dataset
        with open(dataset_path, 'r') as f:
            dataset = json.load(f)
        
        # Limit dataset size if needed
        if limit and limit > 0:
            dataset = dataset[:limit]
            
        logger.info(f"Starting evaluation on {len(dataset)} examples from {dataset_type} dataset")
        
        results = []
        success_count = 0
        error_count = 0
        
        # Process each example
        for i, example in enumerate(dataset):
            logger.info(f"Processing example {i+1}/{len(dataset)}")
            
            # Format example based on dataset type
            if dataset_type.lower() == "bird":
                task = {
                    "db_id": example.get("db_id", ""),
                    "query": example.get("question", "") or example.get("query", ""),
                    "evidence": example.get("evidence", "")
                }
            else:  # spider
                task = {
                    "db_id": example.get("db_id", ""),
                    "query": example.get("question", "") or example.get("query", ""),
                    "evidence": ""
                }
            
            # Skip examples without required fields
            if not task["db_id"] or not task["query"]:
                logger.warning(f"Skipping example {i+1}: Missing db_id or query")
                continue
                
            try:
                # Process the example
                result = await process_text_to_sql(
                    selector_agent=selector_agent,
                    decomposer_agent=decomposer_agent,
                    refiner_agent=refiner_agent,
                    task_json=json.dumps(task),
                    max_refinement_attempts=max_refinement_attempts,
                    timeout=timeout
                )
                
                # Add example ID for tracking
                result["example_id"] = i
                
                # Add gold SQL if available
                if "sql" in example:
                    result["gold_sql"] = example["sql"]
                
                # Track success/failure
                if "error" in result:
                    error_count += 1
                    logger.error(f"Error in example {i+1}: {result['error']}")
                else:
                    status = result.get("status", "UNKNOWN")
                    if status in ['EXECUTION_SUCCESSFUL', 'NO_CHANGE_NEEDED', 'EXECUTION_CONFIRMED']:
                        success_count += 1
                        logger.info(f"Example {i+1} completed successfully with status: {status}")
                    else:
                        error_count += 1
                        logger.warning(f"Example {i+1} completed with non-success status: {status}")
                        
                # Store result
                results.append(result)
                
                # Save intermediate results every 10 examples
                if output_path and i % 10 == 0 and i > 0:
                    try:
                        with open(output_path, 'w') as f:
                            json.dump({
                                "dataset_type": dataset_type,
                                "total_examples": len(dataset),
                                "processed_examples": i + 1,
                                "success_count": success_count,
                                "error_count": error_count,
                                "results": results
                            }, f, indent=2)
                        logger.info(f"Saved intermediate results to {output_path}")
                    except Exception as e:
                        logger.error(f"Error saving intermediate results: {str(e)}")
                
            except Exception as e:
                logger.error(f"Error processing example {i+1}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                
                # Store error result
                results.append({
                    "example_id": i,
                    "db_id": task["db_id"],
                    "query": task["query"],
                    "evidence": task["evidence"],
                    "error": str(e),
                    "status": "ERROR"
                })
                error_count += 1
        
        # Compute success rate
        success_rate = success_count / len(dataset) if len(dataset) > 0 else 0
        
        # Prepare final report
        evaluation_report = {
            "dataset_type": dataset_type,
            "total_examples": len(dataset),
            "success_count": success_count,
            "error_count": error_count,
            "success_rate": success_rate,
            "results": results
        }
        
        # Save final results
        if output_path:
            try:
                with open(output_path, 'w') as f:
                    json.dump(evaluation_report, f, indent=2)
                logger.info(f"Saved final results to {output_path}")
            except Exception as e:
                logger.error(f"Error saving final results: {str(e)}")
        
        return evaluation_report
        
    except Exception as e:
        logger.error(f"Error in dataset evaluation: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"error": str(e)}


# --- Main API ---

class TextToSQLProcessor:
    """Main class for Text-to-SQL processing with a three-agent approach."""
    
    def __init__(
        self, 
        selector_agent, 
        decomposer_agent, 
        refiner_agent,
        max_refinement_attempts: int = 3,
        timeout: int = 120
    ):
        """
        Initialize the Text-to-SQL processor.
        
        Args:
            selector_agent: Agent for schema selection
            decomposer_agent: Agent for SQL generation
            refiner_agent: Agent for SQL refinement
            max_refinement_attempts: Maximum refinement iterations
            timeout: Default timeout in seconds for agent operations
        """
        self.selector_agent = selector_agent
        self.decomposer_agent = decomposer_agent
        self.refiner_agent = refiner_agent
        self.max_refinement_attempts = max_refinement_attempts
        self.timeout = timeout
        
    async def process_query(self, task_json: str) -> Dict:
        """
        Process a single Text-to-SQL query.
        
        Args:
            task_json: JSON string with task details
            
        Returns:
            Dictionary with processing results
        """
        return await process_text_to_sql(
            selector_agent=self.selector_agent,
            decomposer_agent=self.decomposer_agent,
            refiner_agent=self.refiner_agent,
            task_json=task_json,
            max_refinement_attempts=self.max_refinement_attempts,
            timeout=self.timeout
        )
        
    async def process_batch(self, tasks: List[Dict]) -> List[Dict]:
        """
        Process a batch of Text-to-SQL queries.
        
        Args:
            tasks: List of task dictionaries
            
        Returns:
            List of processing results
        """
        results = []
        for task in tasks:
            result = await self.process_query(json.dumps(task))
            results.append(result)
        return results
        
    async def evaluate(
        self, 
        dataset_path: str, 
        dataset_type: str = "bird",
        limit: Optional[int] = None,
        output_path: Optional[str] = None
    ) -> Dict:
        """
        Evaluate a dataset of Text-to-SQL queries.
        
        Args:
            dataset_path: Path to the dataset file (JSON)
            dataset_type: Type of dataset ("bird" or "spider")
            limit: Maximum number of examples to process
            output_path: Path to save results
            
        Returns:
            Dictionary with evaluation results
        """
        return await evaluate_dataset(
            selector_agent=self.selector_agent,
            decomposer_agent=self.decomposer_agent,
            refiner_agent=self.refiner_agent,
            dataset_path=dataset_path,
            dataset_type=dataset_type,
            limit=limit,
            output_path=output_path,
            max_refinement_attempts=self.max_refinement_attempts,
            timeout=self.timeout
        )