#!/usr/bin/env python3
"""
Test single BIRD example for debugging.
Logs to a file named after the question ID for easy troubleshooting.
"""

import json
import os
import sys
import argparse
import logging
from typing import List, Dict, Any, Tuple
import re
from pprint import pprint
import asyncio
import sqlite3
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(project_root, "workflow_v2", "src"))

from text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator


def load_bird_dev_data(data_path: str = "./data/bird/dev.json") -> List[Dict[str, Any]]:
    """Load BIRD dev dataset."""
    with open(data_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def replace_multiple_spaces(sql: str) -> str:
    """Replace multiple spaces with single space."""
    return re.sub(r'\s+', ' ', sql).strip()


def execute_sql(db_path: str, sql: str, timeout: float = 30.0) -> Tuple[bool, Any, float]:
    """
    Execute SQL query and return results with execution time.
    
    Returns:
        Tuple of (success, result/error, execution_time_ms)
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        
        # Set timeout
        conn.execute(f"PRAGMA busy_timeout = {int(timeout * 1000)}")
        
        cursor = conn.cursor()
        
        # Measure execution time
        start_time = time.perf_counter()
        cursor.execute(sql)
        result = cursor.fetchall()
        end_time = time.perf_counter()
        
        execution_time_ms = (end_time - start_time) * 1000
        
        conn.close()
        return True, result, execution_time_ms
        
    except Exception as e:
        return False, str(e), 0.0


def evaluate_ex(db_path: str, pred_sql: str, gold_sql: str) -> Dict[str, Any]:
    """
    Evaluate Execution Accuracy (EX).
    Returns 1 if results match exactly, 0 otherwise.
    """
    # Execute both queries
    pred_success, pred_result, pred_time = execute_sql(db_path, pred_sql)
    gold_success, gold_result, gold_time = execute_sql(db_path, gold_sql)
    
    result = {
        "ex_score": 0,
        "pred_success": pred_success,
        "gold_success": gold_success,
        "pred_rows": 0,
        "gold_rows": 0,
        "pred_time_ms": pred_time,
        "gold_time_ms": gold_time,
        "error": None
    }
    
    if not gold_success:
        result["error"] = f"Gold SQL failed: {gold_result}"
        return result
        
    if not pred_success:
        result["error"] = f"Predicted SQL failed: {pred_result}"
        return result
    
    # Convert results to sets for comparison
    pred_set = set(map(tuple, pred_result))
    gold_set = set(map(tuple, gold_result))
    
    result["pred_rows"] = len(pred_set)
    result["gold_rows"] = len(gold_set)
    
    # Check if results match exactly
    if pred_set == gold_set:
        result["ex_score"] = 1
    
    return result


async def test_single_example(
    example_index: int = 0,
    question_id: str = None,
    data_path: str = None,
    verbose: bool = True
) -> None:
    """
    Test a single BIRD example.
    
    Args:
        example_index: Index of the example to test (0-based)
        question_id: Specific question ID to test (overrides example_index)
        data_path: Path to BIRD data file
        verbose: Whether to show detailed output
    """
    # Set default data path
    if data_path is None:
        data_path = "./data/bird/dev.json"
    
    # Load data
    print(f"\nLoading BIRD data from {data_path}")
    data = load_bird_dev_data(data_path)
    
    # Find the example to test
    if question_id:
        # Find by question ID
        example = None
        for idx, ex in enumerate(data):
            if ex.get('question_id') == question_id:
                example = ex
                example_index = idx
                break
        if not example:
            print(f"Error: Question ID '{question_id}' not found")
            return
    else:
        # Use index
        if example_index >= len(data):
            print(f"Error: Index {example_index} out of range (max: {len(data)-1})")
            return
        example = data[example_index]
    
    # Extract example details
    qid = example.get('question_id', f'idx_{example_index}')
    question = example['question']
    db_id = example['db_id']
    evidence = example.get('evidence', '')
    ground_truth_sql = example.get('SQL', 'Not available')
    
    # Setup logging to file named after question ID
    os.makedirs("./outputs/logs/debug", exist_ok=True)
    log_filename = f"./outputs/logs/debug/{qid}.log"
    
    # Configure detailed logging
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True  # Override any existing configuration
    )
    logger = logging.getLogger(__name__)
    
    # Set log levels for orchestrator components
    if verbose:
        logging.getLogger('TextToSQLTreeOrchestrator').setLevel(logging.DEBUG)
        logging.getLogger('QueryAnalyzerAgent').setLevel(logging.DEBUG)
        logging.getLogger('SchemaLinkerAgent').setLevel(logging.DEBUG)
        logging.getLogger('SQLGeneratorAgent').setLevel(logging.DEBUG)
        logging.getLogger('SQLEvaluatorAgent').setLevel(logging.DEBUG)
        logging.getLogger('TaskStatusChecker').setLevel(logging.DEBUG)
    else:
        # Still keep some noise down
        logging.getLogger('autogen_core').setLevel(logging.WARNING)
        logging.getLogger('autogen_agentchat').setLevel(logging.WARNING)
        logging.getLogger('httpx').setLevel(logging.WARNING)
        logging.getLogger('openai').setLevel(logging.WARNING)
    
    # Log example details
    logger.info("="*80)
    logger.info(f"TESTING SINGLE EXAMPLE: {qid}")
    logger.info("="*80)
    logger.info(f"Index: {example_index}")
    logger.info(f"Database: {db_id}")
    logger.info(f"Question: {question}")
    if evidence:
        logger.info(f"Evidence: {evidence}")
    logger.info(f"Ground Truth SQL:\n{ground_truth_sql}")
    logger.info("-"*80)
    
    try:
        # Initialize orchestrator
        logger.info("Initializing TextToSQLTreeOrchestrator...")
        tables_json_path = "./data/bird/dev_tables.json"
        orchestrator = TextToSQLTreeOrchestrator(
            data_path="./data/bird",
            tables_json_path=tables_json_path,
            dataset_name="bird"
        )
        
        # Generate SQL
        logger.info("\nGenerating SQL...")
        try:
            result = await orchestrator.process_query(
                query=question,
                db_name=db_id,
                evidence=evidence
            )
        except (asyncio.CancelledError, Exception) as e:
            if isinstance(e, asyncio.CancelledError):
                logger.warning("Query processing was cancelled - this may be normal during shutdown")
                result = None
            else:
                logger.error(f"Error during query processing: {str(e)}")
                result = None
        
        # Log the entire result
        logger.info("\n" + "="*80)
        logger.info("FULL RESULT:")
        logger.info("="*80)
        
        if result is not None:
            import pprint as pp
            result_str = pp.pformat(result, width=120)
            logger.info(result_str)
        else:
            logger.error("No result returned from orchestrator")
        
        # Extract SQL
        pred_sql = None
        if result and 'final_result' in result and result['final_result']:
            pred_sql = result['final_result']
            logger.info(f"\nGenerated SQL:\n{pred_sql}")
        else:
            pred_sql = "SELECT 'Failed to generate SQL'"
            logger.error("Failed to generate SQL - no final_result in response")
            
            # Log additional debugging info
            if result and 'query_tree' in result:
                tree = result['query_tree']
                logger.error("\nQuery tree structure:")
                logger.error(f"Root ID: {tree.get('rootId', 'None')}")
                if 'nodes' in tree:
                    for node_id, node_data in tree['nodes'].items():
                        logger.error(f"\nNode {node_id}:")
                        logger.error(f"  Status: {node_data.get('status')}")
                        logger.error(f"  Has SQL: {'sql' in node_data}")
                        if 'executionResult' in node_data:
                            logger.error(f"  Execution: {node_data['executionResult'].get('status')}")
        
        # Clean up SQL (handle None case)
        if pred_sql:
            pred_sql = replace_multiple_spaces(pred_sql)
        else:
            pred_sql = "SELECT 'Failed to generate SQL'"
        
        # Evaluate
        logger.info("\n" + "="*80)
        logger.info("EVALUATION:")
        logger.info("="*80)
        
        db_path = f"./data/bird/dev_databases/{db_id}/{db_id}.sqlite"
        ex_result = evaluate_ex(db_path, pred_sql, ground_truth_sql)
        
        logger.info(f"\nEX Score: {ex_result['ex_score']}")
        if ex_result['error']:
            logger.error(f"Error: {ex_result['error']}")
        else:
            logger.info(f"Predicted rows: {ex_result['pred_rows']}")
            logger.info(f"Ground truth rows: {ex_result['gold_rows']}")
            logger.info(f"Match: {'YES' if ex_result['ex_score'] == 1 else 'NO'}")
            
            # If failed, show detailed comparison
            if ex_result['ex_score'] == 0:
                logger.error("\nMISMATCH DETAILS:")
                logger.error(f"Ground Truth SQL:\n{ground_truth_sql}")
                logger.error(f"\nGenerated SQL:\n{pred_sql}")
                
                # Try to show first few rows of each result
                pred_success, pred_data, _ = execute_sql(db_path, pred_sql)
                gold_success, gold_data, _ = execute_sql(db_path, ground_truth_sql)
                
                if pred_success and gold_success:
                    logger.error(f"\nGround truth first 5 rows:")
                    for row in gold_data[:5]:
                        logger.error(f"  {row}")
                    logger.error(f"\nPredicted first 5 rows:")
                    for row in pred_data[:5]:
                        logger.error(f"  {row}")
        
        # Final status
        logger.info("\n" + "="*80)
        if ex_result['ex_score'] == 1:
            logger.info("✅ TEST PASSED")
        else:
            logger.error("❌ TEST FAILED")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"\n❌ ERROR: {str(e)}", exc_info=True)
    
    print(f"\nLog saved to: {log_filename}")


def main():
    parser = argparse.ArgumentParser(description='Test single BIRD example for debugging')
    parser.add_argument('--index', type=int, default=0,
                        help='Index of example to test (0-based)')
    parser.add_argument('--question_id', type=str, default=None,
                        help='Specific question ID to test (overrides index)')
    parser.add_argument('--data_path', type=str, default=None,
                        help='Path to BIRD data file')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose/debug logging')
    
    args = parser.parse_args()
    
    # Run the test
    asyncio.run(test_single_example(
        example_index=args.index,
        question_id=args.question_id,
        data_path=args.data_path,
        verbose=args.verbose
    ))


if __name__ == "__main__":
    main()