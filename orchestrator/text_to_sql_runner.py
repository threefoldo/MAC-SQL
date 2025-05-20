#!/usr/bin/env python
"""
Text-to-SQL Runner

This script provides a command-line interface for running the text-to-SQL system
in various modes:
- Interactive mode: Process individual queries interactively
- Batch mode: Process a batch of queries from a file
- Evaluation mode: Evaluate on a dataset (BIRD, Spider)

Example usage:
    # Interactive mode
    python text_to_sql_runner.py --mode interactive --db bird

    # Batch mode
    python text_to_sql_runner.py --mode batch --input queries.json --output results.json

    # Evaluation mode
    python text_to_sql_runner.py --mode eval --dataset bird --split dev --limit 10
"""

import os
import sys
import json
import argparse
import asyncio
import logging
from typing import Dict, List, Any, Optional

# Autogen imports
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.agents import AssistantAgent
from autogen_core import CancellationToken

# Project imports
from const import (
    SELECTOR_NAME, DECOMPOSER_NAME, REFINER_NAME,
    selector_template, decompose_template_bird, decompose_template_spider, refiner_template
)
from schema_manager import SchemaManager
from sql_executor import SQLExecutor
from text_to_sql_processor import TextToSQLProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("text_to_sql_runner.log")
    ]
)
logger = logging.getLogger("text_to_sql_runner")

# --- Constants ---
DEFAULT_TIMEOUT = 120  # seconds
MAX_REFINEMENT_ATTEMPTS = 3

# Dataset paths
DATASETS = {
    "bird": {
        "path": "../data/bird",
        "dev": "../data/bird/dev.json",
        "train": "../data/bird/train.json",
        "tables_json": "../data/bird/dev_tables.json"
    },
    "spider": {
        "path": "../data/spider",
        "dev": "../data/spider/dev.json",
        "train": "../data/spider/train_spider.json",
        "tables_json": "../data/spider/tables.json"
    }
}

# --- Helper Functions ---

def load_test_cases(dataset_name: str, limit: Optional[int] = None) -> List[Dict]:
    """Load test cases from a dataset."""
    dataset_path = DATASETS[dataset_name]["dev"]
    with open(dataset_path, 'r') as f:
        data = json.load(f)
    
    # Apply limit if specified
    if limit and limit > 0:
        data = data[:limit]
    
    # Format data for our system
    return [
        {
            "db_id": item.get("db_id", ""),
            "query": item.get("question", "") or item.get("query", ""),
            "evidence": item.get("evidence", ""),
            "gold_sql": item.get("sql", "")
        }
        for item in data if item.get("db_id") and (item.get("question") or item.get("query"))
    ]

# --- Agent Setup ---

def setup_agents(dataset_name: str):
    """Set up the agents for text-to-SQL processing."""
    # Initialize data paths
    data_path = DATASETS[dataset_name]["path"]
    tables_json_path = DATASETS[dataset_name]["tables_json"]
    
    # Initialize schema manager and SQL executor
    schema_manager = SchemaManager(
        data_path=data_path,
        tables_json_path=tables_json_path,
        dataset_name=dataset_name,
        lazy=False
    )
    
    sql_executor = SQLExecutor(
        data_path=data_path,
        dataset_name=dataset_name
    )
    
    # Tool implementation for schema selection and management
    async def get_initial_database_schema(db_id: str) -> str:
        """Retrieves the full database schema information for a given database."""
        logger.info(f"[Tool] Loading schema for database: {db_id}")
        
        # Load database information using SchemaManager
        if db_id not in schema_manager.db2infos:
            schema_manager.db2infos[db_id] = schema_manager._load_single_db_info(db_id)
        
        # Get database information
        db_info = schema_manager.db2dbjsons.get(db_id, {})
        if not db_info:
            return json.dumps({"error": f"Database '{db_id}' not found"})
        
        # Generate full schema description (without pruning)
        is_complex = schema_manager._is_complex_schema(db_id)
        full_schema_str, full_fk_str, _ = schema_manager.generate_schema_description(
            db_id, {}, use_gold_schema=False
        )
        
        # Return schema details
        return json.dumps({
            "db_id": db_id,
            "table_count": db_info.get('table_count', 0),
            "total_column_count": db_info.get('total_column_count', 0),
            "avg_column_count": db_info.get('avg_column_count', 0),
            "is_complex_schema": is_complex,
            "full_schema_str": full_schema_str,
            "full_fk_str": full_fk_str
        })

    async def prune_database_schema(db_id: str, pruning_rules: Dict) -> str:
        """Applies pruning rules to a database schema."""
        logger.info(f"[Tool] Pruning schema for database {db_id}")
        
        # Generate pruned schema description
        schema_str, fk_str, chosen_schema = schema_manager.generate_schema_description(
            db_id, pruning_rules, use_gold_schema=False
        )
        
        # Return pruned schema
        return json.dumps({
            "db_id": db_id,
            "pruning_applied": True,
            "pruning_rules": pruning_rules,
            "pruned_schema_str": schema_str,
            "pruned_fk_str": fk_str,
            "tables_columns_kept": chosen_schema
        })

    # Tool implementation for SQL execution
    async def execute_sql(sql: str, db_id: str) -> str:
        """Executes a SQL query on the specified database."""
        logger.info(f"[Tool] Executing SQL on database {db_id}: {sql[:100]}...")
        
        # Execute SQL with timeout protection
        result = sql_executor.safe_execute(sql, db_id)
        
        # Add validation information
        is_valid, reason = sql_executor.is_valid_result(result)
        result["is_valid_result"] = is_valid
        result["validation_message"] = reason
        
        # Convert to JSON string
        return json.dumps(result)
    
    # Initialize model client
    model_client = OpenAIChatCompletionClient(model="gpt-4o")
    
    # Choose the appropriate template based on the dataset
    decompose_template = decompose_template_bird if dataset_name == "bird" else decompose_template_spider
    
    # Create Schema Selector Agent
    selector_agent = AssistantAgent(
        name=SELECTOR_NAME,
        model_client=model_client,
        system_message=f"""You are a Database Schema Selector specialized in analyzing database schemas for text-to-SQL tasks.

Your job is to help prune large database schemas to focus on the relevant tables and columns for a given query.

TASK OVERVIEW:
1. You will receive a task with database ID, query, and evidence
2. Use the 'get_initial_database_schema' tool to retrieve the full schema
3. Analyze the schema complexity and relevance to the query
4. For complex schemas, determine which tables and columns are relevant
5. Use the 'prune_database_schema' tool to generate a focused schema
6. Return the processed schema information for the next agent

WHEN ANALYZING THE SCHEMA:
- Study the database table structure and relationships
- Identify tables directly mentioned or implied in the query
- Consider foreign key relationships that might be needed
- Follow the pruning guidelines in the following template:
{selector_template}

FORMAT YOUR FINAL RESPONSE AS JSON:
{{
  "db_id": "<database_id>",
  "query": "<natural_language_query>",
  "evidence": "<any_evidence_provided>",
  "pruning_applied": true/false,
  "schema_str": "<schema_description>",
  "fk_str": "<foreign_key_information>"
}}

Remember that high-quality schema selection improves the accuracy of SQL generation.""",
        tools=[get_initial_database_schema, prune_database_schema],
    )
    
    # Create Decomposer Agent
    decomposer_agent = AssistantAgent(
        name=DECOMPOSER_NAME,
        model_client=model_client,
        system_message=f"""You are a Query Decomposer specialized in converting natural language questions into SQL for the {dataset_name.upper()} dataset.

Your job is to analyze a natural language query and relevant database schema, then generate the appropriate SQL query.

TASK OVERVIEW:
1. You will receive a JSON with db_id, query, evidence, and schema information
2. Study the database schema, focusing on tables, columns, and relationships
3. For complex queries, break down the problem into logical steps
4. Generate a clear, efficient SQL query that answers the question
5. Follow the specific query decomposition template:
{decompose_template}

IMPORTANT CONSIDERATIONS:
- Carefully use the evidence provided to understand domain-specific concepts
- Apply type conversion when comparing numeric data (CAST AS REAL/INT)
- Ensure proper handling of NULL values
- Use table aliases (T1, T2, etc.) for clarity, especially in JOINs
- Always use valid SQLite syntax

FORMAT YOUR FINAL RESPONSE AS JSON:
{{
  "db_id": "<database_id>",
  "query": "<natural_language_query>",
  "evidence": "<any_evidence_provided>",
  "sql": "<generated_sql_query>",
  "decomposition": [
    "step1_description", 
    "step2_description",
    ...
  ]
}}

Your goal is to generate SQL that will execute correctly and return the precise information requested.""",
        tools=[],  # SQL generation is the primary LLM task
    )
    
    # Create Refiner Agent
    refiner_agent = AssistantAgent(
        name=REFINER_NAME,
        model_client=model_client,
        system_message=f"""You are an SQL Refiner specializing in executing and fixing SQL queries for the {dataset_name.upper()} dataset.

Your job is to test SQL queries against the database, identify errors, and refine them until they execute successfully.

TASK OVERVIEW:
1. You will receive a JSON with db_id, query, evidence, schema, and SQL
2. Use the 'execute_sql' tool to run the SQL against the database
3. Analyze execution results or errors
4. For errors, refine the SQL using the template:
{refiner_template}
5. For successful execution, validate the results are appropriate for the original query

IMPORTANT CONSIDERATIONS:
- Focus on SQLite-specific syntax and behaviors
- Pay special attention to:
  - Table and column name quoting (use backticks)
  - Type conversions (CAST AS)
  - JOIN conditions
  - Subquery structure and aliases

FORMAT YOUR FINAL RESPONSE AS JSON:
{{
  "db_id": "<database_id>",
  "query": "<natural_language_query>",
  "evidence": "<any_evidence_provided>",
  "original_sql": "<original_sql>",
  "final_sql": "<refined_sql>",
  "status": "<EXECUTION_SUCCESSFUL|REFINEMENT_NEEDED|NO_CHANGE_NEEDED>",
  "execution_result": "<execution_result_summary>",
  "refinement_explanation": "<explanation_of_changes>"
}}

Your goal is to ensure the SQL query executes successfully and returns relevant results.""",
        tools=[execute_sql],
    )
    
    return selector_agent, decomposer_agent, refiner_agent, schema_manager, sql_executor

# --- Mode Handlers ---

async def run_interactive_mode(dataset_name: str):
    """Run the system in interactive mode."""
    print(f"\n{'='*50}")
    print(f"INTERACTIVE TEXT-TO-SQL MODE - {dataset_name.upper()} DATASET")
    print(f"{'='*50}\n")
    print("Type 'quit' or 'exit' to end the session.")
    
    # Setup agents
    selector_agent, decomposer_agent, refiner_agent, schema_manager, sql_executor = setup_agents(dataset_name)
    
    # Create processor
    processor = TextToSQLProcessor(
        selector_agent=selector_agent,
        decomposer_agent=decomposer_agent,
        refiner_agent=refiner_agent,
        max_refinement_attempts=MAX_REFINEMENT_ATTEMPTS,
        timeout=DEFAULT_TIMEOUT
    )
    
    while True:
        # Get database ID
        print("\nAvailable databases:")
        db_list = list(schema_manager.db2dbjsons.keys())[:10]  # Show first 10 databases
        for i, db_id in enumerate(db_list):
            print(f"{i+1}. {db_id}")
        print("... and more")
        
        db_input = input("\nEnter database ID or number from the list: ")
        if db_input.lower() in ['quit', 'exit']:
            break
            
        # Convert number to db_id if needed
        if db_input.isdigit() and 1 <= int(db_input) <= len(db_list):
            db_id = db_list[int(db_input) - 1]
        else:
            db_id = db_input
            
        # Check if database exists
        if db_id not in schema_manager.db2dbjsons:
            print(f"Error: Database '{db_id}' not found. Please enter a valid database ID.")
            continue
            
        # Get query
        query = input("\nEnter your natural language question: ")
        if query.lower() in ['quit', 'exit']:
            break
            
        # Get evidence (optional)
        evidence = input("\nEnter any additional evidence information (optional): ")
        
        # Process query
        task = {
            "db_id": db_id,
            "query": query,
            "evidence": evidence
        }
        
        print("\nProcessing your query... (this might take a minute)\n")
        
        try:
            result = await processor.process_query(json.dumps(task))
            
            # Display result
            print(f"\n{'='*50}")
            print("RESULT")
            print(f"{'='*50}")
            
            # Display database and query info
            print(f"Database: {result.get('db_id', '')}")
            print(f"Query: {result.get('query', '')}")
            
            # Display status
            status = result.get("status", "UNKNOWN")
            print(f"\nExecution Status: {status}")
            
            # Display final SQL
            final_sql = result.get("final_sql", "")
            if final_sql:
                print(f"\nFinal SQL Query:")
                print(final_sql)
            else:
                print("\nNo SQL query was generated.")
                
            # Try to parse and display execution result
            if "final_output" in result:
                final_output = json.loads(result["final_output"])
                if "execution_result" in final_output:
                    print("\nExecution Result:")
                    exec_result = final_output["execution_result"]
                    if isinstance(exec_result, dict):
                        for key, value in exec_result.items():
                            print(f"  {key}: {value}")
                    else:
                        print(exec_result)
                        
            # Display any errors
            if "error" in result:
                print(f"\nError: {result['error']}")
                
        except Exception as e:
            print(f"\nError processing query: {str(e)}")
            import traceback
            print(traceback.format_exc())

async def run_batch_mode(dataset_name: str, input_file: str, output_file: str):
    """Run the system in batch mode."""
    print(f"\n{'='*50}")
    print(f"BATCH TEXT-TO-SQL MODE - {dataset_name.upper()} DATASET")
    print(f"{'='*50}\n")
    
    # Load input file
    try:
        with open(input_file, 'r') as f:
            tasks = json.load(f)
    except Exception as e:
        print(f"Error loading input file: {str(e)}")
        return
    
    print(f"Loaded {len(tasks)} tasks from {input_file}")
    
    # Setup agents
    selector_agent, decomposer_agent, refiner_agent, _, _ = setup_agents(dataset_name)
    
    # Create processor
    processor = TextToSQLProcessor(
        selector_agent=selector_agent,
        decomposer_agent=decomposer_agent,
        refiner_agent=refiner_agent,
        max_refinement_attempts=MAX_REFINEMENT_ATTEMPTS,
        timeout=DEFAULT_TIMEOUT
    )
    
    # Process tasks
    results = []
    success_count = 0
    error_count = 0
    
    for i, task in enumerate(tasks):
        print(f"\nProcessing task {i+1}/{len(tasks)}: {task.get('query', '')[:50]}...")
        
        try:
            result = await processor.process_query(json.dumps(task))
            results.append(result)
            
            # Track success/failure
            if "error" in result:
                error_count += 1
                print(f"  Error: {result['error']}")
            else:
                status = result.get("status", "UNKNOWN")
                if status in ['EXECUTION_SUCCESSFUL', 'NO_CHANGE_NEEDED', 'EXECUTION_CONFIRMED']:
                    success_count += 1
                    print(f"  Success: {status}")
                else:
                    error_count += 1
                    print(f"  Non-success status: {status}")
                    
            # Save intermediate results every 5 tasks
            if output_file and i % 5 == 0 and i > 0:
                try:
                    with open(output_file, 'w') as f:
                        json.dump({
                            "dataset_name": dataset_name,
                            "total_tasks": len(tasks),
                            "processed_tasks": i + 1,
                            "success_count": success_count,
                            "error_count": error_count,
                            "results": results
                        }, f, indent=2)
                    print(f"  Saved intermediate results to {output_file}")
                except Exception as e:
                    print(f"  Error saving intermediate results: {str(e)}")
                    
        except Exception as e:
            print(f"  Error processing task: {str(e)}")
            import traceback
            print(traceback.format_exc())
            
            # Store error result
            results.append({
                "task_id": i,
                "db_id": task.get("db_id", ""),
                "query": task.get("query", ""),
                "evidence": task.get("evidence", ""),
                "error": str(e),
                "status": "ERROR"
            })
            error_count += 1
    
    # Compute success rate
    success_rate = success_count / len(tasks) if len(tasks) > 0 else 0
    
    # Prepare final report
    report = {
        "dataset_name": dataset_name,
        "total_tasks": len(tasks),
        "success_count": success_count,
        "error_count": error_count,
        "success_rate": success_rate,
        "results": results
    }
    
    # Save final results
    if output_file:
        try:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"\nSaved final results to {output_file}")
        except Exception as e:
            print(f"\nError saving final results: {str(e)}")
    
    # Print summary
    print(f"\n{'='*50}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'='*50}")
    print(f"Total tasks: {len(tasks)}")
    print(f"Successful: {success_count} ({success_rate:.1%})")
    print(f"Errors: {error_count} ({error_count / len(tasks):.1%})")

async def run_evaluation_mode(dataset_name: str, split: str, limit: Optional[int], output_file: str):
    """Run the system in evaluation mode."""
    print(f"\n{'='*50}")
    print(f"EVALUATION MODE - {dataset_name.upper()} DATASET - {split.upper()} SPLIT")
    print(f"{'='*50}\n")
    
    # Setup agents
    selector_agent, decomposer_agent, refiner_agent, _, _ = setup_agents(dataset_name)
    
    # Create processor
    processor = TextToSQLProcessor(
        selector_agent=selector_agent,
        decomposer_agent=decomposer_agent,
        refiner_agent=refiner_agent,
        max_refinement_attempts=MAX_REFINEMENT_ATTEMPTS,
        timeout=DEFAULT_TIMEOUT
    )
    
    # Get dataset path
    dataset_path = DATASETS[dataset_name][split]
    
    # Run evaluation
    print(f"Starting evaluation on {dataset_name} {split} dataset")
    if limit:
        print(f"Processing limit: {limit} examples")
        
    evaluation_report = await processor.evaluate(
        dataset_path=dataset_path,
        dataset_type=dataset_name,
        limit=limit,
        output_path=output_file
    )
    
    # Print summary
    print(f"\n{'='*50}")
    print("EVALUATION COMPLETE")
    print(f"{'='*50}")
    print(f"Dataset: {dataset_name} {split}")
    print(f"Total examples: {evaluation_report.get('total_examples', 0)}")
    print(f"Success count: {evaluation_report.get('success_count', 0)}")
    print(f"Error count: {evaluation_report.get('error_count', 0)}")
    print(f"Success rate: {evaluation_report.get('success_rate', 0):.1%}")
    
    if output_file:
        print(f"Full results saved to {output_file}")

# --- Main Entry Point ---

async def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Text-to-SQL Runner")
    
    parser.add_argument("--mode", choices=["interactive", "batch", "eval"], default="interactive",
                        help="Mode to run (interactive, batch, eval)")
    
    parser.add_argument("--db", choices=["bird", "spider"], default="bird",
                        help="Database to use (bird, spider)")
    
    parser.add_argument("--input", type=str, help="Input file for batch mode")
    
    parser.add_argument("--output", type=str, help="Output file for results")
    
    parser.add_argument("--dataset", choices=["bird", "spider"], 
                        help="Dataset to evaluate (bird, spider)")
    
    parser.add_argument("--split", choices=["dev", "train"], default="dev",
                        help="Dataset split to evaluate (dev, train)")
    
    parser.add_argument("--limit", type=int, help="Limit number of examples to process")
    
    args = parser.parse_args()
    
    # Run appropriate mode
    if args.mode == "interactive":
        await run_interactive_mode(args.db)
    
    elif args.mode == "batch":
        if not args.input:
            print("Error: --input file is required for batch mode")
            sys.exit(1)
        
        output_file = args.output or "batch_results.json"
        await run_batch_mode(args.db, args.input, output_file)
    
    elif args.mode == "eval":
        dataset = args.dataset or args.db
        split = args.split
        
        output_file = args.output or f"{dataset}_{split}_evaluation.json"
        await run_evaluation_mode(dataset, split, args.limit, output_file)

if __name__ == "__main__":
    asyncio.run(main())