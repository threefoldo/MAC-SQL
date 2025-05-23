# Text-to-SQL Workflow

This directory contains a modular implementation of a Text-to-SQL workflow that converts natural language questions into SQL queries for database interaction.

## Architecture

The workflow uses a three-agent approach:

1. **Schema Selector Agent**: Analyzes database schemas and prunes them to focus on relevant tables/columns
2. **Decomposer Agent**: Converts natural language into SQL by understanding the schema and query
3. **Refiner Agent**: Executes and refines SQL queries, handling errors and optimizations

## Files and Components

The workflow is organized into modular components:

- **`const.py`**: Common constants and prompt templates used by the agents
- **`schema_manager.py`**: Handles database schema loading and manipulation
- **`sql_executor.py`**: Executes SQL queries against databases with timeout handling
- **`utils.py`**: General utility functions used throughout the project
- **`workflow_utils.py`**: Specific utilities for the text-to-SQL workflow
- **`08-selector-test2.ipynb`**: Demo notebook showing the workflow in action

## Main Process Flow

The Text-to-SQL workflow consists of three main steps:

1. **Schema Selection**:
   - Input: Natural language query, database ID, and evidence
   - Process: Load and analyze database schema, prune irrelevant tables/columns
   - Output: Schema description in XML format

2. **SQL Generation**:
   - Input: Schema from previous step and natural language query
   - Process: Decompose the query into logical steps, generate SQL
   - Output: SQL query string

3. **SQL Refinement**:
   - Input: SQL query, database ID, and original query
   - Process: Execute SQL, identify errors, refine and re-execute
   - Output: Final SQL query with execution results

## Using the Workflow

The workflow can be used in three different modes:

1. **Step-by-Step Mode**: Run each step individually to see detailed intermediate results
2. **Combined Mode**: Run the entire pipeline at once for more concise output
3. **Batch Mode**: Process multiple queries in sequence

Example usage from Python:

```python
from workflow_utils import process_text_to_sql

# Process a single query
result = await process_text_to_sql(
    selector_agent=selector_agent,
    decomposer_agent=decomposer_agent,
    refiner_agent=refiner_agent,
    task_json=json.dumps({
        "db_id": "database_name",
        "query": "Your natural language query",
        "evidence": "Additional context if available"
    })
)
```

## Customization

The workflow can be customized in several ways:

- **Prompts**: Modify templates in `const.py` to adjust agent behavior
- **Timeouts**: Change timeout values to handle more complex queries
- **Error Handling**: Adjust error handling strategies in the workflow functions
- **Output Format**: Modify the result format in the workflow functions

## Testing and Evaluation

The workflow includes tools for testing and evaluation:

- **Test Cases**: Sample test cases are included in the notebook
- **Batch Testing**: Functions for running multiple tests and collecting results
- **Error Analysis**: Detailed error logging for debugging

## Dependencies

- **Autogen**: For creating and managing the agent-based workflow
- **SQLite**: For executing and testing SQL queries
- **OpenAI API**: For language model access through Autogen