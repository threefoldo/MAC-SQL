# Text-to-SQL Tree Orchestration System

This directory contains a modular implementation of a Text-to-SQL tree orchestration system that converts natural language questions into SQL queries for database interaction using a tree-based decomposition approach.

## Architecture

The system uses a tree orchestration approach with specialized agents:

1. **Query Analyzer Agent**: Analyzes natural language queries and creates a tree structure for complex queries
2. **Schema Linker Agent**: Links query elements to database schema (tables/columns)
3. **SQL Generator Agent**: Generates SQL queries for each node in the query tree
4. **SQL Evaluator Agent**: Evaluates and validates generated SQL queries
5. **Tree Orchestrator**: Coordinates the agents and manages the query decomposition tree

## Files and Components

The system is organized into modular components:

- **`const.py`**: Common constants and prompt templates used by the agents
- **`schema_manager.py`**: Handles database schema loading and manipulation
- **`sql_executor.py`**: Executes SQL queries against databases with timeout handling
- **`utils.py`**: General utility functions used throughout the project
- **`text_to_sql_tree_orchestrator.py`**: Main orchestrator that manages the query tree
- **Agent modules**: Individual agent implementations (query_analyzer_agent.py, schema_linker_agent.py, etc.)
- **Memory system**: KeyValueMemory and related content types for state management

## Main Process Flow

The Text-to-SQL tree orchestration consists of these main steps:

1. **Query Analysis**:
   - Input: Natural language query and evidence
   - Process: Analyze query complexity and create tree structure if needed
   - Output: Query tree with sub-queries for complex questions

2. **Schema Linking**:
   - Input: Query (or sub-query) and database schema
   - Process: Identify relevant tables and columns
   - Output: Linked schema elements

3. **SQL Generation**:
   - Input: Query node with linked schema
   - Process: Generate SQL for each node in the tree
   - Output: SQL query for the node

4. **SQL Evaluation**:
   - Input: Generated SQL and database
   - Process: Validate and potentially refine SQL
   - Output: Validated SQL query

5. **Tree Synthesis**:
   - Input: Results from all tree nodes
   - Process: Combine sub-query results for final answer
   - Output: Final SQL query

## Using the Tree Orchestrator

The tree orchestrator automatically handles:

1. **Simple Queries**: Direct SQL generation without decomposition
2. **Complex Queries**: Automatic decomposition into sub-queries
3. **Nested Queries**: Hierarchical tree structure for multi-step questions

Example usage from Python:

```python
from text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator

# Initialize orchestrator
orchestrator = TextToSQLTreeOrchestrator(
    model_name="gpt-4",
    db_schema_path="path/to/schemas"
)

# Process a query
result = await orchestrator.process_query(
    query="Your natural language query",
    db_id="database_name",
    evidence="Additional context if available"
)
```

## Customization

The tree orchestrator can be customized in several ways:

- **Prompts**: Modify templates in `prompts.py` to adjust agent behavior
- **Tree Depth**: Configure maximum tree depth for query decomposition
- **Model Selection**: Choose different LLM models (all agents use the same model)
- **Memory System**: Customize memory storage and retrieval strategies

## Testing and Evaluation

The system includes comprehensive testing:

- **Unit Tests**: Individual tests for each agent and component
- **Integration Tests**: End-to-end workflow testing
- **BIRD Dataset Testing**: Evaluation on standard benchmarks
- **Memory Tracing**: Detailed logging of agent decisions and memory state

## Dependencies

- **Autogen**: For creating and managing the agent-based system
- **SQLite**: For executing and testing SQL queries
- **OpenAI API**: For language model access
- **Pydantic**: For data validation and type safety
- **XML parsing**: For structured agent responses