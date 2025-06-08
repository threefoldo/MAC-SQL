# Basic Orchestration Rules

Simple rules for the text-to-SQL tree orchestrator.

## Table of Contents
1. [Core Architecture](#core-architecture)
2. [Quick Start Guide](#quick-start-guide)
3. [Key Principles](#key-principles)
4. [Common Workflows](#common-workflows)
5. [Troubleshooting](#troubleshooting)

## Quick Start Guide

### Running the System
```python
from src.text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator

# Initialize with LLM config
orchestrator = TextToSQLTreeOrchestrator(llm_config)

# Convert query to SQL
result = await orchestrator.run(
    query="Show me top 5 customers by revenue",
    database_name="ecommerce",
    evidence="Optional hints about the query"
)

# Result contains the final SQL and execution status
print(result.sql)
print(result.status)
```

### Understanding the Workflow
1. User query → Task initialization
2. Orchestrator analyzes state → Chooses agent
3. Agent processes → Updates memory
4. Repeat until SQL is good or max retries
5. Return final SQL result

## Core Architecture

### 1. Orchestrator Agent as State Machine
The orchestrator agent is an LLM-powered state machine that decides which agent to call:
- **Available agents**: SchemaLinker, QueryAnalyzer, SQLGenerator, SQLEvaluator
- **Decision making**: LLM analyzes current state and chooses appropriate agent
- **Task representation**: Node tree where each node represents a step
- **State transitions**: Based on LLM analysis of current task state and node status

### 2. Task Status Management  
The orchestrator calls TaskStatusChecker as a tool to:
- Analyze the whole QueryTree and report current task state
- Identify the current node for processing
- Provide deterministic status overview for LLM decision making
- **Node selection rules**:
  - Always start from the root node
  - If a node is unprocessed → Orchestrator LLM decides which agents to call
  - If a node is processed with good result → Move to next unprocessed node (sibling or parent)
  - If no node is unprocessed → Task is complete

### 3. Node States
- `no_sql`: Node has no SQL
- `need_eval`: Node has SQL, needs evaluation  
- `good_sql`: Node has excellent/good SQL
- `bad_sql`: Node has poor SQL
- `max_retries`: Node failed 3 times

### 4. Agent Architecture
Each agent (except orchestrator and TaskStatusChecker) is a simple wrapper containing:
- **Prompt**: Instructions for the LLM
- **Reader callback**: Extracts data from shared memory as context
- **Parser callback**: Extracts information from LLM output to shared memory
- **Usage**: Agents are used as tools by the orchestrator
- **Implementation**: Built on top of AutoGen agents

### 5. Shared Memory Management
Shared memory is managed by several specialized classes:
- **TaskContextManager**: Manages overall task context and status
- **DatabaseSchemaManager**: Handles database schema information
- **QueryTreeManager**: Manages the query decomposition tree
- **NodeHistoryManager**: Tracks node processing history

**Important**: Agents should NOT read/write directly to shared memory. All memory operations should go through these manager classes for organization and consistency.


## Key Principles

1. **LLM-powered orchestration** - Orchestrator uses LLM to decide which agent to call
2. **Simple node rules** - TaskStatusChecker handles deterministic logic
3. **Clean agent design** - Each agent has clear responsibilities
4. **Managed memory access** - All memory operations through managers
5. **Context changes** - Break failure patterns through different approaches
6. **Clear limits** - Prevent infinite loops with retry limits
7. **Structured status** - Enable good decisions through clear state tracking

## Common Workflows

### Simple Query Flow
```
1. Query: "Show all active customers"
2. Orchestrator → SchemaLinker (find customer table)
3. Orchestrator → SQLGenerator (create SELECT query)
4. Orchestrator → SQLEvaluator (verify results)
5. Done: Returns SQL with good evaluation
```

### Complex Query with Decomposition
```
1. Query: "Compare revenue this month vs last month by category"
2. Orchestrator → QueryAnalyzer (decompose into subqueries)
3. For each subquery node:
   - Orchestrator → SchemaLinker
   - Orchestrator → SQLGenerator
   - Orchestrator → SQLEvaluator
4. Combine results into final SQL
```

### Error Recovery Flow
```
1. SQL fails with "table not found"
2. Orchestrator → SchemaLinker (re-analyze schema)
3. Orchestrator → SQLGenerator (regenerate with correct table)
4. Orchestrator → SQLEvaluator (verify fix)
5. Success or retry with different approach
```

## Troubleshooting

### Common Issues

1. **Agent Selection Loop**
   - **Symptom**: Same agent called repeatedly
   - **Cause**: Node state not updating properly
   - **Fix**: Check manager write operations in agent's parser callback

2. **Schema Linking Failures**
   - **Symptom**: Can't find obvious tables
   - **Cause**: Schema not loaded or incorrect database name
   - **Fix**: Verify DatabaseSchemaManager has loaded schema

3. **SQL Generation Errors**
   - **Symptom**: Invalid SQL syntax
   - **Cause**: Missing schema context or complex query structure
   - **Fix**: Ensure schema_linking completed before SQL generation

4. **Infinite Retry Loops**
   - **Symptom**: Node stuck in retry cycle
   - **Cause**: Same error repeating
   - **Fix**: NodeHistoryManager enforces MAX_RETRIES=3

### Debug Tips

1. **Enable Verbose Logging**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Inspect Memory State**
   ```python
   # Check current task state
   task_context = await task_manager.get_context()
   print(task_context)
   
   # View query tree
   tree = await tree_manager.get_tree()
   print(tree.to_dict())
   ```

3. **Monitor Agent Calls**
   - Log each orchestrator decision
   - Track which agents are called and why
   - Verify state changes after each agent

## Best Practices

1. **Clear Query Intent**
   - Provide specific, unambiguous queries
   - Include evidence/hints when available
   - Use domain-specific terminology consistently

2. **Schema Preparation**
   - Ensure database schema is complete
   - Include sample data for better context
   - Document table relationships clearly

3. **Error Handling**
   - Let orchestrator handle retries
   - Don't catch exceptions in agents
   - Trust the feedback loop mechanism

4. **Performance**
   - Simple queries: < 5 seconds
   - Complex queries: < 30 seconds
   - Use query decomposition for very complex queries