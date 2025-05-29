# Workflow_v2 System Architecture Analysis

## Table of Contents
1. [Overview](#overview)
2. [Architecture Principles](#core-architecture-principles)
3. [System Components](#file-list-and-purposes)
4. [Architecture Layers](#system-architecture-layers)
5. [Workflow Examples](#workflow-example)
6. [Implementation Guide](#implementation-guide)
7. [Testing Strategy](#testing-strategy)

## Overview
The workflow_v2 system is a sophisticated text-to-SQL conversion system that uses a state machine orchestrator with multiple specialized agents and managed shared memory to convert natural language queries into SQL queries.

### Key Features
- **LLM-Powered Orchestration**: Intelligent workflow management using LLM decisions
- **Modular Agent Design**: Specialized agents for specific tasks
- **Managed Memory System**: Type-safe shared state management
- **Robust Error Handling**: Retry mechanisms and feedback loops
- **Extensible Architecture**: Easy to add new agents or modify workflows

## Core Architecture Principles

### 1. LLM-Powered State Machine Orchestration
The orchestrator agent operates as an LLM-powered state machine that:
- Uses LLM to analyze current state and decide which agent to call
- Manages state transitions through the task workflow based on LLM decisions
- Uses TaskStatusChecker for deterministic state evaluation and context

### 2. Agent Architecture
Each agent (except orchestrator and TaskStatusChecker) follows a consistent pattern:
- **Prompt**: Instructions for the LLM
- **Reader Callback**: Extracts context from shared memory
- **Parser Callback**: Writes results back to shared memory
- **Implementation**: Built as AutoGen agent wrappers
- **Usage**: Agents are tools called by the orchestrator

### 3. Managed Memory Access
All memory operations go through specialized manager classes:
- Agents do NOT read/write directly to shared memory
- Managers provide organized, consistent interfaces
- Ensures data integrity and proper state management

## File List and Purposes

### 1. Core Data Types and Memory System

#### `memory_content_types.py`
- **Purpose**: Defines all shared data structures used across the system
- **Key Components**:
  - Enums: TaskStatus, NodeStatus, NodeOperationType
  - Dataclasses: TaskContext, ColumnInfo, TableSchema, ExecutionResult, QueryNode
- **Dependencies**: None (base module)

#### `keyvalue_memory.py`
- **Purpose**: Implements the KeyValueMemory system for state management
- **Key Components**:
  - Simple key-value storage for memory items
  - Async methods for get/set operations
- **Dependencies**: None

### 2. Memory Managers (Shared Memory Access Layer)

#### `task_context_manager.py`
- **Purpose**: Manages task-level context and status
- **Key Functions**: Initialize tasks, update status, track progress
- **Memory Keys**: taskContext
- **Dependencies**: keyvalue_memory.py, memory_content_types.py

#### `database_schema_manager.py`
- **Purpose**: Manages database schema information in memory
- **Key Functions**: Store/retrieve table schemas, columns, relationships
- **Memory Keys**: schema_[table_name]
- **Dependencies**: keyvalue_memory.py, memory_content_types.py

#### `query_tree_manager.py`
- **Purpose**: Manages the query decomposition tree structure
- **Key Functions**: Create/update/traverse query nodes, manage parent-child relationships
- **Memory Keys**: query_tree
- **Dependencies**: keyvalue_memory.py, memory_content_types.py

#### `node_history_manager.py`
- **Purpose**: Tracks all operations performed on query nodes
- **Key Functions**: Record operations, track retry counts
- **Memory Keys**: node_[nodeId]_history
- **Dependencies**: keyvalue_memory.py, memory_content_types.py

### 3. State Evaluation

#### `task_status_checker.py`
- **Purpose**: Tool for deterministic evaluation of current task state
- **Usage**: Called by orchestrator as a tool to get state overview
- **Key Functions**: 
  - Analyze the whole QueryTree to report task state
  - Get task overview (complete node tree status)
  - Identify current node for processing
  - Apply deterministic node selection rules
- **Node Selection Rules**:
  - Start from root node
  - Process unprocessed nodes
  - Move to next after good result
  - Complete when no unprocessed nodes
- **Dependencies**: All memory managers

### 4. Agent Infrastructure

#### `base_memory_agent.py`
- **Purpose**: Base class for memory-enabled agents
- **Key Components**:
  - Wrapper for AutoGen agents
  - Reader callback integration
  - Parser callback integration
- **Dependencies**: keyvalue_memory.py, autogen

### 5. Specialized Agents (Tools for Orchestrator)

#### `query_analyzer_agent.py`
- **Purpose**: Analyzes user queries and creates query trees
- **Components**:
  - Prompt: Query analysis instructions
  - Reader: Extracts task context from memory
  - Parser: Writes query tree to memory
- **Dependencies**: base_memory_agent.py, memory managers

#### `schema_linker_agent.py`
- **Purpose**: Links relevant schema elements to query nodes
- **Components**:
  - Prompt: Schema linking instructions
  - Reader: Gets schema and current node info
  - Parser: Updates node with schema links
- **Dependencies**: base_memory_agent.py, memory managers

#### `sql_generator_agent.py`
- **Purpose**: Generates SQL queries from node intents
- **Components**:
  - Prompt: SQL generation instructions
  - Reader: Gets node intent and schema links
  - Parser: Writes generated SQL to node
- **Dependencies**: base_memory_agent.py, memory managers

#### `sql_evaluator_agent.py`
- **Purpose**: Evaluates SQL query quality
- **Components**:
  - Prompt: SQL evaluation instructions
  - Reader: Gets SQL and execution results
  - Parser: Updates node with evaluation
- **Dependencies**: base_memory_agent.py, memory managers

### 6. Orchestration Layer

#### `orchestrator_agent.py`
- **Purpose**: LLM-powered state machine coordinator for the entire workflow
- **Key Functions**: 
  - Initialize workflow with task context
  - Use TaskStatusChecker to get current state overview
  - Use LLM to analyze state and decide which agent to call
  - Manage state transitions based on LLM decisions
  - Handle termination conditions
- **LLM Decision Making**:
  - Analyzes current task state from TaskStatusChecker
  - Considers node status, history, and context
  - Decides which agent to call and what action to take
  - Can choose: schema_linker, query_analyzer, sql_generator, sql_evaluator
  - Handles feedback loops when SQL needs improvement
- **Dependencies**: All agents, TaskStatusChecker, memory managers

### 7. Workflow Execution

#### `text_to_sql_tree_orchestrator.py`
- **Purpose**: Main entry point for text-to-SQL conversion
- **Key Functions**: 
  - Setup memory and managers
  - Initialize task context
  - Run orchestrator agent
  - Return final SQL results
- **Dependencies**: orchestrator_agent.py, all managers

### 8. Database Interface

#### `schema_reader.py`
- **Purpose**: Reads database schemas from SQLite databases
- **Key Functions**: Load schema info, extract metadata
- **Dependencies**: sqlite3

#### `sql_executor.py`
- **Purpose**: Executes SQL queries against databases
- **Key Functions**: Execute with timeout, handle errors
- **Dependencies**: sqlite3, func_timeout

### 9. Supporting Modules

#### `prompts.py`
- **Purpose**: Centralized prompt templates for all agents
- **Key Components**: Agent-specific prompt templates
- **Dependencies**: None

#### `utils.py`
- **Purpose**: General utility functions
- **Key Functions**: XML parsing, SQL utilities
- **Dependencies**: None

## System Architecture Layers

### Layer 1: Data Models and Memory (Foundation)
- `memory_content_types.py` - Core data structures
- `keyvalue_memory.py` - Memory implementation

### Layer 2: Memory Management (Shared Memory Access)
- `task_context_manager.py` - Task context
- `database_schema_manager.py` - Schema information
- `query_tree_manager.py` - Query tree structure
- `node_history_manager.py` - Operation history

### Layer 3: State Evaluation
- `task_status_checker.py` - Deterministic state rules

### Layer 4: Agent Infrastructure
- `base_memory_agent.py` - Agent base class

### Layer 5: Specialized Agents (Tools)
- `query_analyzer_agent.py` - Query analysis
- `schema_linker_agent.py` - Schema linking
- `sql_generator_agent.py` - SQL generation
- `sql_evaluator_agent.py` - SQL evaluation

### Layer 6: Orchestration (State Machine)
- `orchestrator_agent.py` - Workflow coordinator

### Layer 7: Workflow Execution
- `text_to_sql_tree_orchestrator.py` - Main entry point

### Layer 8: External Interfaces
- `schema_reader.py` - Database schema reading
- `sql_executor.py` - SQL execution

## Dependency Flow

1. **Foundation → Up**: All modules depend on memory_content_types.py
2. **Memory → Managers**: Managers wrap memory access
3. **Managers → Agents**: Agents use managers for memory access
4. **Agents → Orchestrator**: Orchestrator uses agents as tools
5. **Orchestrator → Workflow**: Workflow initializes and runs orchestrator

## Key Design Patterns

1. **LLM-Powered State Machine Pattern**: Orchestrator uses LLM to manage workflow states
2. **Manager Pattern**: Specialized managers for memory organization
3. **Agent as Tool Pattern**: Agents are tools with consistent interfaces
4. **Callback Pattern**: Reader/parser callbacks for memory integration
5. **Tree Structure**: Query decomposition uses tree data structure

## Workflow Example

```
1. User provides query → text_to_sql_tree_orchestrator
2. Initialize task context in memory
3. Orchestrator starts (LLM-powered state machine begins)
4. Loop:
   a. Orchestrator calls TaskStatusChecker tool to analyze whole QueryTree
   b. TaskStatusChecker reports current task state (deterministic analysis)
   c. Orchestrator LLM analyzes state report and decides which agent to call
   d. Agent reads context (reader callback)
   e. Agent processes with LLM
   f. Agent writes results (parser callback)
   g. State transitions based on LLM decision and results
5. Continue until all nodes have good SQL
6. Return final SQL result
```

## Implementation Guide

### Adding a New Agent

1. **Create Agent Class**
```python
# src/new_agent.py
from src.base_memory_agent import BaseMemoryAgent

class NewAgent(BaseMemoryAgent):
    agent_name = "new_agent"
    
    def __init__(self, llm_config):
        self.prompt = NEW_AGENT_PROMPT
        super().__init__(llm_config)
    
    async def _reader_callback(self, memory, task, token):
        # Extract context from managers
        context = await self.some_manager.get_data()
        return {"context": context}
    
    async def _parser_callback(self, memory, task, result, token):
        # Parse LLM output and update memory
        parsed = self._parse_output(result)
        await self.some_manager.update_data(parsed)
```

2. **Add Prompt to prompts.py**
```python
NEW_AGENT_PROMPT = """
You are a specialized agent that...

<instructions>
- Clear task description
- Expected input format
- Required output format
</instructions>

<output_format>
<result>
  <field1>value1</field1>
  <field2>value2</field2>
</result>
</output_format>
"""
```

3. **Register in Orchestrator**
```python
# Add to orchestrator's available agents
self.agents["new_agent"] = NewAgent(llm_config)
```

### Creating a New Manager

```python
# src/new_data_manager.py
from src.keyvalue_memory import KeyValueMemory
from src.memory_content_types import NewDataType

class NewDataManager:
    def __init__(self, memory: KeyValueMemory):
        self.memory = memory
        self.memory_key = "new_data"
    
    async def set_data(self, data: NewDataType):
        await self.memory.set(self.memory_key, data.to_dict())
    
    async def get_data(self) -> Optional[NewDataType]:
        raw = await self.memory.get(self.memory_key)
        return NewDataType.from_dict(raw) if raw else None
```

### Extending the Orchestrator

1. **Add New Decision Logic**
```python
# In orchestrator's decision prompt
"""Available agents and when to use them:
- new_agent: Use when [specific condition]
"""
```

2. **Handle New States**
```python
# Add to task status checker if needed
def _analyze_new_condition(self, node):
    # Return state analysis for new conditions
    pass
```

## Testing Strategy

### Unit Testing Approach

1. **Test Managers Independently**
```python
import pytest
from src.keyvalue_memory import KeyValueMemory
from src.task_context_manager import TaskContextManager

@pytest.mark.asyncio
async def test_task_manager():
    memory = KeyValueMemory()
    manager = TaskContextManager(memory)
    
    # Test initialization
    await manager.initialize_task("test-1", "query", "db")
    
    # Verify storage
    context = await manager.get_context()
    assert context.taskId == "test-1"
```

2. **Test Agents with Mock LLM**
```python
from unittest.mock import Mock

@pytest.mark.asyncio
async def test_schema_linker():
    mock_llm = Mock()
    mock_llm.generate.return_value = "<tables>users,orders</tables>"
    
    agent = SchemaLinkerAgent(llm_config)
    agent.llm = mock_llm
    
    # Test processing
    result = await agent.process(task)
    assert "users" in result
```

### Integration Testing

1. **Test Agent-Manager Interaction**
```python
@pytest.mark.asyncio
async def test_agent_manager_integration():
    # Setup memory and managers
    memory = KeyValueMemory()
    schema_manager = DatabaseSchemaManager(memory)
    
    # Load test schema
    await schema_manager.load_schema("test.db")
    
    # Run agent
    agent = SchemaLinkerAgent(llm_config)
    result = await agent.process_with_managers(
        schema_manager=schema_manager
    )
```

2. **Test Orchestrator Workflows**
```python
@pytest.mark.asyncio
async def test_simple_query_workflow():
    orchestrator = TextToSQLTreeOrchestrator(llm_config)
    
    result = await orchestrator.run(
        query="SELECT all users",
        database_name="test.db"
    )
    
    assert result.status == "completed"
    assert "SELECT" in result.sql
```

### End-to-End Testing

1. **Test Complete Scenarios**
```python
# tests/test_scenarios.py
scenarios = [
    {
        "name": "simple_select",
        "query": "Show all active customers",
        "expected_tables": ["customers"],
        "expected_keywords": ["SELECT", "WHERE", "active"]
    },
    {
        "name": "complex_join",
        "query": "Total revenue by customer last month",
        "expected_tables": ["customers", "orders"],
        "expected_keywords": ["JOIN", "SUM", "GROUP BY"]
    }
]

for scenario in scenarios:
    result = await orchestrator.run(scenario["query"])
    validate_scenario(result, scenario)
```

2. **Performance Testing**
```python
import time

@pytest.mark.performance
async def test_query_performance():
    start = time.time()
    result = await orchestrator.run("complex query...")
    duration = time.time() - start
    
    assert duration < 30  # Complex queries under 30s
    assert result.status == "completed"
```

### Test Coverage Guidelines

1. **Memory Managers**: 100% coverage
   - All CRUD operations
   - Error handling
   - Edge cases

2. **Agents**: >90% coverage
   - Reader/parser callbacks
   - LLM response parsing
   - Error scenarios

3. **Orchestrator**: >85% coverage
   - State transitions
   - Agent selection logic
   - Termination conditions

4. **Integration**: Key workflows
   - Simple queries
   - Complex queries
   - Error recovery
   - Performance limits

This architecture provides clear separation of concerns, managed state access, and a robust state machine for coordinating complex multi-step SQL generation tasks.