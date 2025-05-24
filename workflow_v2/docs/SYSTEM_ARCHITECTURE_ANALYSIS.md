# Workflow_v2 System Architecture Analysis

## Overview
The workflow_v2 system is a sophisticated text-to-SQL conversion system that uses a multi-agent architecture with memory management to convert natural language queries into SQL queries.

## File List and Purposes

### 1. Core Data Types and Memory System

#### `memory_types.py`
- **Purpose**: Defines all shared data structures used across the system
- **Key Components**:
  - Enums: TaskStatus, NodeStatus, NodeOperationType, CombineStrategyType
  - Dataclasses: TaskContext, ColumnInfo, TableSchema, TableMapping, ColumnMapping, JoinMapping, QueryMapping, CombineStrategy, ExecutionResult, QueryNode, NodeOperation
- **Dependencies**: None (base module)

#### `memory.py`
- **Purpose**: Implements the KeyValueMemory system for state management
- **Key Components**:
  - KeyValueMemory class implementing autogen_core.Memory protocol
  - Methods for storing/retrieving MemoryContent items
- **Dependencies**: 
  - External: autogen_core
  - Internal: None

### 2. Memory Managers

#### `task_context_manager.py`
- **Purpose**: Manages task-level context and status
- **Key Functions**: Initialize tasks, update status, track progress
- **Dependencies**: memory.py, memory_types.py

#### `database_schema_manager.py`
- **Purpose**: Manages database schema information in memory
- **Key Functions**: Store/retrieve table schemas, columns, relationships
- **Dependencies**: memory.py, memory_types.py

#### `query_tree_manager.py`
- **Purpose**: Manages the query decomposition tree structure
- **Key Functions**: Create/update/traverse query nodes, manage parent-child relationships
- **Dependencies**: memory.py, memory_types.py

#### `node_history_manager.py`
- **Purpose**: Tracks all operations performed on query nodes
- **Key Functions**: Record create/update/execute/delete operations
- **Dependencies**: memory.py, memory_types.py

### 3. Agent Infrastructure

#### `memory_agent_tool.py`
- **Purpose**: Base class for memory-enabled agents
- **Key Components**:
  - MemoryAgentTool class wrapping agents with memory callbacks
  - Pre/post processing callbacks for memory integration
- **Dependencies**: memory.py, autogen_core, autogen_agentchat

### 4. Specialized Agents

#### `orchestrator_agent.py`
- **Purpose**: Main coordinator agent that manages the entire workflow
- **Key Functions**: Initialize workflow, coordinate agents, make decisions
- **Dependencies**: All managers, all agents, memory.py, schema_reader.py, sql_executor.py

#### `query_analyzer_agent.py`
- **Purpose**: Analyzes user queries and creates query trees
- **Key Functions**: Parse intent, determine complexity, decompose complex queries
- **Dependencies**: All managers, memory_agent_tool.py

#### `schema_linking_agent.py`
- **Purpose**: Links relevant schema elements to query nodes
- **Key Functions**: Select tables/columns, identify joins, create mappings
- **Dependencies**: Database/query/history managers, memory_agent_tool.py

#### `sql_generator_agent.py`
- **Purpose**: Generates SQL queries from node intents and mappings
- **Key Functions**: Create SQL, handle different query types, combine subqueries
- **Dependencies**: Database/query/history managers, memory_agent_tool.py

#### `sql_executor_agent.py`
- **Purpose**: Executes SQL and evaluates results
- **Key Functions**: Execute queries, analyze results, suggest improvements
- **Dependencies**: All managers, memory_agent_tool.py, sql_executor.py

### 5. Database and Schema Management

#### `schema_reader.py`
- **Purpose**: Reads database schemas from SQLite databases
- **Key Functions**: Load schema info, generate XML descriptions, handle different datasets
- **Dependencies**: utils.py, External: sqlite3

#### `sql_executor.py`
- **Purpose**: Executes SQL queries against databases
- **Key Functions**: Execute with timeout, validate results, handle errors
- **Dependencies**: External: sqlite3, func_timeout

### 6. Workflow Utilities

#### `workflow_utils.py`
- **Purpose**: High-level workflow functions
- **Key Functions**: Step-by-step execution, combined workflows, result formatting
- **Dependencies**: utils.py

#### `workflow_tools.py`
- **Purpose**: Tool configurations for agents
- **Key Functions**: Configure database tools, schema tools, execution tools
- **Dependencies**: None (configuration module)

#### `workflow_runners.py`
- **Purpose**: Runner functions for different execution modes
- **Key Functions**: Step-by-step execution, batch processing, result display
- **Dependencies**: workflow_utils.py

### 7. Supporting Modules

#### `const.py`
- **Purpose**: Constants and prompt templates
- **Key Components**: Agent names, template strings, patterns
- **Dependencies**: None

#### `utils.py`
- **Purpose**: General utility functions
- **Key Functions**: File I/O, JSON/XML parsing, SQL parsing, data validation
- **Dependencies**: const.py

#### `test_cases.py`
- **Purpose**: Sample test cases for evaluation
- **Key Components**: BIRD and Spider test case definitions
- **Dependencies**: None

### 8. Jupyter Notebooks (Test Files)
- Various `.ipynb` files for testing individual components
- Not part of the core system

## System Architecture Layers

### Layer 1: Data Models and Memory (Foundation)
- `memory_types.py` - Core data structures
- `memory.py` - Memory implementation

### Layer 2: Memory Management
- `task_context_manager.py`
- `database_schema_manager.py`
- `query_tree_manager.py`
- `node_history_manager.py`

### Layer 3: Database Interface
- `schema_reader.py`
- `sql_executor.py`

### Layer 4: Agent Infrastructure
- `memory_agent_tool.py`

### Layer 5: Specialized Agents
- `query_analyzer_agent.py`
- `schema_linking_agent.py`
- `sql_generator_agent.py`
- `sql_executor_agent.py`

### Layer 6: Orchestration
- `orchestrator_agent.py`

### Layer 7: Workflow Management
- `workflow_utils.py`
- `workflow_tools.py`
- `workflow_runners.py`

### Layer 8: Configuration and Testing
- `const.py`
- `utils.py`
- `test_cases.py`

## Dependency Flow

1. **Foundation → Up**: All modules depend on memory_types.py and memory.py
2. **Managers → Agents**: Agents use managers to access/modify memory
3. **Schema/SQL → Agents**: Database interfaces used by agents
4. **Agents → Orchestrator**: Orchestrator coordinates all agents
5. **All → Workflow**: Workflow layers use everything below

## Key Design Patterns

1. **Memory-Centric Architecture**: All state is stored in KeyValueMemory
2. **Manager Pattern**: Specialized managers provide clean APIs for memory access
3. **Agent Tool Pattern**: Agents wrapped with memory callbacks
4. **Tree Structure**: Query decomposition uses tree data structure
5. **Event Sourcing**: Node history tracks all operations

## Testing Strategy

Based on the layered architecture, testing should proceed from bottom to top:

1. **Unit Tests**: Test each layer independently
2. **Integration Tests**: Test layer interactions
3. **End-to-End Tests**: Test complete workflows

This analysis provides a comprehensive understanding of the system architecture and dependencies.