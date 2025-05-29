# Shared Memory Design

## Overview

The shared memory system provides a centralized state management solution for the text-to-SQL workflow. It uses a key-value store with structured data types and is accessed exclusively through manager classes.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        Agents Layer                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │
│  │ Schema   │ │  Query   │ │   SQL    │ │   SQL    │     │
│  │ Linker   │ │ Analyzer │ │Generator │ │Evaluator │     │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘     │
│       │            │            │            │              │
│       └────────────┴────────────┴────────────┘              │
│                         │                                    │
├─────────────────────────┼────────────────────────────────────┤
│                    Manager Layer                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │TaskContext   │ │DatabaseSchema│ │ QueryTree    │        │
│  │Manager       │ │Manager       │ │ Manager      │        │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘        │
│         │                │                │                  │
│  ┌──────┴──────────────┴──────────────┴─────┐              │
│  │         KeyValueMemory Store              │              │
│  └───────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

## Core Memory Layout

The shared memory is organized into several key sections managed by specialized managers:

```xml
<shared_memory>
  <!-- Task Context (Managed by TaskContextManager) -->
  <task_context>
    <taskId>string</taskId>
    <originalQuery>string</originalQuery>
    <databaseName>string</databaseName>
    <startTime>string</startTime>
    <status>TaskStatus</status>
    <evidence>string</evidence>
  </task_context>
  
  <!-- Query Tree (Managed by QueryTreeManager) -->
  <query_tree>
    <rootId>string</rootId>
    <currentNodeId>string</currentNodeId>
    <nodes>
      <node id="nodeId">
        <!-- QueryNode structure -->
      </node>
    </nodes>
  </query_tree>
  
  <!-- Database Schema (Managed by DatabaseSchemaManager) -->
  <database_schema>
    <database name="dbName">
      <tables>
        <!-- TableSchema list -->
      </tables>
      <relationships>
        <!-- Foreign key relationships -->
      </relationships>
      <last_loaded>string</last_loaded>
    </database>
  </database_schema>
  
  <!-- Node Processing History (Managed by NodeHistoryManager) -->
  <node_history>
    <node id="nodeId">
      <retry_count>int</retry_count>
      <processing_events>
        <!-- ProcessingEvent list -->
      </processing_events>
      <last_updated>string</last_updated>
    </node>
  </node_history>
</shared_memory>
```

## Data Structures

### Core Types (from memory_content_types.py)

```python
# Enums
class TaskStatus(Enum):
    INITIALIZING = "initializing"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class NodeStatus(Enum):
    CREATED = "created"
    SQL_GENERATED = "sql_generated"
    EXECUTED_SUCCESS = "executed_success"
    EXECUTED_FAILED = "executed_failed"
    REVISED = "revised"

# Main Data Classes
@dataclass
class TaskContext:
    taskId: str
    originalQuery: str
    databaseName: str
    startTime: datetime
    status: TaskStatus
    evidence: Optional[str]

@dataclass
class QueryNode:
    nodeId: str
    status: NodeStatus
    parentId: Optional[str]
    childIds: List[str]
    intent: str
    evidence: Optional[str]
    
    # Agent outputs
    schema_linking: Dict[str, Any]
    decomposition: Optional[Dict[str, Any]]
    generation: Dict[str, Any]
    evaluation: Dict[str, Any]

@dataclass
class TableSchema:
    name: str
    columns: List[ColumnInfo]
    sample_data: Optional[List[Dict]]
    metadata: Dict[str, Any]

@dataclass
class ColumnInfo:
    name: str
    dataType: str
    nullable: bool
    primaryKey: bool
    foreignKey: Optional[ForeignKeyInfo]
    typical_values: Optional[List[Any]]
```

## Manager Classes

### 1. TaskContextManager
**Purpose**: Manages task lifecycle and metadata

```python
class TaskContextManager:
    # Key operations
    async def initialize_task(taskId, query, database, evidence)
    async def get_context() -> TaskContext
    async def update_status(status: TaskStatus)
    
    # Memory location: 'task_context'
```

### 2. DatabaseSchemaManager
**Purpose**: Manages database schema information

```python
class DatabaseSchemaManager:
    # Key operations
    async def load_schema(database_name)
    async def get_table_schema(table_name) -> TableSchema
    async def get_all_tables() -> List[TableSchema]
    async def find_related_tables(table_name) -> List[str]
    
    # Memory location: 'database_schema'
```

### 3. QueryTreeManager
**Purpose**: Manages query decomposition tree

```python
class QueryTreeManager:
    # Key operations
    async def initialize_tree(root_intent)
    async def add_node(parent_id, intent) -> QueryNode
    async def update_node(node_id, updates)
    async def get_current_node() -> QueryNode
    async def set_current_node(node_id)
    async def get_tree() -> Dict
    
    # Memory location: 'query_tree'
```

### 4. NodeHistoryManager
**Purpose**: Tracks processing history and retries

```python
class NodeHistoryManager:
    # Key operations
    async def record_operation(node_id, operation_type, details)
    async def get_retry_count(node_id) -> int
    async def increment_retry(node_id)
    async def get_node_history(node_id) -> List[ProcessingEvent]
    
    # Memory location: 'node_history'
```

## Memory Access Patterns

### ❌ Direct Access (Never Do This)
```python
# Bad - Direct memory manipulation
memory.set("task_context", {"status": "processing"})
task = memory.get("task_context")
```

### ✅ Manager Access (Always Do This)
```python
# Good - Manager-mediated access
await task_manager.update_status(TaskStatus.PROCESSING)
task = await task_manager.get_context()
```

## Implementation Guidelines

### For New Managers

1. **Single Responsibility**: Each manager handles one data domain
2. **Type Safety**: Use typed dataclasses from memory_content_types.py
3. **Async Operations**: All methods should be async
4. **Error Handling**: Graceful handling of missing data
5. **Serialization**: Handle dataclass to/from dict conversions

### Manager Template
```python
class NewDataManager:
    def __init__(self, memory: KeyValueMemory):
        self.memory = memory
        self.memory_key = "data_domain"
    
    async def get_data(self) -> DataType:
        raw_data = await self.memory.get(self.memory_key)
        if not raw_data:
            return None
        return DataType.from_dict(raw_data)
    
    async def set_data(self, data: DataType):
        await self.memory.set(self.memory_key, data.to_dict())
```

## Best Practices

### 1. Data Consistency
- Always use managers for memory operations
- Validate data before storing
- Handle missing data gracefully
- Use transactions for multi-step updates

### 2. Performance
- Cache frequently accessed data in managers
- Minimize memory reads/writes
- Use batch operations when possible
- Clean up old data periodically

### 3. Error Handling
```python
try:
    schema = await schema_manager.get_table_schema("users")
    if not schema:
        # Handle missing schema
        logger.warning("Table 'users' not found")
except Exception as e:
    logger.error(f"Failed to get schema: {e}")
    # Graceful degradation
```

### 4. Testing
- Mock memory for unit tests
- Test manager operations independently
- Verify data persistence
- Test concurrent access scenarios

## Memory Lifecycle

### 1. Initialization
```python
# 1. Create memory instance
memory = KeyValueMemory()

# 2. Initialize managers
task_manager = TaskContextManager(memory)
schema_manager = DatabaseSchemaManager(memory)
tree_manager = QueryTreeManager(memory)
history_manager = NodeHistoryManager(memory)

# 3. Initialize task
await task_manager.initialize_task(...)
```

### 2. Processing
```python
# Agents read context
context = await task_manager.get_context()
schema = await schema_manager.get_table_schema(table)

# Agents write results
await tree_manager.update_node(node_id, {
    "generation": {"sql": generated_sql}
})
```

### 3. Cleanup
```python
# Mark task complete
await task_manager.update_status(TaskStatus.COMPLETED)

# Optional: Clear memory for next task
await memory.clear()
```

## Debugging Memory Issues

### 1. Inspect Memory State
```python
# View all memory keys
all_keys = await memory.list_keys()
print(f"Memory keys: {all_keys}")

# Inspect specific data
task_data = await memory.get("task_context")
print(f"Task context: {task_data}")
```

### 2. Common Issues

**Issue**: Data not persisting between agent calls
- **Cause**: Not using await on async operations
- **Fix**: Ensure all manager calls use await

**Issue**: Stale data in memory
- **Cause**: Reading before writing completes
- **Fix**: Proper async/await usage

**Issue**: Memory growing too large
- **Cause**: Not cleaning up old data
- **Fix**: Implement cleanup in managers

### 3. Memory Monitoring
```python
# Add memory size tracking
class MemoryMonitor:
    async def get_memory_stats(self):
        return {
            "total_keys": len(await memory.list_keys()),
            "task_count": 1,  # Current task
            "node_count": len(tree.nodes),
            "schema_tables": len(schema.tables)
        }
```

## Future Enhancements

1. **Persistence Layer**: Add database backing for memory
2. **Memory Transactions**: ACID compliance for complex updates
3. **Memory Versioning**: Track changes over time
4. **Memory Sharding**: Scale across multiple instances
5. **Memory Analytics**: Track usage patterns and optimize