# Text-to-SQL Workflow v2 Testing Plan

## Overview
This document outlines a comprehensive layer-by-layer testing strategy for the text-to-SQL workflow system. Testing proceeds from foundational components up to the complete orchestrated workflow.

## Layer 1: Core Infrastructure Testing

### 1.1 Memory Types (`test_memory_types.ipynb`)
**Purpose**: Test all data structures and their serialization/deserialization
```python
# Test areas:
- TaskContext creation and conversion
- TableSchema and ColumnInfo structures
- QueryNode with all fields
- NodeOperation recording
- Enum conversions (TaskStatus, NodeStatus, etc.)
```

### 1.2 KeyValueMemory (`test_memory.ipynb`)
**Purpose**: Test memory storage and retrieval
```python
# Test areas:
- Basic set/get operations
- Complex nested data storage
- Memory persistence across operations
- Error handling for missing keys
```

## Layer 2: Memory Managers Testing

### 2.1 Task Context Manager (`test_task_context.ipynb`)
**Purpose**: Test task lifecycle management
```python
# Test areas:
- Task initialization with all required fields
- Status updates (initializing -> processing -> completed/failed)
- Getter methods for task properties
- Error handling for invalid states
```

### 2.2 Database Schema Manager (`test_schema_manager.ipynb`)
**Purpose**: Test schema storage and retrieval
```python
# Test areas:
- Adding tables with columns
- Foreign key relationships
- Sample data storage
- Schema queries (find columns by type, get relationships)
- Metadata management
```

### 2.3 Query Tree Manager (`test_query_tree.ipynb`)
**Purpose**: Test tree structure operations
```python
# Test areas:
- Tree initialization with root node
- Adding/updating/deleting nodes
- Parent-child relationships
- Tree traversal methods
- Node status updates
- Finding nodes by various criteria
```

### 2.4 Node History Manager (`test_node_history.ipynb`)
**Purpose**: Test operation history tracking
```python
# Test areas:
- Recording all operation types
- Retrieving operations by node/type
- Node lifecycle tracking
- History queries and summaries
```

## Layer 3: Database Integration Testing

### 3.1 Schema Reader (`test_schema_reader.ipynb`)
**Purpose**: Test database schema loading
```python
# Test areas:
- Loading schema from SQLite databases
- XML schema generation
- Foreign key detection
- Sample data extraction
- Schema pruning for complex databases
```

### 3.2 SQL Executor (`test_sql_executor.ipynb`)
**Purpose**: Test SQL execution
```python
# Test areas:
- Basic SELECT queries
- JOIN queries
- Aggregation queries
- Error handling for invalid SQL
- Result formatting
```

## Layer 4: Individual Agent Testing

### 4.1 Query Analyzer Agent (`test_query_analyzer.ipynb`)
**Purpose**: Test query analysis and decomposition
```python
# Test scenarios:
1. Simple query (single table select)
2. Medium query (join two tables)
3. Complex query (requiring decomposition)
4. Aggregation query
5. Nested query requirements

# Verify:
- Correct intent extraction
- Proper complexity classification
- Decomposition logic
- Tree structure creation
```

### 4.2 Schema Linking Agent (`test_schema_linking.ipynb`)
**Purpose**: Test schema element selection
```python
# Test scenarios:
1. Simple table/column selection
2. Multi-table join identification
3. Implicit column requirements
4. Aggregation column selection
5. Complex relationship detection

# Verify:
- All required tables selected
- Minimal column selection
- Correct join relationships
- Purpose annotations
```

### 4.3 SQL Generator Agent (`test_sql_generator.ipynb`)
**Purpose**: Test SQL generation from mappings
```python
# Test scenarios:
1. Simple SELECT with WHERE
2. INNER JOIN query
3. LEFT JOIN with NULL handling
4. GROUP BY with aggregations
5. Subquery generation
6. CTE (WITH clause) queries

# Verify:
- Syntactically correct SQL
- Proper use of mapped schema
- Correct join conditions
- Appropriate filtering
```

### 4.4 SQL Executor Agent (`test_sql_executor_agent.ipynb`)
**Purpose**: Test execution and evaluation
```python
# Test scenarios:
1. Successful execution evaluation
2. Failed execution handling
3. Performance analysis
4. Result validation
5. Improvement suggestions

# Verify:
- Execution status updates
- Error capture and reporting
- Performance metrics
- Improvement recommendations
```

## Layer 5: Orchestrator Testing

### 5.1 Orchestrator Agent (`test_orchestrator.ipynb`)
**Purpose**: Test end-to-end workflow coordination
```python
# Test scenarios:
1. Simple query full workflow
2. Complex query with decomposition
3. Error recovery (failed SQL)
4. Iterative improvement
5. Multi-node coordination

# Verify:
- Correct tool selection
- State-based decisions
- Error handling and retry
- Final result compilation
```

## Layer 6: Integration Testing

### 6.1 Full Workflow Tests (`test_integration.ipynb`)
**Purpose**: Test complete scenarios
```python
# Test databases:
1. Simple e-commerce DB
2. Complex university DB
3. Financial DB with many tables

# Test queries:
1. Basic selection queries
2. Multi-table joins
3. Aggregations and grouping
4. Complex analytical queries
5. Nested subqueries

# Verify:
- End-to-end success
- Performance metrics
- Result accuracy
- Error handling
```

## Layer 7: Stress and Edge Case Testing

### 7.1 Stress Tests (`test_stress.ipynb`)
**Purpose**: Test system limits
```python
# Test areas:
- Large schemas (100+ tables)
- Deep query trees (10+ nodes)
- Complex queries with many joins
- Concurrent operations
- Memory usage patterns
```

### 7.2 Edge Cases (`test_edge_cases.ipynb`)
**Purpose**: Test unusual scenarios
```python
# Test areas:
- Empty schemas
- Queries with no valid SQL
- Circular dependencies
- Invalid table references
- Malformed queries
```

## Testing Utilities

### Create Test Helpers (`test_helpers.py`)
```python
# Utilities:
- Sample database creators
- Query generators
- Result validators
- Performance benchmarking
- Test data fixtures
```

## Execution Order

1. **Week 1**: Layers 1-2 (Core infrastructure and managers)
2. **Week 2**: Layer 3-4 (Database integration and agents)
3. **Week 3**: Layer 5-6 (Orchestrator and integration)
4. **Week 4**: Layer 7 (Stress tests and refinement)

## Success Criteria

- [ ] All unit tests pass (>95% coverage)
- [ ] Integration tests handle 20+ query patterns
- [ ] Performance: <5s for simple queries, <30s for complex
- [ ] Error recovery works for common failures
- [ ] Memory usage stays under 1GB for typical workloads
- [ ] Clear logging and debugging information

## Test Data Requirements

1. **Sample Databases**:
   - E-commerce (5-10 tables)
   - University (10-15 tables)
   - Healthcare (15-20 tables)
   - Financial (20-30 tables)

2. **Query Sets**:
   - 50 simple queries
   - 30 medium queries
   - 20 complex queries
   - 10 edge cases

## Monitoring and Metrics

Track for each test:
- Execution time
- Memory usage
- SQL accuracy
- Number of retries
- Error types and frequencies