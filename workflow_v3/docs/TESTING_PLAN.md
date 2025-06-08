# Text-to-SQL Workflow v2 Testing Plan

## Table of Contents
1. [Overview](#overview)
2. [Testing Principles](#testing-principles)
3. [Layer 1: Memory Managers](#layer-1-memory-managers-testing)
4. [Layer 2: Individual Agents](#layer-2-individual-agent-testing)
5. [Layer 3: Orchestrator](#layer-3-orchestrator-testing)
6. [Layer 4: Integration](#layer-4-integration-testing)
7. [Layer 5: Edge Cases](#layer-5-edge-cases-and-stress-testing)
8. [Implementation Timeline](#testing-implementation-strategy)
9. [Test Automation](#test-automation-guide)

## Overview
This document outlines a comprehensive layer-by-layer testing strategy for the text-to-SQL workflow system. Testing focuses on three core principles:
1. **Memory Managers**: Verify correct data placement and retrieval
2. **Agents**: Ensure they only prepare context, call LLM, and extract outputs (no business logic)
3. **Orchestrator**: Test intelligent agent coordination and task tree navigation

## Testing Principles

### 1. Isolation Testing
- Test each component independently with mocks
- Verify single responsibility principle
- Focus on input/output contracts

### 2. No Business Logic in Agents
- Agents should NOT make decisions
- All logic should be in prompts for LLM
- Test only context preparation and output parsing

### 3. Deterministic Testing
- Mock LLM responses for predictable tests
- Use fixed test data and schemas
- Ensure reproducible results

## Layer 1: Memory Managers Testing

### 1.1 Task Context Manager
**Purpose**: Test task lifecycle and metadata management
```python
# Critical tests:
- Initialize task with all required fields (taskId, originalQuery, databaseName, startTime, status, evidence)
- Update task status correctly (INITIALIZING → PROCESSING → COMPLETED/FAILED)
- Store/retrieve task context from correct memory location
- Handle missing or invalid task data gracefully
- Verify data persistence across operations

# Memory location verification:
- Data stored at 'task_context' key
- TaskStatus enum conversions work correctly
- Optional evidence field handled properly
```

### 1.2 Database Schema Manager  
**Purpose**: Test schema storage and lookup operations
```python
# Critical tests:
- Store database schema at correct memory location ('database_schema')
- Add tables with complete column information (dataType, nullable, primaryKey, foreignKey)
- Store foreign key relationships correctly
- Cache sample data and metadata properly
- Retrieve schema elements by various queries (table names, column types, relationships)
- Handle multiple databases simultaneously

# Memory location verification:
- Schema stored per database: database_schema[dbName]
- TableSchema and ColumnInfo serialization works
- last_loaded timestamp management
```

### 1.3 Query Tree Manager
**Purpose**: Test tree structure operations and node management
```python
# Critical tests:
- Initialize tree with root node at correct location ('query_tree')
- Add/update/delete nodes with proper parent-child relationships
- Track current node (currentNodeId) correctly
- Store node data at nodes[nodeId] location
- Handle tree traversal and node finding operations
- Update node status (CREATED → SQL_GENERATED → EXECUTED_SUCCESS/FAILED)

# Memory location verification:
- Tree structure at 'query_tree' key
- Individual nodes at query_tree.nodes[nodeId]
- QueryNode serialization preserves all agent outputs
```

### 1.4 Node History Manager
**Purpose**: Test operation history and retry tracking
```python
# Critical tests:
- Record operations at correct location ('node_history')
- Track retry counts per node accurately
- Store processing events with timestamps
- Retrieve operation history by node/type
- Handle retry limit enforcement (MAX_RETRIES = 3)

# Memory location verification:
- History stored at node_history[nodeId]
- NodeOperation serialization works correctly
- Timestamp and retry count tracking accurate
```

## Layer 2: Individual Agent Testing

### 2.1 Schema Linker Agent
**Purpose**: Test LLM-based schema element selection (NO business logic)
```python
# Agent responsibility verification:
1. CONTEXT PREPARATION:
   - Read database schema from DatabaseSchemaManager
   - Read user query from current node intent
   - Format schema into LLM-friendly XML/text

2. LLM INTERACTION:
   - Send properly formatted prompt to LLM
   - Handle LLM response parsing (XML extraction)
   - Robust parsing with fallback strategies

3. OUTPUT EXTRACTION & STORAGE:
   - Extract selected tables/columns from LLM response
   - Store results in node.schema_linking field
   - NO validation logic - trust LLM output
   - NO schema analysis logic - LLM decides

# Test scenarios:
- Simple single-table queries
- Multi-table join requirements  
- Complex schema with many relationships
- Edge cases (ambiguous table names, missing schema)

# Verify agent does NOT:
- Implement table selection logic in code
- Validate schema selections
- Make decisions about query complexity
```

### 2.2 Query Analyzer Agent  
**Purpose**: Test LLM-based query decomposition (NO business logic)
```python
# Agent responsibility verification:
1. CONTEXT PREPARATION:
   - Read user query and schema analysis context
   - Format query complexity assessment prompt
   - Include schema context when available

2. LLM INTERACTION:
   - Send query analysis prompt to LLM
   - Parse LLM response for complexity and decomposition
   - Handle XML/text extraction reliably

3. OUTPUT EXTRACTION & STORAGE:
   - Extract complexity classification from LLM
   - Store decomposition results in node.decomposition field
   - Create child nodes only if LLM requests decomposition
   - NO hardcoded complexity rules

# Test scenarios:
- Simple queries (should not decompose)
- Complex queries requiring subqueries
- Queries with/without schema context
- Malformed or ambiguous queries

# Verify agent does NOT:
- Implement query complexity logic in code
- Make decomposition decisions independently
- Apply hardcoded query patterns
```

### 2.3 SQL Generator Agent
**Purpose**: Test LLM-based SQL generation (NO business logic)  
```python
# Agent responsibility verification:
1. CONTEXT PREPARATION:
   - Read schema_linking results from current node
   - Read node intent and evidence
   - Format selected schema elements for LLM

2. LLM INTERACTION:
   - Send SQL generation prompt with schema context
   - Parse LLM response for SQL and explanations
   - Handle multiple output formats (XML, code blocks)

3. OUTPUT EXTRACTION & STORAGE:
   - Extract SQL from LLM response (with fallback parsing)
   - Store SQL and explanation in node.generation field
   - NO SQL validation or modification
   - NO query optimization logic

# Test scenarios:
- Simple SELECT queries
- Multi-table JOINs
- Aggregation queries with GROUP BY
- Subqueries and CTEs
- Complex analytical queries

# Verify agent does NOT:
- Modify or validate generated SQL
- Implement SQL optimization logic
- Make decisions about SQL structure
```

### 2.4 SQL Evaluator Agent
**Purpose**: Test LLM-based SQL evaluation (NO business logic)
```python
# Agent responsibility verification:
1. CONTEXT PREPARATION:
   - Execute SQL using SQLExecutor
   - Format execution results and errors
   - Prepare evaluation context for LLM

2. LLM INTERACTION:
   - Send evaluation prompt with execution results
   - Parse LLM response for quality assessment
   - Handle evaluation criteria extraction

3. OUTPUT EXTRACTION & STORAGE:
   - Extract result quality, correctness, intent matching
   - Store evaluation in node.evaluation field
   - NO hardcoded evaluation logic
   - NO automatic retry decisions

# Test scenarios:
- Successful SQL execution
- Failed SQL with syntax errors
- Incorrect results (wrong data)
- Performance issues
- Partially correct results

# Verify agent does NOT:
- Implement evaluation scoring logic
- Make automatic retry decisions
- Validate results against expected patterns
```

## Layer 3: Orchestrator Testing

### 3.1 Orchestrator Agent Coordination
**Purpose**: Test intelligent agent selection and task tree navigation
```python
# Core orchestrator responsibilities:
1. ANALYZE CURRENT STATE:
   - Read task status from TaskStatusChecker
   - Understand node states and error conditions
   - Identify missing data (schema, SQL, evaluation)

2. MAKE INTELLIGENT DECISIONS:
   - Choose appropriate agent based on missing data
   - Handle error-driven context changes
   - Navigate feedback loops for SQL improvement

3. COORDINATE AGENT EXECUTION:
   - Call selected agents with proper context
   - Monitor agent completion
   - Update task state appropriately

# Test decision matrix:
- No schema analysis → call schema_linker
- No query decomposition → call query_analyzer  
- Node missing SQL → call sql_generator
- Node has SQL but no evaluation → call sql_evaluator
- Bad SQL with schema errors → call schema_linker again
- Bad SQL with logic errors → call query_analyzer again
- Good SQL → move to next node via TaskStatusChecker

# Error recovery scenarios:
1. Schema linking failure → retry or try different approach
2. SQL generation failure → get more schema context
3. SQL execution failure → analyze error type and choose appropriate agent
4. Evaluation failure → retry with different context

# Multi-node coordination:
1. Simple query (single node workflow)
2. Complex query (decomposed into multiple nodes)
3. Failed node requiring revision
4. Parallel node processing for independent subqueries
```

### 3.2 Feedback Loop Testing
**Purpose**: Test iterative improvement through agent coordination
```python
# Feedback loop patterns:
1. SCHEMA ERRORS → Schema Linker:
   - "table X doesn't exist" → get better schema analysis
   - "column Y not found" → re-examine schema mapping

2. LOGIC ERRORS → Query Analyzer:
   - "results don't match intent" → re-analyze query complexity
   - "missing conditions" → decompose query differently

3. SYNTAX ERRORS → SQL Generator:
   - "SQL syntax error" → regenerate with better context
   - "invalid join" → provide clearer schema relationships

# Test termination conditions:
- SUCCESS: Good SQL with acceptable evaluation
- MAX_RETRIES: Node exceeded retry limit (3 attempts)
- UNRECOVERABLE: Error type that can't be improved
- TIMEOUT: Process took too long

# Verify orchestrator:
- Makes logical agent selection decisions
- Builds effective feedback loops
- Terminates appropriately
- Maintains context across iterations
```

## Layer 4: Integration Testing

### 4.1 Complete Workflow Tests
**Purpose**: Test end-to-end scenarios with real data
```python
# Test databases:
1. Simple e-commerce (customers, orders, products)
2. University system (students, courses, enrollments)  
3. Financial system (accounts, transactions, balances)

# Query complexity levels:
1. Single table selection: "Show all customers"
2. Simple joins: "Show customer orders"
3. Aggregations: "Count orders per customer"
4. Complex analytics: "Top customers by revenue this year"
5. Multi-step decomposition: "Compare this month vs last month sales"

# Success metrics:
- Correct SQL generation (>90% accuracy)
- Appropriate agent selection
- Efficient feedback loops (minimal retries)
- Clear error handling and recovery
- Complete task tree execution
```

## Layer 5: Edge Cases and Stress Testing

### 5.1 Memory Manager Stress Tests
```python
# Test memory manager limits:
- Large schemas (100+ tables) in DatabaseSchemaManager
- Deep query trees (10+ nodes) in QueryTreeManager  
- High retry counts in NodeHistoryManager
- Concurrent memory operations
- Memory cleanup and garbage collection
```

### 5.2 Agent Robustness Tests
```python
# Test agent error handling:
- Malformed LLM responses (invalid XML)
- Partial LLM outputs (cut-off responses)
- Empty or null LLM responses
- Extreme schema complexity
- Ambiguous queries with no clear intent
```

### 5.3 Orchestrator Edge Cases
```python
# Test orchestrator resilience:
- Circular retry loops (prevent infinite loops)
- All agents failing repeatedly
- Memory corruption scenarios
- Invalid task states
- Timeout handling
```

## Test Automation Guide

### Setting Up Test Environment

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-mock pytest-cov

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test layer
pytest tests/test_layer1_memory_managers.py -v
```

### Test Structure

```python
# tests/conftest.py
import pytest
from src.keyvalue_memory import KeyValueMemory

@pytest.fixture
async def memory():
    """Provide clean memory instance for each test"""
    return KeyValueMemory()

@pytest.fixture
def mock_llm():
    """Mock LLM for agent testing"""
    from unittest.mock import Mock
    llm = Mock()
    llm.generate = Mock(return_value="<result>test</result>")
    return llm
```

### Writing Effective Tests

```python
# Example: Testing a manager
@pytest.mark.asyncio
async def test_task_manager_lifecycle(memory):
    manager = TaskContextManager(memory)
    
    # Test initialization
    await manager.initialize_task(
        taskId="test-1",
        originalQuery="Show all users",
        databaseName="test.db"
    )
    
    # Verify storage
    context = await manager.get_context()
    assert context.taskId == "test-1"
    assert context.status == TaskStatus.INITIALIZING
    
    # Test status update
    await manager.update_status(TaskStatus.PROCESSING)
    context = await manager.get_context()
    assert context.status == TaskStatus.PROCESSING
```

## Testing Implementation Strategy

### Phase 1: Memory Managers (Week 1)
```python
# Priority order:
1. KeyValueMemory basic operations
2. TaskContextManager lifecycle  
3. QueryTreeManager node operations
4. DatabaseSchemaManager storage/retrieval
5. NodeHistoryManager retry tracking

# Success criteria:
- All memory operations store data in correct locations
- Serialization/deserialization works perfectly
- No data loss during operations
- Clean error handling for invalid inputs
```

### Phase 2: Individual Agents (Week 2)  
```python
# Priority order:
1. Schema Linker (foundation for others)
2. Query Analyzer (controls decomposition)
3. SQL Generator (core functionality)
4. SQL Evaluator (feedback generation)

# Success criteria:
- Agents contain NO business logic
- Only context preparation, LLM calls, output extraction
- Robust parsing handles all LLM response formats
- Clean separation between agent code and LLM decisions
```

### Phase 3: Orchestrator Coordination (Week 3)
```python
# Priority order:
1. Basic agent selection logic
2. Error-driven feedback loops
3. Multi-node coordination
4. Termination conditions

# Success criteria:
- Intelligent agent selection based on node state
- Effective feedback loops improve SQL quality
- Proper task tree navigation
- Clean termination (success/failure/timeout)
```

### Phase 4: Integration & Performance (Week 4)
```python
# Complete workflow testing:
- End-to-end query processing
- Performance benchmarking
- Error recovery validation
- Real-world database testing

# Success criteria:
- >90% SQL accuracy on test queries
- <5s for simple queries, <30s for complex
- Robust error handling and recovery
- Efficient memory usage (<1GB typical workloads)
```

## Test Data Requirements

### Sample Databases
1. **E-commerce** (customers, orders, products, categories)
2. **University** (students, courses, enrollments, professors) 
3. **Financial** (accounts, transactions, balances, customers)

### Query Test Sets
1. **Simple queries** (50): Single table, basic WHERE clauses
2. **Medium queries** (30): 2-3 table joins, aggregations
3. **Complex queries** (20): Multiple joins, subqueries, analytics
4. **Edge cases** (10): Ambiguous, malformed, impossible queries

## Success Metrics

### Memory Managers
- [ ] 100% data integrity (no lost or corrupted data)
- [ ] Correct memory location usage
- [ ] Clean error handling for all edge cases

### Agents  
- [ ] Zero business logic in agent code
- [ ] Robust LLM response parsing (>95% success rate)
- [ ] Clean context preparation and output extraction

### Orchestrator
- [ ] Intelligent agent selection (>90% appropriate choices)
- [ ] Effective feedback loops (SQL quality improves with iterations)
- [ ] Proper termination (no infinite loops, clear success/failure)

### Integration
- [ ] >90% end-to-end success rate on test queries
- [ ] Performance targets met (5s simple, 30s complex)
- [ ] Memory efficiency maintained
- [ ] Clear error reporting and recovery

## Continuous Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Test Suite

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install -r requirements-test.txt
    
    - name: Run tests
      run: |
        pytest tests/ -v --cov=src --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v1
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: tests
        name: tests
        entry: pytest tests/test_layer1_memory_managers.py -v
        language: system
        pass_filenames: false
        always_run: true
```

## Test Maintenance

### Regular Reviews
1. **Weekly**: Review failing tests and fix
2. **Monthly**: Update test data and scenarios
3. **Quarterly**: Performance benchmark review
4. **Release**: Full regression testing

### Test Documentation
- Keep test names descriptive
- Add docstrings explaining test purpose
- Document any complex test setup
- Maintain test data documentation