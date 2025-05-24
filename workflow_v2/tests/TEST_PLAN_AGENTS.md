# Comprehensive Test Plan for MAC-SQL Agent Tools

## Overview
This document outlines the test strategy for the 4 core agent tools in the MAC-SQL Workflow v2 system:
1. QueryAnalyzerAgent
2. SchemaLinkingAgent  
3. SQLGeneratorAgent
4. SQLExecutorAgent

## Testing Principles
- **Isolation**: Each agent should be tested independently with mocked dependencies
- **Integration**: Agent interactions should be tested in realistic workflows
- **Determinism**: Use mocked LLM responses for predictable test outcomes
- **Coverage**: Test normal cases, edge cases, and error scenarios
- **Performance**: Include tests for response time and resource usage

## 1. QueryAnalyzerAgent Test Cases

### Core Functionality Tests
1. **Simple Query Analysis**
   - Single table SELECT queries
   - Basic WHERE conditions
   - Simple ORDER BY/LIMIT
   - Expected: Single QueryNode with SELECT type

2. **Complex Query Decomposition**
   - Multi-step analytical queries
   - Queries requiring intermediate calculations
   - Nested business logic
   - Expected: Query tree with multiple nodes and combination strategy

3. **Query Type Classification**
   - SELECT queries → QueryType.SELECT
   - JOIN queries → QueryType.JOIN  
   - Aggregate queries → QueryType.AGGREGATE
   - Complex queries → QueryType.COMPLEX

4. **Combination Strategy Detection**
   - UNION queries → CombineStrategyType.UNION
   - Comparison queries → CombineStrategyType.COMPARE
   - Join-based combinations → CombineStrategyType.JOIN
   - Custom logic → CombineStrategyType.CUSTOM

### Edge Cases
1. **Ambiguous Queries**
   - Vague references ("show me the data")
   - Missing context ("get the latest")
   - Expected: Best-effort interpretation with warnings

2. **Very Long Queries**
   - Multi-paragraph business requirements
   - Expected: Proper decomposition without truncation

3. **Special Characters**
   - Queries with quotes, special chars
   - SQL-like syntax in natural language
   - Expected: Proper escaping and handling

### Error Scenarios
1. **Invalid Input**
   - Empty query string
   - Non-English queries
   - Binary/encoded data
   - Expected: Graceful error with clear message

2. **Memory Failures**
   - Memory service unavailable
   - Corrupted memory state
   - Expected: Proper error propagation

## 2. SchemaLinkingAgent Test Cases

### Core Functionality Tests
1. **Basic Table Mapping**
   - Direct table name matches
   - Synonym recognition (e.g., "staff" → "employees")
   - Plural/singular handling
   - Expected: Correct table identification

2. **Column Mapping**
   - Direct column matches
   - Column purpose inference (e.g., "total" → "amount")
   - Data type considerations
   - Expected: Accurate column selection

3. **Join Path Discovery**
   - Direct foreign keys
   - Multi-hop joins (A→B→C)
   - Self-joins (employees.manager_id)
   - Expected: Optimal join paths

4. **Batch Processing**
   - Multiple nodes in single call
   - Parallel processing efficiency
   - Expected: All nodes mapped correctly

### Complex Scenarios
1. **Ambiguous Mappings**
   - Multiple possible tables (e.g., "users" vs "customers")
   - Same column in multiple tables
   - Expected: Context-aware selection

2. **Schema Evolution**
   - Tables added during session
   - Columns removed/renamed
   - Expected: Dynamic adaptation

3. **Cross-Database Queries**
   - References to multiple databases
   - Schema namespace handling
   - Expected: Proper database prefixing

### Error Scenarios
1. **Missing Schema Elements**
   - Referenced table doesn't exist
   - Column not found
   - Expected: Clear error with suggestions

2. **Circular Dependencies**
   - Recursive join paths
   - Expected: Detection and prevention

## 3. SQLGeneratorAgent Test Cases

### Core SQL Generation
1. **Basic SELECT**
   - Simple column selection
   - WHERE conditions
   - ORDER BY, LIMIT
   - Expected: Valid, executable SQL

2. **JOIN Queries**
   - INNER, LEFT, RIGHT, FULL OUTER
   - Multiple joins
   - Join conditions
   - Expected: Correct join syntax

3. **Aggregate Functions**
   - SUM, AVG, COUNT, MIN, MAX
   - GROUP BY clauses
   - HAVING conditions
   - Expected: Proper aggregation

4. **Advanced SQL Features**
   - Subqueries (correlated/uncorrelated)
   - CTEs (Common Table Expressions)
   - Window functions
   - CASE statements
   - Expected: Database-compatible syntax

### Complex Generation
1. **Child Node Integration**
   - Using results from child queries
   - Combining multiple child results
   - Expected: Proper result integration

2. **Combination Strategies**
   - UNION/UNION ALL generation
   - Complex JOIN combinations
   - Comparison query structures
   - Expected: Correct combination SQL

3. **Performance Optimization**
   - Index usage hints
   - Query optimization
   - Expected: Efficient SQL

### Safety & Security
1. **SQL Injection Prevention**
   - Parameterized queries
   - Input sanitization
   - Expected: Safe SQL generation

2. **Resource Limits**
   - Preventing cartesian products
   - Limiting result sets
   - Expected: Resource-conscious queries

## 4. SQLExecutorAgent Test Cases

### Execution Tests
1. **Successful Execution**
   - Normal query execution
   - Result parsing
   - Performance metrics
   - Expected: Complete ExecutionResult

2. **Large Result Sets**
   - Pagination handling
   - Memory efficiency
   - Streaming results
   - Expected: Controlled resource usage

3. **Empty Results**
   - Valid query, no data
   - Expected: Proper empty result handling

4. **Timeout Handling**
   - Long-running queries
   - Expected: Graceful timeout with partial results

### Result Evaluation
1. **Intent Matching**
   - Results match query intent
   - Completeness check
   - Accuracy validation
   - Expected: Quality assessment

2. **Performance Analysis**
   - Execution time analysis
   - Query plan evaluation
   - Resource usage
   - Expected: Performance insights

3. **Optimization Suggestions**
   - Index recommendations
   - Query rewrite suggestions
   - Expected: Actionable improvements

### Error Handling
1. **SQL Errors**
   - Syntax errors
   - Runtime errors (division by zero)
   - Permission errors
   - Expected: Clear error diagnosis

2. **Connection Issues**
   - Database unavailable
   - Network timeouts
   - Connection pool exhaustion
   - Expected: Retry logic and fallbacks

3. **Data Issues**
   - Type mismatches
   - Encoding problems
   - Expected: Data quality reporting

## Integration Test Scenarios

### End-to-End Workflows
1. **Simple Query Flow**
   ```
   Query → Analyze → Link Schema → Generate SQL → Execute
   ```
   - Test: "Show all employees"
   - Expected: Complete execution with results

2. **Complex Multi-Step Query**
   ```
   Query → Decompose → Multiple parallel paths → Combine results
   ```
   - Test: "Compare top products by region with year-over-year growth"
   - Expected: Proper decomposition and result combination

3. **Error Recovery Flow**
   ```
   Failed execution → Analyze error → Regenerate SQL → Retry
   ```
   - Test: Initial SQL fails, system self-corrects
   - Expected: Successful recovery

### Memory Consistency Tests
1. **State Synchronization**
   - Multiple agents updating same node
   - Concurrent access patterns
   - Expected: Consistent state

2. **Transaction Boundaries**
   - Rollback on failure
   - Atomic updates
   - Expected: No partial states

## Performance Test Cases

### Response Time
1. **Agent Latency**
   - Individual agent response times
   - LLM call optimization
   - Expected: <2s for simple, <5s for complex

2. **Throughput**
   - Concurrent query processing
   - Batch operation efficiency
   - Expected: Linear scaling

### Resource Usage
1. **Memory Footprint**
   - Large result set handling
   - Memory leak detection
   - Expected: Bounded memory growth

2. **Database Connections**
   - Connection pooling
   - Cleanup on failure
   - Expected: No connection leaks

## Test Implementation Strategy

### Phase 1: Unit Tests
- Mock LLM responses for determinism
- Test each agent in isolation
- Focus on core functionality

### Phase 2: Integration Tests
- Test agent interactions
- Use real memory system
- Test error propagation

### Phase 3: End-to-End Tests
- Complete workflow testing
- Performance benchmarks
- Stress testing

### Phase 4: Regression Suite
- Automated test runs
- Coverage reporting
- Performance tracking

## Mock Data Requirements

### LLM Response Mocks
- Predefined XML responses for each test case
- Error response templates
- Performance simulation

### Database Mocks
- Sample schemas (ecommerce, HR, finance)
- Test data generators
- Query result fixtures

### Memory Mocks
- State snapshots
- Failure injection
- Concurrency simulation

## Success Criteria
- 95% code coverage across all agents
- All core functionality tests passing
- Error scenarios handled gracefully
- Performance within defined limits
- No memory leaks or resource issues
- Integration tests demonstrate correct agent cooperation