# Test Suite for Text-to-SQL Tree Orchestration

This directory contains comprehensive tests for the text-to-SQL workflow system.

## Test Organization

### Core Component Tests
- `test_layer1_memory_content_types.py` - Tests for data structures (TaskContext, QueryNode, etc.)
- `test_layer2_memory.py` - Tests for KeyValueMemory implementation
- `test_layer3_managers.py` - Tests for manager classes (TaskContextManager, QueryTreeManager, etc.)
- `test_layer4_agents.py` - Tests for agent integrations
- `test_keyvalue_memory.py` - KeyValueMemory functionality tests

### Agent Tests
- `test_query_analyzer_agent.py` - Query analyzer agent tests
- `test_schema_linker_agent.py` - Schema linker agent tests
- `test_sql_generator_agent.py` - SQL generator agent tests
- `test_sql_evaluator_agent.py` - SQL evaluator agent tests
- `test_task_status_checker.py` - Task status checker tests
- `test_all_agents.py` - Comprehensive agent integration tests

### Workflow Tests
- `test_workflow_simple.py` - Basic workflow functionality tests (runnable without pytest)
- `test_workflow_with_bird.py` - Integration tests using BIRD dataset
- `test_workflow_fixed.py` - Tests for edge cases and data quality issues

### Other Tests
- `test_prompts_integration.py` - Prompt integration tests
- `test_prompts_simple.py` - Simple prompt tests
- `test_edge_cases.py` - Edge case testing
- `test_memory_trace.py` - Memory tracing tests
- `test_text_to_sql_workflow.py` - Main workflow tests

## Running Tests

### Quick Test (without pytest)
```bash
python test_workflow_simple.py
```

### Run All Tests with pytest
```bash
pytest -v
```

### Run Specific Test Categories
```bash
# Core components
pytest test_layer*.py -v

# Agent tests
pytest test_*_agent.py -v

# Workflow tests
pytest test_workflow*.py -v
```

### Run Individual Test Files
```bash
pytest test_query_analyzer_agent.py -v
pytest test_workflow_with_bird.py -v
```

## Test Coverage

The test suite covers:
- Memory and data structure operations
- All four main agents (QueryAnalyzer, SchemaLinker, SQLGenerator, SQLEvaluator)
- Tree orchestration workflow
- Integration with BIRD dataset
- Edge cases and error handling
- Prompt engineering and integration

## Documentation

- `TEST_PLAN_AGENTS.md` - Comprehensive test plan for agent tools
- `AGENT_TEST_SUMMARY.md` - Overview of agent test structure
- `FINAL_TEST_REPORT.md` - Production readiness report