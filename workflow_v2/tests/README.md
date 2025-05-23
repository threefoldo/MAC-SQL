# Test Suite for MAC-SQL Workflow v2

This directory contains comprehensive tests for the text-to-SQL workflow system.

## Test Organization

### Core Component Tests
- `test_layer1_memory_types.py` - Tests for data structures (TaskContext, QueryNode, etc.)
- `test_layer2_memory.py` - Tests for KeyValueMemory implementation
- `test_layer3_managers.py` - Tests for manager classes (TaskContextManager, QueryTreeManager, etc.)
- `test_layer4_agents.py` - Tests for agent integrations

### Workflow Scenario Tests
- `test_workflow_simple.py` - Basic workflow functionality tests
- `test_workflow_cases.py` - Common use case scenarios
- `test_workflow_scenarios_v2.py` - Advanced workflow patterns
- `test_multi_database_scenarios.py` - Multi-database workflow tests
- `test_workflow_memory_simulation.py` - Complete workflow simulation with XML

### Integration and Real Data Tests
- `test_integration.py` - End-to-end integration tests
- `test_memory_types_real_data.py` - Tests with real BIRD dataset
- `test_cases.py` - Additional test cases

### Test Runners
- `run_all_tests.py` - Script to run all tests

### Test Data and Outputs
- `test_workflow_simulation.xml` - XML workflow simulation data
- `*.json` files - Test output and verification data
- `*.ipynb` files - Jupyter notebooks for interactive testing

## Running Tests

### Run All Tests
```bash
cd tests
python run_all_tests.py
```

### Run Individual Test Modules
```bash
# Core component tests
python test_layer1_memory_types.py
python test_layer2_memory.py
python test_layer3_managers.py
python test_layer4_agents.py

# Workflow scenario tests
python test_workflow_simple.py
python test_workflow_cases.py
python test_multi_database_scenarios.py

# Integration tests
python test_integration.py
python test_memory_types_real_data.py
```

### Interactive Testing
Open the Jupyter notebooks for interactive testing:
```bash
jupyter notebook memory-test.ipynb
jupyter notebook selector_agent_test.ipynb
# etc.
```

## Test Coverage

The test suite covers:

1. **Memory Types** (Layer 1)
   - TaskContext, QueryNode, ExecutionResult
   - Data serialization/deserialization
   - Field validation and type checking

2. **Memory System** (Layer 2)
   - KeyValueMemory storage and retrieval
   - JSON serialization of complex objects
   - Memory content management

3. **Manager Classes** (Layer 3)
   - TaskContextManager: Task lifecycle management
   - QueryTreeManager: Query decomposition and tree operations
   - DatabaseSchemaManager: Schema storage and retrieval
   - NodeHistoryManager: Operation tracking and history

4. **Agent Integration** (Layer 4)
   - Agent tool interfaces
   - Memory-backed agent operations
   - Cross-agent data sharing

5. **Workflow Scenarios**
   - Simple aggregation queries
   - Complex multi-table joins
   - Query decomposition patterns
   - Iterative query refinement
   - Multi-database operations
   - Cross-database analysis
   - Window functions and CTEs
   - Schema linking and selection

6. **Real-World Use Cases**
   - BIRD dataset integration
   - Production-like scenarios
   - Error handling and recovery
   - Performance considerations

## Test Data

- **BIRD Dataset**: Real text-to-SQL benchmark data from `/home/norman/work/text-to-sql/MAC-SQL/data/bird/`
- **Synthetic Data**: Generated test cases for specific scenarios
- **XML Simulations**: Complete workflow simulations for integration testing

## Adding New Tests

When adding new tests:

1. Follow the naming convention: `test_*.py`
2. Place in appropriate category (layer1-4, workflow, integration)
3. Include docstrings explaining the test purpose
4. Use the existing memory and manager patterns
5. Update this README if adding new test categories

## Dependencies

Tests require the same dependencies as the main system:
- Python 3.12+
- asyncio for async test functions
- json for data serialization
- datetime for timestamps
- pathlib for file operations

All tests are designed to be self-contained and not require external databases or services.