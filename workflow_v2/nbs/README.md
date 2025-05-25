# Text-to-SQL Workflow Notebooks

This directory contains Jupyter notebooks demonstrating various aspects of the text-to-SQL workflow system.

## Notebooks Overview

### 1. `01-memory-content-test.ipynb`
- Tests the memory content types and data structures
- Demonstrates how different memory types work together
- Shows basic memory operations

### 2. `01-memory_agent_test.ipynb`
- Original demonstration of memory-based agent workflow
- Uses 3 agents: schema_selector, sql_generator, sql_evaluator
- Shows basic agent coordination with shared memory

### 3. `02-agent-*.ipynb` (Individual Agent Tests)
- `02-agent-query-analyzer.ipynb`: Tests QueryAnalyzerAgent
- `02-agent-schema-linker.ipynb`: Tests SchemaLinkerAgent
- `02-agent-sql-generator.ipynb`: Tests SQLGeneratorAgent
- `02-agent-sql-evaluator.ipynb`: Tests SQLEvaluatorAgent
- Each notebook tests one agent in isolation

### 4. `03-complete-text-to-sql-workflow.ipynb` ⭐ **NEW**
- **Complete workflow using all 4 agents**
- Demonstrates full text-to-SQL pipeline
- Shows agent coordination and error handling
- Includes query tree visualization
- Tests both simple and complex queries

## Running the Notebooks

### Prerequisites
```bash
# Navigate to the workflow directory
cd /home/norman/work/text-to-sql/MAC-SQL/workflow_v2

# Load environment variables (including OpenAI API key)
source ../.env && export OPENAI_API_KEY

# Start Jupyter
jupyter notebook
```

### Recommended Order
1. Start with `01-memory_agent_test.ipynb` to understand the basic pattern
2. Review individual agent notebooks (`02-agent-*.ipynb`) to understand each component
3. Run `03-complete-text-to-sql-workflow.ipynb` to see the complete system in action

## Key Concepts

### Memory Architecture
- **KeyValueMemory**: Shared memory storage for all agents
- **Managers**: Specialized classes for different data types
  - TaskContextManager: Manages task metadata
  - QueryTreeManager: Manages query tree nodes
  - DatabaseSchemaManager: Manages database schema
  - NodeHistoryManager: Tracks operation history

### Agent Roles
1. **QueryAnalyzerAgent**: Understands user intent, creates query structure
2. **SchemaLinkerAgent**: Maps intent to database schema
3. **SQLGeneratorAgent**: Generates SQL from mappings
4. **SQLEvaluatorAgent**: Executes and evaluates SQL

### Workflow Pattern
```
User Query → QueryAnalyzer → SchemaLinker → SQLGenerator → SQLEvaluator → Results
                    ↓              ↓              ↓              ↓
                Query Tree    Schema Links    SQL Query    Evaluation
```

## Tips for Development

1. **Debugging**: Set logging level to DEBUG for detailed output
2. **Memory Inspection**: Use `memory.show_all()` to see current state
3. **Query Tree**: Use `display_query_tree()` helper to visualize structure
4. **API Keys**: Ensure OPENAI_API_KEY is properly set before running

## Common Issues

1. **Missing API Key**: Run `source ../.env && export OPENAI_API_KEY`
2. **Import Errors**: Ensure you're in the correct directory with `sys.path.append('../src')`
3. **Database Path**: Verify BIRD dataset is available at the expected path