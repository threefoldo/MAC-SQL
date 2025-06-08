# Text-to-SQL Workflow v2 Documentation

Welcome to the comprehensive documentation for the Text-to-SQL Workflow v2 system. This guide provides everything you need to understand, implement, and test the system.

## 🚀 Quick Start

```python
from src.text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator

# Initialize the orchestrator
orchestrator = TextToSQLTreeOrchestrator(llm_config)

# Convert natural language to SQL
result = await orchestrator.run(
    query="Show me the top 5 customers by total order value",
    database_name="ecommerce_db",
    evidence="Focus on completed orders only"
)

print(f"Generated SQL: {result.sql}")
print(f"Execution Status: {result.status}")
```

## 📚 Documentation Overview

### 1. [Basic Rules](BASIC_RULES.md)
**Start here if you're new to the system**
- Core orchestration principles
- Quick start guide
- Common workflows
- Troubleshooting tips

### 2. [System Architecture](SYSTEM_ARCHITECTURE_ANALYSIS.md)
**Deep dive into the system design**
- Complete file structure and purposes
- Architecture layers and dependencies
- Implementation guidelines
- Adding new components

### 3. [Memory Design](MEMORY_DESIGN.md)
**Understanding the shared memory system**
- Memory structure and layout
- Manager classes and their roles
- Best practices for memory access
- Debugging memory issues

### 4. [Agent Consistency](AGENT_CONSISTENCY_REVIEW.md)
**Guidelines for maintaining consistent agents**
- Agent architecture patterns
- Implementation checklist
- Common pitfalls to avoid
- Code review guidelines

### 5. [Testing Plan](TESTING_PLAN.md)
**Comprehensive testing strategy**
- Layer-by-layer testing approach
- Test automation setup
- Success metrics
- CI/CD integration

## 🏗️ System Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                   User Query                             │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│           TextToSQLTreeOrchestrator                      │
│                 (Entry Point)                            │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│            OrchestratorAgent                             │
│        (LLM-Powered State Machine)                       │
│  ┌─────────────────────────────────────┐                │
│  │ 1. Call TaskStatusChecker            │                │
│  │ 2. Analyze state with LLM           │                │
│  │ 3. Choose appropriate agent         │                │
│  │ 4. Execute and update state         │                │
│  │ 5. Repeat until complete            │                │
│  └─────────────────────────────────────┘                │
└────────────────┬───────────────────┬────────────────────┘
                 │                   │
                 ▼                   ▼
┌────────────────────────┐  ┌─────────────────────────────┐
│   Specialized Agents   │  │    Shared Memory System     │
├────────────────────────┤  ├─────────────────────────────┤
│ • QueryAnalyzer        │  │ • TaskContextManager        │
│ • SchemaLinker         │  │ • DatabaseSchemaManager     │
│ • SQLGenerator         │  │ • QueryTreeManager          │
│ • SQLEvaluator         │  │ • NodeHistoryManager        │
└────────────────────────┘  └─────────────────────────────┘
```

## 🎯 Key Concepts

### 1. LLM-Powered Orchestration
The orchestrator uses an LLM to intelligently decide which agent to call based on the current state of the task. This provides flexibility and adaptability without hardcoded rules.

### 2. Agent as Tools
Each specialized agent is a simple tool that:
- Prepares context from shared memory
- Calls an LLM with a specific prompt
- Parses the LLM output back to shared memory
- Contains NO business logic

### 3. Managed Memory Access
All memory operations go through manager classes:
- Never access memory directly
- Type-safe operations
- Consistent data structures
- Clear separation of concerns

### 4. Query Tree Structure
Complex queries are decomposed into a tree where:
- Each node represents a sub-task
- Nodes can have parent-child relationships
- The orchestrator processes nodes intelligently
- Results combine to form the final SQL

## 🛠️ Implementation Guide

### Creating a New Agent

1. **Inherit from BaseMemoryAgent**
```python
from src.base_memory_agent import BaseMemoryAgent

class MyNewAgent(BaseMemoryAgent):
    agent_name = "my_new_agent"
```

2. **Add prompt to prompts.py**
```python
MY_NEW_AGENT_PROMPT = """
You are a specialized agent that...
"""
```

3. **Implement callbacks**
```python
async def _reader_callback(self, memory, task, token):
    # Read context from managers
    pass

async def _parser_callback(self, memory, task, result, token):
    # Write results to managers
    pass
```

### Adding a Manager

1. **Create manager class**
```python
class MyDataManager:
    def __init__(self, memory: KeyValueMemory):
        self.memory = memory
        self.key = "my_data"
```

2. **Implement CRUD operations**
```python
async def set_data(self, data: MyDataType):
    await self.memory.set(self.key, data.to_dict())

async def get_data(self) -> MyDataType:
    raw = await self.memory.get(self.key)
    return MyDataType.from_dict(raw) if raw else None
```

## 🧪 Testing Guidelines

### Test Pyramid
1. **Unit Tests** (70%)
   - Test managers independently
   - Mock LLM responses for agents
   - Verify data transformations

2. **Integration Tests** (20%)
   - Test agent-manager interactions
   - Verify orchestrator decisions
   - Test error recovery

3. **E2E Tests** (10%)
   - Complete query workflows
   - Real database schemas
   - Performance benchmarks

### Running Tests
```bash
# Run all tests
pytest tests/ -v

# Run specific layer
pytest tests/test_layer1_memory_managers.py

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## 📈 Performance Guidelines

### Expected Performance
- Simple queries: < 5 seconds
- Medium complexity: < 15 seconds
- Complex queries: < 30 seconds

### Optimization Tips
1. Cache database schemas
2. Reuse LLM connections
3. Batch memory operations
4. Use appropriate LLM models

## 🔍 Debugging Tips

### Enable Verbose Logging
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Inspect Memory State
```python
# View current task
task = await task_manager.get_context()
print(f"Task: {task}")

# Check query tree
tree = await tree_manager.get_tree()
print(f"Tree: {tree}")

# Review node history
history = await history_manager.get_node_history(node_id)
print(f"History: {history}")
```

### Common Issues
1. **Agent not updating state**: Check parser callback
2. **Wrong agent selected**: Review orchestrator prompt
3. **Memory not persisting**: Ensure await on async calls
4. **SQL generation fails**: Verify schema linking completed

## 🤝 Contributing

### Code Style
- Use type hints
- Follow PEP 8
- Add docstrings
- Write tests

### Pull Request Process
1. Create feature branch
2. Implement with tests
3. Update documentation
4. Submit PR with description

## 📞 Support

### Getting Help
- Check troubleshooting in [Basic Rules](BASIC_RULES.md)
- Review test examples in [Testing Plan](TESTING_PLAN.md)
- Examine architecture in [System Architecture](SYSTEM_ARCHITECTURE_ANALYSIS.md)

### Reporting Issues
When reporting issues, include:
- Query that failed
- Database schema
- Error messages
- Memory state if possible

## 🚦 Next Steps

1. **New Users**: Start with [Basic Rules](BASIC_RULES.md)
2. **Developers**: Review [System Architecture](SYSTEM_ARCHITECTURE_ANALYSIS.md)
3. **Contributors**: Check [Agent Consistency](AGENT_CONSISTENCY_REVIEW.md)
4. **Testers**: Follow [Testing Plan](TESTING_PLAN.md)

---

Remember: **Keep agents simple, let the LLM do the thinking!**