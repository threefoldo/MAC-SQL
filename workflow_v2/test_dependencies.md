# Test Dependencies and Order

## Dependency Graph

```
Layer 1: Core Infrastructure
├── memory_types.py
└── memory.py (KeyValueMemory)
    ↓
Layer 2: Memory Managers (all depend on Layer 1)
├── task_context_manager.py
├── database_schema_manager.py
├── query_tree_manager.py
└── node_history_manager.py
    ↓
Layer 3: External Interfaces
├── schema_reader.py (depends on Layer 1)
└── sql_executor.py (standalone)
    ↓
Layer 4: Base Agent Framework
└── memory_agent_tool.py (depends on Layer 1)
    ↓
Layer 5: Specialized Agents (all depend on Layers 1, 2, 4)
├── query_analyzer_agent.py
├── schema_linking_agent.py (also needs schema_reader)
├── sql_generator_agent.py
└── sql_executor_agent.py (also needs sql_executor)
    ↓
Layer 6: Orchestration
└── orchestrator_agent.py (depends on all agents)
    ↓
Layer 7: Workflow Integration
├── workflow_utils.py
├── workflow_tools.py
└── workflow_runners.py
```

## Test Execution Order

### Phase 1: Foundation (Day 1-2)
1. Test `memory_types.py` - All data structures
2. Test `memory.py` - Storage and retrieval

### Phase 2: Memory Management (Day 3-4)
3. Test `task_context_manager.py` - Task lifecycle
4. Test `database_schema_manager.py` - Schema storage
5. Test `query_tree_manager.py` - Tree operations
6. Test `node_history_manager.py` - History tracking

### Phase 3: External Systems (Day 5)
7. Test `schema_reader.py` - Schema loading
8. Test `sql_executor.py` - SQL execution

### Phase 4: Agent Framework (Day 6)
9. Test `memory_agent_tool.py` - Base agent functionality

### Phase 5: Individual Agents (Day 7-9)
10. Test `query_analyzer_agent.py` - Query analysis
11. Test `schema_linking_agent.py` - Schema selection
12. Test `sql_generator_agent.py` - SQL generation
13. Test `sql_executor_agent.py` - Execution evaluation

### Phase 6: Orchestration (Day 10-11)
14. Test `orchestrator_agent.py` - Workflow coordination

### Phase 7: Integration (Day 12-14)
15. End-to-end workflow tests
16. Multi-query scenarios
17. Error recovery tests
18. Performance benchmarks

## Testing Strategy for Each Component

### Unit Tests (Each Component)
- Test all public methods
- Test error conditions
- Test edge cases
- Mock dependencies

### Integration Tests (Component Pairs)
- Test data flow between components
- Test error propagation
- Test state consistency

### System Tests (Full Workflow)
- Test complete query processing
- Test complex scenarios
- Test failure recovery
- Test performance limits

## Key Test Scenarios by Layer

### Layer 1-2: Data and Storage
- Create, store, retrieve complex nested data
- Update partial data structures
- Handle missing data gracefully

### Layer 3: Database Integration  
- Load schemas from real SQLite databases
- Execute various SQL query types
- Handle database errors

### Layer 4-5: Agent Intelligence
- Analyze simple to complex queries
- Select minimal required schema
- Generate correct SQL
- Evaluate execution results

### Layer 6-7: Orchestration
- Coordinate multi-step workflows
- Handle failures and retry
- Optimize performance
- Produce final results