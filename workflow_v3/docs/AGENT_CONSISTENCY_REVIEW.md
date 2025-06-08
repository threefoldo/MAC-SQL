# Agent Consistency Review - workflow_v2/src

## Executive Summary

The workflow_v2 system implements a clear architectural pattern where:
- The **Orchestrator** operates as an LLM-powered state machine that coordinates workflow
- **Specialized agents** (QueryAnalyzer, SchemaLinker, SQLGenerator, SQLEvaluator) are simple wrappers with consistent interfaces
- **TaskStatusChecker** provides deterministic state evaluation
- All agents access shared memory through **manager classes** for organization

## Quick Implementation Guide

### Creating a New Agent
1. Inherit from `BaseMemoryAgent`
2. Define the agent prompt in `prompts.py`
3. Implement `_reader_callback` to extract context from memory
4. Implement `_parser_callback` to write results back to memory
5. Use manager classes for all memory operations

### Key Principles
- **No business logic in agents** - Let the LLM decide everything
- **Agents are tools** - Called by the orchestrator based on LLM decisions
- **Memory through managers** - Never access memory directly
- **Simple is better** - Each agent does one thing well

## Architecture Overview

```
TextToSQLTreeOrchestrator (Main Entry Point)
    │
    ├── OrchestratorAgent (LLM-Powered State Machine)
    │   ├── Calls TaskStatusChecker tool to analyze whole QueryTree
    │   ├── Uses LLM to analyze state report and decide which agent to call
    │   └── Calls agents as tools based on LLM decision:
    │       ├── QueryAnalyzerAgent → Analyzes queries
    │       ├── SchemaLinkerAgent → Links schema elements
    │       ├── SQLGeneratorAgent → Generates SQL
    │       └── SQLEvaluatorAgent → Evaluates SQL quality
    │
    └── Shared Memory (Managed Access)
        ├── TaskContextManager → Task state
        ├── DatabaseSchemaManager → Schema info
        ├── QueryTreeManager → Query tree
        └── NodeHistoryManager → Operation history
```

## Agent Pattern Implementation

### Core Agent Architecture

Each specialized agent (except Orchestrator and TaskStatusChecker) follows this pattern:

```python
class SpecificAgent(BaseMemoryAgent):
    """Simple wrapper around AutoGen agent."""
    
    agent_name = "specific_agent"
    
    def __init__(self, llm_config):
        # 1. Prompt - Instructions for the LLM
        self.prompt = AGENT_SPECIFIC_PROMPT
        
        # 2. Initialize with AutoGen agent
        super().__init__(llm_config)
    
    async def _reader_callback(self, memory, task, token):
        # 3. Reader - Extract context from shared memory
        # Uses manager classes, NOT direct memory access
        context = await self.some_manager.get_relevant_data()
        return {"context": context}
    
    async def _parser_callback(self, memory, task, result, token):
        # 4. Parser - Write results back to shared memory
        # Uses manager classes, NOT direct memory access
        parsed_data = self._parse_llm_output(result)
        await self.some_manager.update(parsed_data)
```

### Agent Components

1. **Prompt**: LLM instructions (from `prompts.py`)
2. **Reader Callback**: Extracts context from memory via managers
3. **Parser Callback**: Writes results to memory via managers
4. **AutoGen Integration**: Built on top of AutoGen agents

## State Machine Orchestration

### OrchestratorAgent (LLM-Powered State Machine)

The orchestrator is different - it's an LLM-powered state machine coordinator:

```python
class OrchestratorAgent:
    """
    LLM-powered state machine that coordinates the workflow.
    Does NOT inherit from BaseMemoryAgent because it's not a simple agent.
    It's the conductor that uses LLM to decide which agents to call.
    """
    
    def run(self):
        while not done:
            # 1. Call TaskStatusChecker tool to analyze whole QueryTree
            state_report = call_tool(task_status_checker, "analyze_query_tree")
            
            # 2. LLM analyzes state report and decides which agent to call
            llm_decision = self.llm.analyze_state_and_decide(
                state_report=state_report,
                available_agents=["schema_linker", "query_analyzer", 
                                "sql_generator", "sql_evaluator"],
                task_context=context
            )
            
            # 3. Execute the agent chosen by LLM
            call_tool(llm_decision.chosen_agent)
            
            # 4. Handle state transitions based on results
            update_state_based_on_results()
```

### TaskStatusChecker (Deterministic Analysis Tool)

Tool that provides deterministic state evaluation:

```python
class TaskStatusChecker:
    """
    Tool that analyzes the whole QueryTree and reports task state.
    NOT an LLM agent - pure deterministic logic.
    Called by orchestrator as a tool.
    """
    
    def analyze_query_tree(self):
        # Analyze the complete QueryTree structure
        # Apply node selection rules:
        # - Start from root
        # - Process unprocessed nodes
        # - Move to next after good result
        # - Complete when no unprocessed nodes
        return StateReport(
            current_node=current,
            tree_status=complete_tree_analysis,
            needs_processing=what_needs_doing,
            suggested_focus=next_priority_area
        )
```

## Memory Management Pattern

### Manager Classes (Shared Memory Access)

All memory operations go through specialized managers:

```python
# ❌ BAD - Direct memory access
memory.set("some_key", some_value)

# ✅ GOOD - Manager-mediated access
await self.task_manager.update_status(TaskStatus.PROCESSING)
await self.schema_manager.add_table_schema(table_info)
await self.tree_manager.update_node(node_id, sql_query)
await self.history_manager.record_operation(operation)
```

### Why Managers?

1. **Organization**: Each manager handles specific data types
2. **Consistency**: Ensures data integrity and proper structure
3. **Abstraction**: Hides memory implementation details
4. **Type Safety**: Managers work with typed data structures

## Pattern Compliance Summary

### ✅ Agents Following the Pattern

1. **QueryAnalyzerAgent**
   - ✓ Prompt + Reader + Parser structure
   - ✓ Uses managers for memory access
   - ✓ Single responsibility: Query analysis

2. **SchemaLinkerAgent**
   - ✓ Prompt + Reader + Parser structure
   - ✓ Uses managers for memory access
   - ✓ Single responsibility: Schema linking

3. **SQLGeneratorAgent**
   - ✓ Prompt + Reader + Parser structure
   - ✓ Uses managers for memory access
   - ✓ Single responsibility: SQL generation

4. **SQLEvaluatorAgent**
   - ✓ Prompt + Reader + Parser structure
   - ✓ Uses managers for memory access
   - ✓ Single responsibility: SQL evaluation

### 🎯 Special Purpose Components

5. **OrchestratorAgent**
   - State machine coordinator
   - Does NOT follow agent pattern (intentionally)
   - Manages workflow and agent selection

6. **TaskStatusChecker**
   - Deterministic QueryTree analysis tool
   - NOT an LLM agent
   - Called by orchestrator to analyze whole QueryTree
   - Provides rules-based state reports

## Workflow Example

```
1. User Query → TextToSQLTreeOrchestrator
   └── Initializes memory and managers

2. OrchestratorAgent starts (LLM-powered state machine)
   └── Loop until complete:
   
3. Orchestrator calls TaskStatusChecker tool
   └── TaskStatusChecker analyzes whole QueryTree
   └── Returns: complete tree status, current_node, what_needs_doing
   
4. Orchestrator LLM analyzes state report and selects agent
   └── Example: LLM reads "node needs SQL" → chooses SQLGeneratorAgent
   
5. Agent execution:
   a. Reader callback → Get context via managers
   b. LLM processing → Generate result
   c. Parser callback → Update memory via managers
   
6. State transition based on results
   └── Continue loop or terminate
```

## Key Design Principles

1. **LLM-Powered State Machine Orchestration**
   - Orchestrator uses LLM to analyze state and make decisions
   - Flexible decision logic based on LLM analysis of current state

2. **Simple Agent Design**
   - Each agent = Prompt + Reader + Parser
   - Single responsibility per agent
   - No complex logic in agents

3. **Managed Memory Access**
   - All memory operations through managers
   - No direct memory manipulation
   - Type-safe data structures

4. **Separation of Concerns**
   - Orchestration logic separate from agent logic
   - State evaluation separate from state changes
   - Memory management separate from business logic

## Implementation Checklist

### For New Agents
- [ ] Inherits from `BaseMemoryAgent`
- [ ] Has a descriptive `agent_name` property
- [ ] Prompt defined in `prompts.py`
- [ ] `_reader_callback` only reads via managers
- [ ] `_parser_callback` only writes via managers
- [ ] No business logic - only context prep and output parsing
- [ ] Proper XML parsing with fallback strategies
- [ ] Clear docstring explaining agent's purpose

### For Code Reviews
- [ ] No direct memory access (no `memory.get()` or `memory.set()`)
- [ ] All memory operations through appropriate managers
- [ ] Agent doesn't make decisions - LLM does
- [ ] Proper error handling for LLM response parsing
- [ ] No hardcoded rules or logic

## Common Pitfalls to Avoid

1. **Adding Logic to Agents**
   ```python
   # ❌ BAD - Agent making decisions
   if len(tables) > 5:
       selected_tables = self._filter_relevant_tables(tables)
   
   # ✅ GOOD - Let LLM decide
   context = {"all_tables": tables}
   # LLM will select relevant tables
   ```

2. **Direct Memory Access**
   ```python
   # ❌ BAD - Direct memory manipulation
   memory.set("node_status", "processing")
   
   # ✅ GOOD - Manager-mediated access
   await self.tree_manager.update_node_status(node_id, NodeStatus.PROCESSING)
   ```

3. **Complex Agent Logic**
   ```python
   # ❌ BAD - Business logic in agent
   def _validate_sql(self, sql):
       # Complex validation logic
       pass
   
   # ✅ GOOD - LLM evaluates SQL
   # Just pass SQL to LLM for evaluation
   ```

## Recommendations

### 1. Maintain Current Architecture ✓
The architecture correctly implements:
- LLM-powered state machine orchestration
- Simple agent wrappers
- Managed memory access

### 2. Document Special Components
Clearly document why OrchestratorAgent and TaskStatusChecker don't follow the agent pattern - they serve different architectural purposes.

### 3. Enforce Manager Usage
Ensure all agents use managers for memory access:
- Code reviews should check for direct memory access
- Consider making memory private to managers

### 4. Keep Agents Simple
Agents should remain simple wrappers:
- Complex logic belongs in the orchestrator
- Agents just transform data with LLM help

### 5. Testing Focus
- Test manager operations independently
- Mock LLM responses for agent testing
- Focus integration tests on orchestrator coordination

## Conclusion

The workflow_v2 architecture successfully implements a clean separation of concerns with:
- An LLM-powered state machine orchestrator for intelligent workflow control
- Simple, consistent agents as processing tools
- Managed memory access through specialized managers
- Clear architectural boundaries and responsibilities

This design enables maintainable, testable, and extensible text-to-SQL conversion.