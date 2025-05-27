# LLM Prompt Updates for Tree Orchestration

## Overview
All LLM prompts and system messages have been updated to reflect the new tree orchestration paradigm instead of workflow-based language.

## Key Terminology Changes

### 1. File Headers and Documentation
- **Before**: "for text-to-SQL workflow"
- **After**: "for text-to-SQL tree orchestration"

### 2. System Messages and Prompts
- **Before**: "workflow steps", "workflow completion"
- **After**: "tree processing steps", "tree completion"

### 3. Agent Descriptions
- **Before**: "workflow by coordinating multiple agents"
- **After**: "tree processing by coordinating multiple agents"

### 4. Class Descriptions
- **Before**: "use in workflows"
- **After**: "use in tree orchestration"

## Specific Updates by Agent

### QueryAnalyzerAgent
- Updated file header to reference "tree orchestration"
- System prompt remains focused on query analysis and tree creation

### SchemaLinkerAgent
- Updated file header and class documentation
- System prompt unchanged (already focused on schema linking)

### SQLGeneratorAgent
- Updated file header
- System prompt unchanged (already focused on SQL generation)

### SQLEvaluatorAgent
- Updated file header and internal documentation
- Updated progression logic comments to reference "tree processing"
- Completion messages now use "TREE COMPLETE"

### OrchestratorAgent
- **System message updated**:
  - "orchestrator for text-to-SQL conversion workflow" → "orchestrator for text-to-SQL tree processing"
  - "Workflow steps:" → "Tree processing steps:"
  - "follow the workflow" → "follow the tree processing steps"
- **Class documentation updated**:
  - "workflow by coordinating" → "tree processing by coordinating"
  - "Initializes the workflow" → "Initializes the tree processing"

### BaseMemoryAgent
- Updated class description from "workflow" to "tree orchestration"
- Updated tool description for "use in tree orchestration"

### Memory Components
- Updated KeyValueMemory documentation
- Updated MemoryAgentTool provider reference

## Coordinator Prompt Analysis
The main coordinator prompt in `text_to_sql_tree_orchestrator.py` was already updated to use:
- "Tree Processing:" instead of "Workflow:"
- "tree processing is ONLY complete" instead of "workflow is ONLY complete"
- "TREE COMPLETE" instead of "WORKFLOW COMPLETE"
- References to tree structure and node navigation

## Impact on LLM Understanding
These changes help the LLM understand that:
1. **Non-linear processing**: Not a sequential workflow but dynamic tree navigation
2. **Node-based operation**: Processing happens at the node level, not workflow level
3. **Completion criteria**: Tree is complete when ALL nodes have good SQL
4. **Navigation pattern**: Moving between nodes based on evaluation results
5. **Orchestration model**: Coordinating agents to process tree nodes

## Verification
All prompts now consistently use tree orchestration terminology, making it clearer to the LLM that this is:
- A tree data structure being processed
- Non-linear navigation between nodes
- Completion based on all nodes having good quality
- Orchestration of multiple specialized agents
- Dynamic routing based on current node status

This should improve the LLM's understanding of the system's true nature as a tree processor rather than a linear workflow executor.