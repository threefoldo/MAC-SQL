"""
Complete Text-to-SQL Tree Orchestrator Implementation

This module provides a complete text-to-SQL tree orchestrator using all 4 specialized agents:
- QueryAnalyzerAgent: Analyzes user queries and creates query trees
- SchemaLinkerAgent: Links query intents to database schema
- SQLGeneratorAgent: Generates SQL from linked schema
- SQLEvaluatorAgent: Executes and evaluates SQL results

The tree processing is orchestrated by a coordinator agent that ensures correct SQL generation for all nodes.
"""

import os
import sys
import asyncio
import logging
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from dotenv import load_dotenv

# Import all required components
from keyvalue_memory import KeyValueMemory
from task_context_manager import TaskContextManager
from query_tree_manager import QueryTreeManager
from database_schema_manager import DatabaseSchemaManager
from node_history_manager import NodeHistoryManager
from schema_reader import SchemaReader

# All 4 agents + task status checker
from query_analyzer_agent import QueryAnalyzerAgent
from schema_linker_agent import SchemaLinkerAgent
from sql_generator_agent import SQLGeneratorAgent
from sql_evaluator_agent import SQLEvaluatorAgent
from task_status_checker import TaskStatusChecker

# Memory types
from memory_content_types import (
    TaskContext, QueryNode, NodeStatus, TaskStatus,
    TableSchema, ColumnInfo, ExecutionResult
)

# AutoGen components
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_ext.models.openai import OpenAIChatCompletionClient


class TextToSQLTreeOrchestrator:
    """
    Complete text-to-SQL tree orchestrator using 4 specialized agents.
    
    This class orchestrates the entire tree processing from query analysis to SQL execution,
    navigating through query tree nodes until all have good quality SQL.
    """
    
    def __init__(self, 
                 data_path: str,
                 tables_json_path: str,
                 dataset_name: str = "bird",
                 llm_config: Optional[Dict[str, Any]] = None,
                 max_steps: int = 100):
        """
        Initialize the text-to-SQL tree orchestrator.
        
        Args:
            data_path: Path to the database files
            tables_json_path: Path to the tables JSON file
            dataset_name: Name of the dataset (bird, spider, etc.)
            llm_config: Configuration for the LLM
            max_steps: Maximum number of coordinator steps before stopping (default: 100)
        """
        self.data_path = data_path
        self.tables_json_path = tables_json_path
        self.dataset_name = dataset_name
        self.max_steps = max_steps
        
        # Default LLM configuration
        self.llm_config = llm_config or {
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        }
        
        # Initialize memory and managers
        self.memory = KeyValueMemory()
        self.task_manager = TaskContextManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.schema_manager = DatabaseSchemaManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
        
        # Initialize schema reader
        self.schema_reader = SchemaReader(
            data_path=self.data_path,
            tables_json_path=self.tables_json_path,
            dataset_name=self.dataset_name,
            lazy=False
        )
        
        # Initialize agents
        self.query_analyzer = QueryAnalyzerAgent(self.memory, self.llm_config)
        self.schema_linker = SchemaLinkerAgent(self.memory, self.llm_config)
        self.sql_generator = SQLGeneratorAgent(self.memory, self.llm_config)
        self.sql_evaluator = SQLEvaluatorAgent(self.memory, self.llm_config)
        
        # Initialize TaskStatusChecker (no LLM config needed)
        self.task_status_checker = TaskStatusChecker(self.memory)
        
        # Initialize coordinator
        self.coordinator = None
        
        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def initialize_database(self, db_name: str) -> None:
        """
        Initialize the database schema in memory.
        
        Args:
            db_name: Name of the database to load
        """
        await self.schema_manager.load_from_schema_reader(self.schema_reader, db_name)
        
        # Get schema summary
        summary = await self.schema_manager.get_schema_summary()
        self.logger.info(f"Loaded database '{db_name}' schema:")
        self.logger.info(f"  Tables: {summary['table_count']}")
        self.logger.info(f"  Total columns: {summary['total_columns']}")
        self.logger.info(f"  Foreign keys: {summary['total_foreign_keys']}")
    
    def _create_coordinator(self) -> AssistantAgent:
        """Create the coordinator agent with improved prompt."""
        coordinator_client = OpenAIChatCompletionClient(
            model="gpt-4o",
            temperature=0.1,
            timeout=300,
            api_key=os.getenv("OPENAI_API_KEY")
        )
        
        coordinator = AssistantAgent(
            name="orchestrator",
            system_message="""You are a FEEDBACK LOOP ORCHESTRATOR for text-to-SQL generation.

## YOUR MISSION: BUILD FEEDBACK LOOPS TO GET GOOD SQL

Your ONLY goal is to construct feedback loops that iterate until you get GOOD SQL results.

**FEEDBACK LOOP PATTERN:**
1. Check status → 2. If bad SQL, analyze error → 3. Choose agent to change context → 4. Retry → 5. Repeat until GOOD

## AVAILABLE TOOLS:
- **task_status_checker**: Gets current tree status and node states
- **schema_linker**: Changes schema context (table/column selection)  
- **query_analyzer**: Changes query decomposition context
- **sql_generator**: Generates SQL from current context
- **sql_evaluator**: Executes SQL and provides error feedback

## FEEDBACK LOOP CONSTRUCTION:

### STEP 1: Always start with task_status_checker
```
Call task_status_checker to understand current situation
```

### STEP 2: Interpret the status report
The status report tells you:
- Current node and its state (no_sql, need_eval, good_sql, bad_sql)
- Error messages if SQL failed
- Whether nodes reached max retries
- Completion status

### STEP 3: Choose next agent based on FEEDBACK LOOP LOGIC

**If status shows "No query tree found":**
- No schema analysis? → Call schema_linker with task=""
- Has schema? → Call query_analyzer with task=""

**If current node "State: no_sql":**
- Call sql_generator with task="node_[NODE_ID]"

**If current node "State: need_eval":**
- Call sql_evaluator with task="node_[NODE_ID]"

**If current node "State: bad_sql":**
- FEEDBACK LOOP CRITICAL POINT: Analyze error to change context
- Schema errors (table/column not found) → Call schema_linker with task="node_[NODE_ID]"
- Logic errors (syntax/joins) → Call query_analyzer with task="node_[NODE_ID]"
- Unknown errors → Try schema_linker first (most common issue)

**If current node "State: good_sql":**
- TaskStatusChecker automatically moves to next node
- Call task_status_checker again to see new current node

**If status shows "All nodes processed successfully":**
- Say TERMINATE - you achieved the goal!

**If status shows "reached max retries":**
- Node failed too many times, TaskStatusChecker moved to next
- Continue the feedback loop with new current node

### STEP 4: After calling any agent, ALWAYS return to task_status_checker
This completes the feedback loop cycle.

## TERMINATION CONDITIONS:

✅ **SUCCESS TERMINATION:** Say TERMINATE when status shows "All nodes processed successfully"
❌ **LIMIT TERMINATION:** Say TERMINATE when:
- You've made 50+ agent calls (step limit reached)
- 5+ minutes have passed (time limit)
- All nodes have reached max retries (no more progress possible)

## CRITICAL FEEDBACK LOOP RULES:

1. **ALWAYS start each cycle with task_status_checker**
2. **NEVER repeat the same failed approach** - if schema_linker fails, try query_analyzer next
3. **Error messages guide context changes:**
   - Table/column errors → Change schema context (schema_linker)
   - Logic/syntax errors → Change decomposition context (query_analyzer)
4. **Each failure provides information** - agents see full context and adapt
5. **Context changes break failure patterns** - different context → different SQL
6. **Trust the process** - keep iterating until good SQL or limits reached

## EXAMPLE FEEDBACK LOOP CYCLES:

**Cycle 1:** task_status_checker → schema_linker → task_status_checker → query_analyzer
**Cycle 2:** task_status_checker → sql_generator → task_status_checker → sql_evaluator  
**Cycle 3:** task_status_checker → schema_linker (error context change) → task_status_checker → sql_generator
**Cycle 4:** task_status_checker → sql_evaluator → task_status_checker → TERMINATE (good result!)

## AGENT TASK PARAMETERS:
- **Empty task ""**: For initial schema analysis or query analysis
- **"node_[ID]"**: For node-specific operations (sql_generator, sql_evaluator, retries)

## REMEMBER: 
- Your job is to BUILD FEEDBACK LOOPS, not to solve SQL directly
- Each error is information to choose the next context-changing agent
- Keep iterating until GOOD SQL or limits reached
- The goal is a working SQL query that answers the user's question""",
            model_client=coordinator_client,
            tools=[self.query_analyzer.get_tool(), self.schema_linker.get_tool(), 
                   self.sql_generator.get_tool(), self.sql_evaluator.get_tool(),
                   self.task_status_checker.get_tool()]
        )
        
        return coordinator
    
    async def process_query(self, 
                           query: str, 
                           db_name: str,
                           task_id: Optional[str] = None,
                           evidence: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a text-to-SQL query with fixed initial sequence then flexible feedback-driven orchestration.
        
        WORKFLOW:
        1. SchemaLinker: Enrich user query with evidence and find relevant schema (ALWAYS FIRST)
        2. QueryAnalyzer: Create query tree based on schema-enriched understanding (ALWAYS SECOND)  
        3. Flexible orchestration: Use feedback loops to guide subsequent agent decisions
        
        Args:
            query: The natural language query
            db_name: Name of the database to query
            task_id: Optional task ID (auto-generated if not provided)
            evidence: Optional evidence/hints for the query
            
        Returns:
            Dictionary containing tree processing results
        """
        # Generate task ID if not provided
        if not task_id:
            import time
            task_id = f"tree_{int(time.time())}"
        
        # Initialize task with evidence
        await self.task_manager.initialize(task_id, query, db_name, evidence)
        await self.initialize_database(db_name)
        
        # Initialize schema_linking for coordination
        await self._initialize_schema_linking(query, db_name, evidence)
        
        # Initialize query tree with root node
        root_id = await self.tree_manager.initialize(query, evidence)
        await self.tree_manager.set_current_node_id(root_id)
        self.logger.info(f"Initialized query tree with root node: {root_id}")
        
        self.logger.info(f"Processing query with fixed initial sequence: {query}")
        self.logger.info(f"Database: {db_name}")
        self.logger.info(f"Task ID: {task_id}")
        if evidence:
            self.logger.info(f"Evidence: {evidence}")
        
        # Use orchestrator agent to build feedback loops
        return await self._process_with_orchestrator_agent()
    
    async def _process_with_orchestrator_agent(self) -> Dict[str, Any]:
        """
        Process query using orchestrator agent that builds feedback loops.
        
        The orchestrator agent makes decisions about which agents to call
        and constructs feedback loops to achieve good SQL results.
        """
        if not self.coordinator:
            self.coordinator = self._create_coordinator()
        
        # Create team with termination condition
        termination_condition = TextMentionTermination("TERMINATE")
        team = RoundRobinGroupChat(
            participants=[self.coordinator],
            termination_condition=termination_condition
        )
        
        # Initial instruction to start feedback loop construction
        start_instruction = """Begin building feedback loops to generate good SQL for the user's query.

Start by calling task_status_checker to understand the current situation, then construct feedback loops to iteratively improve until you get good SQL results.

Remember:
- Always start each cycle with task_status_checker
- Analyze errors to choose which agent changes context
- Keep iterating until good SQL or limits reached
- The task_status_checker will signal completion when ready"""

        # Run the orchestrator with step and time control
        self.logger.info("Starting orchestrator agent feedback loop construction")
        stream = team.run_stream(task=start_instruction)
        
        # Process messages with loop control
        step_count = 0
        max_steps = 50  # Step limit
        start_time = asyncio.get_event_loop().time()
        time_limit = 300  # 5 minutes time limit
        messages = []
        
        async for message in stream:
            messages.append(message)
            current_time = asyncio.get_event_loop().time()
            
            # Process orchestrator messages
            if hasattr(message, 'source') and message.source == 'orchestrator':
                step_count += 1
                
                # Check step limit
                if step_count >= max_steps:
                    self.logger.warning(f"Reached step limit ({max_steps}). Terminating orchestrator.")
                    break
                
                # Check time limit
                if current_time - start_time >= time_limit:
                    self.logger.warning(f"Reached time limit ({time_limit}s). Terminating orchestrator.")
                    break
                
                if hasattr(message, 'content'):
                    content = str(message.content) if message.content else ""
                    self.logger.debug(f"Orchestrator step {step_count}: {content[:100]}...")
                    
                    # Check for termination
                    if "TERMINATE" in content:
                        self.logger.info(f"Orchestrator completed successfully after {step_count} steps")
                        break
        
        # Log completion status
        total_time = asyncio.get_event_loop().time() - start_time
        if step_count >= max_steps:
            self.logger.error(f"Orchestrator stopped due to step limit ({max_steps} steps)")
        elif total_time >= time_limit:
            self.logger.error(f"Orchestrator stopped due to time limit ({time_limit}s)")
        else:
            self.logger.info(f"Orchestrator completed in {step_count} steps, {total_time:.1f}s")
        
        # Get final results
        return await self._get_tree_results()
    
    
    async def _get_tree_results(self) -> Dict[str, Any]:
        """Extract and format the tree processing results."""
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree:
            return {"error": "No query tree found"}
        
        results = {
            "query_tree": tree,
            "nodes": {},
            "final_result": None,  # Just the root node's SQL
            "tree_complete": await self.memory.get("tree_complete", False)
        }
        
        # Get root node ID
        root_id = tree.get("rootId")
        
        # Process each node
        for node_id, node_data in tree["nodes"].items():
            # Extract SQL from generation field
            sql = None
            if node_data.get("generation") and isinstance(node_data["generation"], dict):
                sql = node_data["generation"].get("sql")
            elif node_data.get("sql"):  # Fallback to old field
                sql = node_data.get("sql")
            
            node_result = {
                "node_id": node_id,
                "intent": node_data.get("intent"),
                "status": node_data.get("status"),
                "mapping": node_data.get("schema_linking", node_data.get("mapping")),  # Handle both fields
                "sql": sql,
                "execution_result": node_data.get("executionResult"),
                "analysis": None
            }
            
            # Get analysis if available
            analysis_key = f"node_{node_id}_analysis"
            analysis = await self.memory.get(analysis_key)
            if analysis:
                node_result["analysis"] = analysis
            
            results["nodes"][node_id] = node_result
        
        # Final result is simply the root node's SQL (regardless of quality)
        if root_id and root_id in tree["nodes"]:
            root_node = tree["nodes"][root_id]
            # Extract SQL from generation field
            if root_node.get("generation") and isinstance(root_node["generation"], dict):
                root_sql = root_node["generation"].get("sql")
            else:
                root_sql = root_node.get("sql")  # Fallback to old field
            results["final_result"] = root_sql
        
        return results
    
    async def display_query_tree(self) -> None:
        """Display the current query tree structure."""
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree:
            print("No query tree found")
            return
        
        print("\n" + "="*60)
        print("QUERY TREE STRUCTURE")
        print("="*60)
        
        current_node_id = await self.tree_manager.get_current_node_id()
        
        for node_id, node_data in tree["nodes"].items():
            is_current = "→" if node_id == current_node_id else " "
            print(f"\n{is_current} Node: {node_id}")
            print(f"  Intent: {node_data.get('intent', 'N/A')}")
            print(f"  Status: {node_data.get('status', 'N/A')}")
            
            # Show mapping if available (check both new and old field names)
            mapping = node_data.get('schema_linking', node_data.get('mapping'))
            if mapping:
                # Handle new structure
                if 'selected_tables' in mapping:
                    tables_data = mapping.get('selected_tables', {})
                    if isinstance(tables_data, dict) and 'table' in tables_data:
                        table_info = tables_data['table']
                        if isinstance(table_info, dict):
                            print(f"  Tables: {table_info.get('name', 'unknown')}")
                        elif isinstance(table_info, list):
                            table_names = [t.get('name', 'unknown') for t in table_info if isinstance(t, dict)]
                            print(f"  Tables: {', '.join(table_names)}")
                # Handle old structure
                elif mapping.get('tables'):
                    tables = [t['name'] for t in mapping['tables']]
                    print(f"  Tables: {', '.join(tables)}")
            
            # Show SQL if available (from generation field)
            sql = None
            if node_data.get('generation') and isinstance(node_data['generation'], dict):
                sql = node_data['generation'].get('sql')
            elif node_data.get('sql'):  # Fallback to old field
                sql = node_data.get('sql')
            
            if sql:
                sql_preview = str(sql).strip().replace('\n', ' ')[:100]
                print(f"  SQL: {sql_preview}..." if len(sql_preview) == 100 else f"  SQL: {sql_preview}")
            
            # Show execution result if available
            if 'executionResult' in node_data and node_data['executionResult']:
                result = node_data['executionResult']
                print(f"  Execution: {result.get('rowCount', 0)} rows")
                if result.get('error'):
                    print(f"  Error: {result['error']}")
            
            # Show analysis quality
            analysis_key = f"node_{node_id}_analysis"
            analysis = await self.memory.get(analysis_key)
            if analysis:
                quality = analysis.get("result_quality", "N/A")
                print(f"  Quality: {quality.upper()}")
    
    async def display_final_results(self) -> None:
        """Display the final SQL from the root node."""
        results = await self._get_tree_results()
        
        print("\n" + "="*60)
        print("FINAL RESULT")
        print("="*60)
        
        if results.get("tree_complete"):
            print("✅ Tree Processing Status: COMPLETE")
        else:
            print("⚠️  Tree Processing Status: INCOMPLETE")
        
        # Display the final SQL (root node's SQL)
        final_sql = results.get("final_result")
        if final_sql:
            print(f"\nFinal SQL:\n{final_sql}")
            
            # Get root node for additional info
            tree = results.get("query_tree", {})
            root_id = tree.get("rootId")
            if root_id and root_id in results["nodes"]:
                root_node = results["nodes"][root_id]
                
                # Show execution result if available
                if root_node.get('execution_result'):
                    exec_result = root_node['execution_result']
                    print(f"\nExecution Result:")
                    print(f"  Status: {exec_result.get('status', 'N/A')}")
                    print(f"  Rows returned: {exec_result.get('rowCount', 0)}")
                    
                    if exec_result.get('data') and len(exec_result['data']) > 0:
                        print(f"\nData (first 5 rows):")
                        for i, row in enumerate(exec_result['data'][:5]):
                            print(f"  {row}")
                
                # Show quality assessment if available
                if root_node.get('analysis'):
                    analysis = root_node['analysis']
                    print(f"\nQuality Assessment:")
                    print(f"  Result quality: {analysis.get('result_quality', 'N/A').upper()}")
        else:
            print("\nNo final SQL available. Root node has not generated SQL yet.")
    
    async def _initialize_schema_linking(self, query: str, db_name: str, evidence: str = None) -> None:
        """
        Initialize the schema_linking for schema linking coordination.
        Query decomposition and SQL storage handled by query_tree.
        """
        try:
            # Create the shared context for schema linking
            schema_context = {
                "original_query": query,
                "database_name": db_name,
                "evidence": evidence,
                "initialized_at": datetime.now().isoformat(),
                "schema_analysis": None,  # Updated by SchemaLinker
                "last_update": None
            }
            
            # Store in memory for SchemaLinker to access
            await self.memory.set("schema_linking", schema_context)
            
            self.logger.debug("✓ Schema linking context initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing schema_linking: {str(e)}", exc_info=True)
            raise


# Convenience function for quick tree orchestration execution
async def run_text_to_sql(query: str, 
                         db_name: str,
                         data_path: str = "/home/norman/work/text-to-sql/MAC-SQL/data/bird",
                         dataset_name: str = "bird",
                         display_results: bool = True,
                         evidence: Optional[str] = None) -> Dict[str, Any]:
    """
    Quick function to run a text-to-SQL tree orchestration.
    
    Args:
        query: Natural language query
        db_name: Database name
        data_path: Path to database files
        dataset_name: Dataset name (bird, spider, etc.)
        display_results: Whether to display results
        evidence: Optional evidence/hints for the query
        
    Returns:
        Dictionary containing tree processing results
    """
    # Load environment variables
    load_dotenv()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.getLogger('autogen_core').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    
    # Initialize tree orchestrator
    tables_json_path = str(Path(data_path) / "dev_tables.json")
    orchestrator = TextToSQLTreeOrchestrator(
        data_path=data_path,
        tables_json_path=tables_json_path,
        dataset_name=dataset_name
    )
    
    # Process query
    results = await orchestrator.process_query(
        query=query,
        db_name=db_name,
        evidence=evidence
    )
    
    # Display results if requested
    if display_results:
        await orchestrator.display_query_tree()
        await orchestrator.display_final_results()
    
    # Add compatibility fields for easier access
    if results:
        results["sql"] = results.get("final_result")
        results["success"] = bool(results.get("final_result") and results.get("tree_complete", False))
    
    return results


# Example usage and testing
if __name__ == "__main__":
    async def test_tree_orchestrator():
        """Test the complete tree orchestrator."""
        # Example queries
        test_queries = [
            ("What is the highest eligible free rate for K-12 students in schools located in Alameda County?", "california_schools"),
            ("Find the top 5 schools with the highest average math SAT scores", "california_schools"),
            ("How many schools are there in each county?", "california_schools")
        ]
        
        for i, (query, db_name) in enumerate(test_queries[:1]):  # Test first query only
            print(f"\n{'='*80}")
            print(f"TEST {i+1}: {query}")
            print('='*80)
            
            try:
                results = await run_text_to_sql(
                    query=query,
                    db_name=db_name,
                    display_results=True
                )
                
                print(f"\n✓ Test {i+1} completed")
                
            except Exception as e:
                print(f"✗ Test {i+1} failed: {e}")
                import traceback
                traceback.print_exc()
    
    # Run test
    asyncio.run(test_tree_orchestrator())