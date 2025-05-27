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
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
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
    QueryMapping, TableMapping, ColumnMapping, JoinMapping,
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
            timeout=120,
            api_key=os.getenv("OPENAI_API_KEY")
        )
        
        coordinator = AssistantAgent(
            name="coordinator",
            system_message="""You are a smart orchestrator for a text-to-SQL workflow.

Your job is to:
1. Examine node status and decide which tool to call
2. Call tools based on what each node needs
3. Say TERMINATE when all nodes are complete

Available tools:
- query_analyzer: Breaks down the user's query into a tree structure
- schema_linker: Links the current node to database schema (ALWAYS call this before generating SQL)
- sql_generator: Generates SQL for the current node
- sql_evaluator: Executes and evaluates SQL for the current node
- task_status_checker: Tells you the current node status and what needs to be done

IMPORTANT: 
- Tools operate on the "current node" automatically - the system tracks which node to work on.
- All tools require a 'task' parameter. Always call tools with task="current task description"

DECISION LOGIC based on node status:
1. No tree exists → call query_analyzer with the user's query as task
2. Node has no SQL → ALWAYS call schema_linker first with task="link schema for current node", then sql_generator
3. Node has bad SQL (evaluation showed poor quality) → call schema_linker with task="relink schema with fixes", then sql_generator with task="regenerate SQL with fixes"
4. Node has SQL but not evaluated → call sql_evaluator with task="evaluate SQL for current node"
5. After any evaluation → call task_status_checker with task="check task status"

CRITICAL: Even if query_analyzer creates some mapping, you MUST still call schema_linker before sql_generator. The mapping from query_analyzer is not sufficient for SQL generation.

The task_status_checker will tell you:
- Current node status (what it has/needs)
- Quality of results (if evaluated)
- ACTION directive:
  - "ACTION: PROCESS NODE" → Node needs work (always schema_linker then sql_generator)
  - "ACTION: RETRY NODE" → Node has poor results (schema_linker then sql_generator again)
  - "ACTION: TASK COMPLETE" → All nodes done, say TERMINATE
  - "ACTION: ERROR" → Something went wrong

WORKFLOW:
1. Start: call query_analyzer with the query
2. Call task_status_checker to understand current state
3. Based on the status report:
   - If node needs SQL → ALWAYS: schema_linker first, then sql_generator
   - If SQL needs retry → ALWAYS: schema_linker first, then sql_generator
   - If SQL not evaluated → sql_evaluator
   - After evaluation → task_status_checker
4. Repeat until task_status_checker says "TASK COMPLETE"

Remember: 
- ALWAYS call schema_linker before sql_generator (even on retries)
- Nodes can be retried if results are poor
- SQL can be regenerated if evaluation shows issues
- Always check status after evaluation to determine next steps""",
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
        Process a text-to-SQL query through the complete tree structure.
        
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
        
        self.logger.info(f"Processing query: {query}")
        self.logger.info(f"Database: {db_name}")
        self.logger.info(f"Task ID: {task_id}")
        if evidence:
            self.logger.info(f"Evidence: {evidence}")
        
        return await self._process_with_coordinator(query)
    
    async def _process_with_coordinator(self, query: str) -> Dict[str, Any]:
        """Process query using the coordinator agent with max steps control."""
        if not self.coordinator:
            self.coordinator = self._create_coordinator()
        
        # Create team with termination condition
        termination_condition = TextMentionTermination("TERMINATE")
        team = RoundRobinGroupChat(
            participants=[self.coordinator],
            termination_condition=termination_condition
        )
        
        # Run the tree processing with step control
        self.logger.info("Starting coordinator-based tree processing")
        stream = team.run_stream(task=query)
        
        # Process messages with loop control
        step_count = 0
        max_steps = self.max_steps  # Use configurable safety limit
        messages = []
        last_message_content = None  # Track duplicate messages
        duplicate_count = 0
        
        async for message in stream:
            messages.append(message)
            
            # Check for duplicate messages
            current_content = str(message.content) if hasattr(message, 'content') else None
            if current_content and current_content == last_message_content:
                duplicate_count += 1
                if duplicate_count >= 3:  # Stop after 3 identical messages
                    self.logger.warning("Detected repeated messages. Stopping to prevent infinite loop.")
                    break
            else:
                duplicate_count = 0
                last_message_content = current_content
            
            # Process coordinator messages
            if hasattr(message, 'source') and message.source == 'coordinator':
                step_count += 1
                
                # Check max steps
                if step_count >= max_steps:
                    self.logger.warning(f"Reached maximum steps ({max_steps}). Stopping to prevent infinite loop.")
                    break
                
                if hasattr(message, 'content'):
                    if isinstance(message.content, str):
                        self.logger.debug(f"Coordinator step {step_count}: {message.content[:100]}...")
                        
                        # Check for termination
                        if "TERMINATE" in message.content:
                            self.logger.info(f"Workflow completed successfully after {step_count} steps")
                            break
        
        # Log completion status
        if step_count >= max_steps:
            self.logger.error(f"Workflow stopped after reaching max steps limit ({max_steps})")
        elif duplicate_count >= 3:
            self.logger.error("Workflow stopped due to repeated messages (possible infinite loop)")
        
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
            node_result = {
                "node_id": node_id,
                "intent": node_data.get("intent"),
                "status": node_data.get("status"),
                "mapping": node_data.get("mapping"),
                "sql": node_data.get("sql"),
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
            root_sql = tree["nodes"][root_id].get("sql")
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
            
            # Show mapping if available
            if 'mapping' in node_data and node_data['mapping']:
                mapping = node_data['mapping']
                if mapping.get('tables'):
                    tables = [t['name'] for t in mapping['tables']]
                    print(f"  Tables: {', '.join(tables)}")
                if mapping.get('columns'):
                    cols = [f"{c['table']}.{c['column']}" for c in mapping['columns']]
                    print(f"  Columns: {', '.join(cols[:3])}..." if len(cols) > 3 else f"  Columns: {', '.join(cols)}")
            
            # Show SQL if available
            if 'sql' in node_data and node_data['sql']:
                sql_preview = node_data['sql'].strip().replace('\n', ' ')[:100]
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