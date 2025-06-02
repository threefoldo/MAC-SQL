"""
Complete Text-to-SQL Tree Orchestrator Implementation

This module provides a complete text-to-SQL tree orchestrator using all 4 specialized agents:
- QueryAnalyzerAgent: Analyzes user queries and creates query trees
- SchemaLinkerAgent: Links query intents to database schema
- SQLGeneratorAgent: Generates SQL from linked schema
- SQLEvaluatorAgent: Executes and evaluates SQL results

The text-to-sql task is represented in a tree, the root node represent the initial user query. 
If it's simple, then there is only one node. If it's complex, it will be decomposed first, then 
process the simpler child nodes. The generated SQL of the parent node is the combination of the 
SQL of children nodes. TaskStatusChecker will report the overall status by navigating the complete 
tree. Orchestrator will based on that status report to decide how to process each node. 

There are 4 tools in total: 
- schema_linker (select which tables and columns will be used in the SQL)
- query_analyzer (understand the intent of query, give hints for SQL generation)
- sql_generator (generate SQL based on all available information) 
- sql_evaluator (execute the SQL and analyze the result, give suggestions if the SQL needs improvement)

There is no order of these tools, just call them when necessary, until TaskStatusChecker report 
all nodes are successfully processed (with good SQLs).
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

# Memory types - updated imports based on actual content
from memory_content_types import (
    TaskContext, QueryNode, NodeStatus, TaskStatus,
    TableSchema, ColumnInfo, ExecutionResult, NodeOperation
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
            "model_name": "gpt-4.1",
            "temperature": 0.1,
            "timeout": 300
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
        """Create the coordinator agent with intelligent context playbook."""
        coordinator_client = OpenAIChatCompletionClient(
            model="gpt-4o",
            temperature=0.1,
            timeout=300,
            api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Build dynamic system message with intelligent context playbook
        base_system_message = self._build_intelligent_system_message()
        
        coordinator = AssistantAgent(
            name="orchestrator",
            system_message=base_system_message,
            model_client=coordinator_client,
            tools=[
                self.schema_linker.get_tool(),
                self.query_analyzer.get_tool(), 
                self.sql_generator.get_tool(), 
                self.sql_evaluator.get_tool(),
                self.task_status_checker.get_tool()
            ]
        )
        
        return coordinator
    
    def _build_intelligent_system_message(self) -> str:
        """Build the system message for the orchestrator agent."""
        from prompts.prompt_loader import PromptLoader
        loader = PromptLoader()
        return loader.get_prompt("orchestrator", version="v1.0")
    
    async def process_query(self, 
                           query: str, 
                           db_name: str,
                           task_id: Optional[str] = None,
                           evidence: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a text-to-SQL query using orchestrator agent for flexible workflow decisions.
        
        WORKFLOW:
        The orchestrator agent will decide the workflow based on task status:
        1. Schema analysis (if needed)
        2. Query analysis and decomposition (if needed) 
        3. SQL generation for each node
        4. SQL evaluation and iteration until good results
        
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
        
        # Schema linking is now handled through task context and nodes
        
        # Initialize query tree with root node (user query and evidence)
        root_id = await self.tree_manager.initialize(query, evidence)
        await self.tree_manager.set_current_node_id(root_id)
        self.logger.info(f"Initialized query tree with root node: {root_id}")
        
        self.logger.info(f"Processing query with orchestrator-driven workflow: {query}")
        self.logger.info(f"Database: {db_name}")
        self.logger.info(f"Task ID: {task_id}")
        if evidence:
            self.logger.info(f"Evidence: {evidence}")
        
        # Use orchestrator agent to build feedback loops
        return await self._process_with_orchestrator_agent()
    
    async def _process_with_orchestrator_agent(self) -> Dict[str, Any]:
        """
        Process query using orchestrator agent.
        
        Simple orchestration: just run the agent, log tool calls, count steps, and check for TERMINATE.
        No business logic - let the orchestrator and tools handle all the workflow decisions.
        """
        if not self.coordinator:
            self.coordinator = self._create_coordinator()
        
        # Create team with termination condition
        termination_condition = TextMentionTermination("TERMINATE")
        team = RoundRobinGroupChat(
            participants=[self.coordinator],
            termination_condition=termination_condition
        )
        
        # Run the orchestrator
        start_instruction = "Process the text-to-SQL workflow using shared memory"
        self.logger.info("Starting orchestrator workflow...")
        
        # Simple message processing - just log tool calls and count steps
        step_count = 0
        max_steps = self.max_steps
        start_time = asyncio.get_event_loop().time()
        time_limit = 600  # 10 minutes
        workflow_complete = False
        terminate_count = 0  # Track how many times TERMINATE has been said
        
        try:
            stream = team.run_stream(task=start_instruction)
            
            async for message in stream:
                current_time = asyncio.get_event_loop().time()
                
                # Process orchestrator messages
                if hasattr(message, 'source') and message.source == 'orchestrator':
                    step_count += 1
                    
                    # Safety limits
                    if step_count >= max_steps:
                        self.logger.warning(f"Reached maximum steps ({max_steps}). Stopping.")
                        break
                    
                    if current_time - start_time >= time_limit:
                        self.logger.warning(f"Reached time limit ({time_limit}s). Stopping.")
                        break
                    
                    if hasattr(message, 'content'):
                        if isinstance(message.content, list) and len(message.content) > 0:
                            # Tool calls - just log the tool name and arguments
                            for tool_call in message.content:
                                if hasattr(tool_call, 'name'):
                                    tool_name = tool_call.name
                                    tool_args = getattr(tool_call, 'arguments', {})
                                    self.logger.info(f"[Step {step_count}] Calling: {tool_name} with args: {tool_args}")
                        
                        elif isinstance(message.content, str):
                            # Check for termination
                            if "TERMINATE" in str(message.content):
                                terminate_count += 1
                                workflow_complete = True
                                self.logger.info(f"[Step {step_count}] ✅ WORKFLOW COMPLETE (TERMINATE #{terminate_count})")
                                # Force immediate termination - no more processing
                                break
                                
        except (asyncio.CancelledError, Exception) as e:
            if isinstance(e, asyncio.CancelledError):
                self.logger.debug("Stream cancelled during processing (this is normal during shutdown)")
            else:
                self.logger.error(f"Error during orchestrator processing: {str(e)}")
            # Don't re-raise - continue to results extraction
        
        # Log completion
        total_time = asyncio.get_event_loop().time() - start_time
        if workflow_complete:
            self.logger.info("WORKFLOW COMPLETED SUCCESSFULLY")
        else:
            self.logger.warning("WORKFLOW STOPPED (may not have completed)")
        
        self.logger.info(f"Total execution time: {total_time:.1f}s, Steps: {step_count}")
        
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
        
        # Final result priority: 1) Root node SQL, 2) Best quality SQL from any node, 3) Any SQL found
        final_sql = None
        
        # First try root node
        if root_id and root_id in tree["nodes"]:
            root_node = tree["nodes"][root_id]
            if root_node.get("generation") and isinstance(root_node["generation"], dict):
                final_sql = root_node["generation"].get("sql")
            elif root_node.get("sql"):
                final_sql = root_node.get("sql")
        
        # If root has no SQL, find the best SQL from any child node
        if not final_sql or final_sql.startswith("SELECT 'Failed"):
            best_score = -1
            for node_id, node_data in tree["nodes"].items():
                # Extract SQL from this node
                node_sql = None
                if node_data.get("generation") and isinstance(node_data["generation"], dict):
                    node_sql = node_data["generation"].get("sql")
                elif node_data.get("sql"):
                    node_sql = node_data.get("sql")
                
                # Evaluate quality if we have SQL
                if node_sql and node_sql.strip() and not node_sql.startswith("SELECT 'Failed"):
                    # Simple scoring: prefer completed nodes with execution results
                    score = 0
                    if node_data.get("status") == "sql_generated":
                        score += 10
                    if node_data.get("evaluation"):
                        score += 20
                        eval_data = node_data["evaluation"]
                        if eval_data.get("result_quality") == "excellent":
                            score += 50
                        elif eval_data.get("result_quality") == "good":
                            score += 30
                        if eval_data.get("answers_intent") == "yes":
                            score += 20
                    
                    # Prefer complex queries over simple ones
                    sql_lower = node_sql.lower()
                    if "join" in sql_lower:
                        score += 15
                    if "count" in sql_lower:
                        score += 10
                    if "where" in sql_lower:
                        score += 5
                    
                    if score > best_score:
                        best_score = score
                        final_sql = node_sql
        
        results["final_result"] = final_sql
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
            
            # Show schema linking info if available
            schema_linking = node_data.get('schema_linking', {})
            if schema_linking:
                # Extract table names from schema linking structure
                table_names = []
                if 'selected_tables' in schema_linking:
                    selected_tables = schema_linking['selected_tables']
                    if isinstance(selected_tables, dict) and 'table' in selected_tables:
                        tables = selected_tables['table']
                        if isinstance(tables, list):
                            table_names = [t.get('name', 'unknown') for t in tables if isinstance(t, dict)]
                        elif isinstance(tables, dict):
                            table_names = [tables.get('name', 'unknown')]
                if table_names:
                    print(f"  Tables: {', '.join(table_names)}")
            
            # Show SQL from generation field
            generation = node_data.get('generation', {})
            if generation and isinstance(generation, dict):
                sql = generation.get('sql')
                if sql:
                    sql_preview = str(sql).strip().replace('\n', ' ')[:100]
                    print(f"  SQL: {sql_preview}..." if len(sql_preview) == 100 else f"  SQL: {sql_preview}")
            
            # Show execution result from evaluation field
            evaluation = node_data.get('evaluation', {})
            if evaluation and isinstance(evaluation, dict):
                exec_result = evaluation.get('execution_result', {})
                if exec_result:
                    row_count = exec_result.get('row_count', 0)
                    print(f"  Execution: {row_count} rows")
                    if exec_result.get('error'):
                        print(f"  Error: {exec_result['error']}")
                
                # Show evaluation quality
                quality = evaluation.get('result_quality', 'N/A')
                if quality != 'N/A':
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
            if root_id and root_id in tree.get("nodes", {}):
                root_node_data = tree["nodes"][root_id]
                
                # Show execution result from evaluation field
                evaluation = root_node_data.get('evaluation', {})
                if evaluation and isinstance(evaluation, dict):
                    exec_result = evaluation.get('execution_result', {})
                    if exec_result:
                        print(f"\nExecution Result:")
                        print(f"  Status: {exec_result.get('status', 'N/A')}")
                        print(f"  Rows returned: {exec_result.get('row_count', 0)}")
                        
                        if exec_result.get('data') and len(exec_result['data']) > 0:
                            print(f"\nData (first 5 rows):")
                            for i, row in enumerate(exec_result['data'][:5]):
                                print(f"  {row}")
                    
                    # Show quality assessment
                    quality = evaluation.get('result_quality', 'N/A')
                    if quality != 'N/A':
                        print(f"\nQuality Assessment:")
                        print(f"  Result quality: {quality.upper()}")
                        
                        # Show summary if available
                        summary = evaluation.get('result_summary')
                        if summary:
                            print(f"  Summary: {summary[:200]}..." if len(summary) > 200 else f"  Summary: {summary}")
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
    
    # Set up clean logging - minimal noise, focus on workflow
    logging.basicConfig(
        level=logging.WARNING,  # Higher level to reduce noise
        format='%(name)s - %(levelname)s - %(message)s'  # Simpler format
    )
    
    # Only show agent-specific logs at INFO level
    agent_loggers = [
        'QueryAnalyzerAgent', 'SchemaLinkerAgent', 
        'SQLGeneratorAgent', 'SQLEvaluatorAgent', 'TaskStatusChecker',
        'TextToSQLTreeOrchestrator'
    ]
    
    for logger_name in agent_loggers:
        logging.getLogger(logger_name).setLevel(logging.INFO)
    
    # Silence very noisy libraries completely
    noisy_loggers = [
        'autogen_core', 'autogen_agentchat', 'httpx', 'openai', 
        'httpcore', 'httpcore.connection', 'httpcore.http11'
    ]
    
    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.ERROR)
    
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