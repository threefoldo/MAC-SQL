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

# All 4 agents
from query_analyzer_agent import QueryAnalyzerAgent
from schema_linker_agent import SchemaLinkerAgent
from sql_generator_agent import SQLGeneratorAgent
from sql_evaluator_agent import SQLEvaluatorAgent

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
                 llm_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the text-to-SQL tree orchestrator.
        
        Args:
            data_path: Path to the database files
            tables_json_path: Path to the tables JSON file
            dataset_name: Name of the dataset (bird, spider, etc.)
            llm_config: Configuration for the LLM
        """
        self.data_path = data_path
        self.tables_json_path = tables_json_path
        self.dataset_name = dataset_name
        
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
            system_message="""You coordinate a text-to-SQL tree processing system using 4 specialized agents.

Your agents:
1. query_analyzer - Analyzes user queries and creates query trees
2. schema_linker - Links query intent to database schema
3. sql_generator - Generates SQL from linked schema
4. sql_evaluator - Executes and evaluates SQL results

Tree Processing:
1. Call query_analyzer with the user's query
   - This creates a query tree and stores the node ID in memory

2. Call schema_linker with: "Link query to database schema"
   - The agent will automatically use the node ID from memory

3. Call sql_generator with: "Generate SQL query"
   - The agent will automatically use the node ID from memory

4. Call sql_evaluator with: "Analyze SQL execution results"
   - The agent will automatically use the node ID from memory
   - CRITICAL: After calling, CHECK THE LOGS for quality assessment

5. CRITICAL - Understanding sql_evaluator results:
   - Agent tools return completion status, NOT evaluation quality
   - You MUST check the logs after sql_evaluator for these indicators:
     * "Result quality: EXCELLENT" or "Result quality: GOOD" → proceed
     * "Result quality: ACCEPTABLE" → must retry with improvements  
     * "Result quality: POOR" → significant issues, must fix
     * "NODE NEEDS IMPROVEMENT" → do not proceed, fix the issues
   - DO NOT assume success just because the tool call completed

6. Node progression and tree completion:
   - The sql_evaluator will automatically progress to the next node ONLY when quality is good
   - If quality is not good, the current node remains active for retry
   - For complex queries with multiple nodes:
     * Each child node must be processed completely (good quality SQL)
     * Only after all children are complete will it move to parent
     * Parent node combines results from children
   - The tree processing is ONLY complete when ALL nodes have good quality

7. TERMINATION RULES - CRITICAL:
   - DO NOT terminate just because agents completed without errors
   - DO NOT provide final answers if any node has poor/acceptable quality
   - Only say "TERMINATE" when:
     * You see "✅ TREE COMPLETE" in the logs
     * ALL nodes show "Result quality: GOOD" or "EXCELLENT" 
     * No "NODE NEEDS IMPROVEMENT" messages in recent logs
   - Before terminating, verify ALL nodes have good SQL results

8. For quality issues:
   - "acceptable" quality means retry is needed - DO NOT terminate
   - "poor" quality means significant issues - analyze and fix
   - Only "good" or "excellent" quality allows progression
   - Common fixes:
     * Wrong tables? → retry schema_linker with guidance
     * Bad SQL? → retry sql_generator with error details
     * Missing data? → check if schema linking was complete

IMPORTANT: 
- Always check evaluation logs before making decisions
- This is a tree structure - ensure every branch is complete
- The agents automatically track the current node ID in memory
- When you have correct SQL with good results for ALL nodes, provide a final answer and say "TERMINATE" """,
            model_client=coordinator_client,
            tools=[self.query_analyzer.get_tool(), self.schema_linker.get_tool(), 
                   self.sql_generator.get_tool(), self.sql_evaluator.get_tool()]
        )
        
        return coordinator
    
    async def process_query(self, 
                           query: str, 
                           db_name: str,
                           task_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a text-to-SQL query through the complete tree structure.
        
        Args:
            query: The natural language query
            db_name: Name of the database to query
            task_id: Optional task ID (auto-generated if not provided)
            
        Returns:
            Dictionary containing tree processing results
        """
        # Generate task ID if not provided
        if not task_id:
            import time
            task_id = f"tree_{int(time.time())}"
        
        # Initialize task
        await self.task_manager.initialize(task_id, query, db_name)
        await self.initialize_database(db_name)
        
        self.logger.info(f"Processing query: {query}")
        self.logger.info(f"Database: {db_name}")
        self.logger.info(f"Task ID: {task_id}")
        
        return await self._process_with_coordinator(query)
    
    async def _process_with_coordinator(self, query: str) -> Dict[str, Any]:
        """Process query using the coordinator agent."""
        if not self.coordinator:
            self.coordinator = self._create_coordinator()
        
        # Create team with termination condition
        termination_condition = TextMentionTermination("TERMINATE")
        team = RoundRobinGroupChat(
            participants=[self.coordinator],
            termination_condition=termination_condition
        )
        
        # Run the tree processing
        self.logger.info("Starting coordinator-based tree processing")
        stream = team.run_stream(task=query)
        
        messages = []
        async for message in stream:
            messages.append(message)
            if hasattr(message, 'source') and message.source == 'coordinator':
                if hasattr(message, 'content') and isinstance(message.content, str):
                    self.logger.debug(f"Coordinator: {message.content[:100]}...")
        
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
            "final_results": [],
            "tree_complete": await self.memory.get("tree_complete", False)
        }
        
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
            
            # Add to final results if it has SQL and good quality
            if node_data.get("sql") and analysis:
                quality = analysis.get("result_quality", "").lower()
                if quality in ["excellent", "good"]:
                    results["final_results"].append(node_result)
        
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
        """Display the final SQL and execution results."""
        results = await self._get_tree_results()
        
        print("\n" + "="*60)
        print("FINAL RESULTS")
        print("="*60)
        
        if results.get("tree_complete"):
            print("✅ Tree Processing Status: COMPLETE")
        else:
            print("⚠️  Tree Processing Status: INCOMPLETE")
        
        # Display results for nodes with good quality
        if results.get("final_results"):
            for node_result in results["final_results"]:
                print(f"\nNode: {node_result['node_id']}")
                print(f"Intent: {node_result.get('intent', 'N/A')}")
                print(f"\nSQL:\n{node_result.get('sql', 'N/A')}")
                
                if node_result.get('execution_result'):
                    exec_result = node_result['execution_result']
                    print(f"\nExecution Result:")
                    print(f"  Rows returned: {exec_result.get('rowCount', 0)}")
                    
                    if exec_result.get('data') and len(exec_result['data']) > 0:
                        print(f"\nData (first 5 rows):")
                        for i, row in enumerate(exec_result['data'][:5]):
                            print(f"  {row}")
                
                if node_result.get('analysis'):
                    analysis = node_result['analysis']
                    print(f"\nEvaluation:")
                    print(f"  Answers intent: {analysis.get('answers_intent', 'N/A').upper()}")
                    print(f"  Result quality: {analysis.get('result_quality', 'N/A').upper()}")
                    print(f"  Summary: {analysis.get('result_summary', 'N/A')}")
        else:
            print("\nNo nodes with good quality results found.")


# Convenience function for quick tree orchestration execution
async def run_text_to_sql(query: str, 
                         db_name: str,
                         data_path: str = "/home/norman/work/text-to-sql/MAC-SQL/data/bird",
                         dataset_name: str = "bird",
                         display_results: bool = True) -> Dict[str, Any]:
    """
    Quick function to run a text-to-SQL tree orchestration.
    
    Args:
        query: Natural language query
        db_name: Database name
        data_path: Path to database files
        dataset_name: Dataset name (bird, spider, etc.)
        display_results: Whether to display results
        
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
        db_name=db_name
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