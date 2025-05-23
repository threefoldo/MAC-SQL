"""
Orchestrator Agent for text-to-SQL workflow.

This agent orchestrates the entire text-to-SQL conversion process by:
1. Managing the workflow
2. Calling appropriate agents as tools
3. Making decisions based on the current state
4. Ensuring all nodes have correct SQL
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime

from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.messages import TextMessage, ToolCallMessage, ToolCallResultMessage
from autogen_core.tools import FunctionTool

from memory import KeyValueMemory
from task_context_manager import TaskContextManager
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager
from schema_reader import SchemaReader

from query_analyzer_agent import QueryAnalyzerAgent
from schema_linking_agent import SchemaLinkingAgent
from sql_generator_agent import SQLGeneratorAgent
from sql_executor_agent import SQLExecutorAgent
from sql_executor import SQLExecutor

from memory_types import TaskStatus, NodeStatus


class OrchestratorAgent:
    """
    Orchestrates the text-to-SQL workflow by coordinating multiple agents.
    
    This agent:
    1. Initializes the workflow with database schema
    2. Analyzes the user query
    3. Links schema to query nodes
    4. Generates SQL for all nodes
    5. Executes and validates SQL
    6. Updates final results
    """
    
    def __init__(self,
                 memory: KeyValueMemory,
                 schema_reader: SchemaReader,
                 sql_executor: SQLExecutor,
                 model_name: str = "gpt-4o",
                 debug: bool = False):
        """
        Initialize the orchestrator agent.
        
        Args:
            memory: The KeyValueMemory instance
            schema_reader: The schema reader for loading database schemas
            sql_executor: The SQL executor for running queries
            model_name: The LLM model to use
            debug: Whether to enable debug logging
        """
        self.memory = memory
        self.schema_reader = schema_reader
        self.sql_executor = sql_executor
        self.model_name = model_name
        self.debug = debug
        
        # Initialize managers
        self.task_manager = TaskContextManager(memory)
        self.schema_manager = DatabaseSchemaManager(memory)
        self.tree_manager = QueryTreeManager(memory)
        self.history_manager = NodeHistoryManager(memory)
        
        # Initialize agents
        self.query_analyzer = QueryAnalyzerAgent(memory, model_name, debug)
        self.schema_linker = SchemaLinkingAgent(memory, model_name, debug)
        self.sql_generator = SQLGeneratorAgent(memory, model_name, debug)
        self.sql_executor_agent = SQLExecutorAgent(memory, sql_executor, model_name, debug)
        
        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)
        if debug:
            self.logger.setLevel(logging.DEBUG)
        
        # Create the orchestrator agent
        self._setup_agent()
    
    def _setup_agent(self):
        """Setup the orchestrator agent with tools."""
        
        # Define agent tools
        self.tools = [
            FunctionTool(
                self._analyze_query,
                description="Analyze the user query and decompose if needed"
            ),
            FunctionTool(
                self._link_schema,
                description="Link database schema to a query node"
            ),
            FunctionTool(
                self._generate_sql,
                description="Generate SQL for a query node"
            ),
            FunctionTool(
                self._execute_sql,
                description="Execute and evaluate SQL for a query node"
            ),
            FunctionTool(
                self._get_tree_status,
                description="Get the current status of the query tree"
            ),
            FunctionTool(
                self._get_node_details,
                description="Get detailed information about a specific node"
            ),
            FunctionTool(
                self._update_final_result,
                description="Update the final result and complete the task"
            )
        ]
        
        # System message for the orchestrator
        system_message = """You are an orchestrator for text-to-SQL conversion workflow.

Your goal is to convert a natural language query into correct SQL by:
1. Analyzing the query (may decompose complex queries)
2. Linking relevant schema to each query node
3. Generating SQL for each node
4. Executing and validating SQL
5. Ensuring all nodes have correct SQL before completing

Workflow steps:
1. First, analyze the query using analyze_query tool
2. Get tree status to see all nodes
3. For each node without schema mapping, use link_schema
4. For each node with mapping but no SQL, use generate_sql
5. For each node with SQL, use execute_sql to validate
6. If any SQL fails or needs improvement, regenerate
7. When all nodes have correct SQL, use update_final_result

Make decisions based on the current state and call appropriate tools.
Always check tree status after major operations.
"""
        
        # Create the assistant agent
        self.agent = AssistantAgent(
            name="orchestrator",
            model_client=self._create_model_client(),
            tools=self.tools,
            system_message=system_message,
        )
    
    def _create_model_client(self):
        """Create the model client for the agent."""
        # This is a placeholder - implement based on your autogen setup
        # You'll need to configure the appropriate model client here
        from autogen_ext.models import OpenAIChatCompletionClient
        return OpenAIChatCompletionClient(model=self.model_name)
    
    async def _analyze_query(self, query: str) -> Dict[str, Any]:
        """
        Analyze the user query and create query tree.
        
        Args:
            query: The natural language query
            
        Returns:
            Analysis result with tree structure
        """
        self.logger.info(f"Analyzing query: {query}")
        
        result = await self.query_analyzer.analyze(query)
        
        # Get tree status after analysis
        tree_stats = await self.tree_manager.get_tree_stats()
        
        return {
            "status": "success" if result and not result.get("error") else "failed",
            "complexity": result.get("complexity", "unknown"),
            "node_count": tree_stats.get("total_nodes", 0),
            "message": f"Query analyzed. Created {tree_stats.get('total_nodes', 0)} nodes."
        }
    
    async def _link_schema(self, node_id: str) -> Dict[str, Any]:
        """
        Link schema to a query node.
        
        Args:
            node_id: The node ID to link schema for
            
        Returns:
            Linking result
        """
        self.logger.info(f"Linking schema for node: {node_id}")
        
        result = await self.schema_linker.link_schema(node_id)
        
        if result and not result.get("error"):
            tables = [t["name"] for t in result.get("tables", [])]
            return {
                "status": "success",
                "node_id": node_id,
                "tables_linked": tables,
                "message": f"Linked {len(tables)} tables to node"
            }
        else:
            return {
                "status": "failed",
                "node_id": node_id,
                "error": result.get("error", "Unknown error"),
                "message": "Failed to link schema"
            }
    
    async def _generate_sql(self, node_id: str) -> Dict[str, Any]:
        """
        Generate SQL for a query node.
        
        Args:
            node_id: The node ID to generate SQL for
            
        Returns:
            Generation result
        """
        self.logger.info(f"Generating SQL for node: {node_id}")
        
        result = await self.sql_generator.generate_sql(node_id)
        
        if result and not result.get("error"):
            return {
                "status": "success",
                "node_id": node_id,
                "query_type": result.get("query_type", "unknown"),
                "message": "SQL generated successfully"
            }
        else:
            return {
                "status": "failed",
                "node_id": node_id,
                "error": result.get("error", "Unknown error"),
                "message": "Failed to generate SQL"
            }
    
    async def _execute_sql(self, node_id: str) -> Dict[str, Any]:
        """
        Execute and evaluate SQL for a node.
        
        Args:
            node_id: The node ID to execute SQL for
            
        Returns:
            Execution and evaluation result
        """
        self.logger.info(f"Executing SQL for node: {node_id}")
        
        result = await self.sql_executor_agent.execute_and_evaluate(node_id)
        
        execution = result.get("execution", {})
        evaluation = result.get("evaluation", {})
        
        return {
            "status": "success" if execution.get("success") else "failed",
            "node_id": node_id,
            "row_count": execution.get("row_count", 0),
            "matches_intent": evaluation.get("result_analysis", {}).get("matches_intent", False),
            "usable": evaluation.get("final_verdict", {}).get("usable", False),
            "improvements_needed": evaluation.get("final_verdict", {}).get("confidence") == "low",
            "message": evaluation.get("final_verdict", {}).get("summary", "Execution completed")
        }
    
    async def _get_tree_status(self) -> Dict[str, Any]:
        """
        Get the current status of the query tree.
        
        Returns:
            Tree status with node states
        """
        tree = await self.tree_manager.get_tree()
        if not tree:
            return {"error": "No query tree found"}
        
        nodes_status = {
            "total": 0,
            "no_mapping": [],
            "no_sql": [],
            "not_executed": [],
            "failed": [],
            "completed": []
        }
        
        for node_id, node_data in tree.get("nodes", {}).items():
            nodes_status["total"] += 1
            
            # Check node state
            has_mapping = bool(node_data.get("mapping", {}).get("tables"))
            has_sql = bool(node_data.get("sql"))
            status = node_data.get("status", "")
            
            if not has_mapping:
                nodes_status["no_mapping"].append(node_id)
            elif not has_sql:
                nodes_status["no_sql"].append(node_id)
            elif status == NodeStatus.SQL_GENERATED.value:
                nodes_status["not_executed"].append(node_id)
            elif status == NodeStatus.EXECUTED_FAILED.value:
                nodes_status["failed"].append(node_id)
            elif status == NodeStatus.EXECUTED_SUCCESS.value:
                nodes_status["completed"].append(node_id)
        
        # Determine next action
        if nodes_status["no_mapping"]:
            next_action = f"Link schema for nodes: {nodes_status['no_mapping'][:3]}"
        elif nodes_status["no_sql"]:
            next_action = f"Generate SQL for nodes: {nodes_status['no_sql'][:3]}"
        elif nodes_status["not_executed"]:
            next_action = f"Execute SQL for nodes: {nodes_status['not_executed'][:3]}"
        elif nodes_status["failed"]:
            next_action = f"Fix failed nodes: {nodes_status['failed'][:3]}"
        else:
            next_action = "All nodes completed! Update final result."
        
        return {
            "nodes_status": nodes_status,
            "all_completed": len(nodes_status["completed"]) == nodes_status["total"],
            "next_action": next_action
        }
    
    async def _get_node_details(self, node_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a node.
        
        Args:
            node_id: The node ID
            
        Returns:
            Node details
        """
        node = await self.tree_manager.get_node(node_id)
        if not node:
            return {"error": "Node not found"}
        
        details = {
            "node_id": node_id,
            "intent": node.intent,
            "status": node.status.value,
            "has_mapping": bool(node.mapping and node.mapping.tables),
            "has_sql": bool(node.sql),
            "has_result": bool(node.executionResult)
        }
        
        if node.mapping and node.mapping.tables:
            details["tables"] = [t.name for t in node.mapping.tables]
        
        if node.sql:
            details["sql_preview"] = node.sql[:100] + "..." if len(node.sql) > 100 else node.sql
        
        if node.executionResult:
            details["row_count"] = node.executionResult.rowCount
            details["has_error"] = bool(node.executionResult.error)
        
        return details
    
    async def _update_final_result(self) -> Dict[str, Any]:
        """
        Update the final result and complete the task.
        
        Returns:
            Completion status
        """
        # Get all successful nodes
        successful_nodes = await self.tree_manager.get_successful_nodes()
        
        if not successful_nodes:
            return {
                "status": "failed",
                "message": "No successful nodes found"
            }
        
        # Get root node
        root_id = await self.tree_manager.get_root_id()
        root = await self.tree_manager.get_node(root_id) if root_id else None
        
        # Prepare final result
        final_result = {
            "query": root.intent if root else "Unknown",
            "total_nodes": len(successful_nodes),
            "final_sql": root.sql if root and root.sql else None,
            "execution_summary": {}
        }
        
        # Add execution summaries
        for node in successful_nodes:
            if node.executionResult:
                final_result["execution_summary"][node.nodeId] = {
                    "intent": node.intent,
                    "row_count": node.executionResult.rowCount
                }
        
        # Store final result
        await self.memory.set("final_result", final_result)
        
        # Update task status
        await self.task_manager.mark_as_completed()
        
        return {
            "status": "success",
            "message": f"Task completed successfully! Processed {len(successful_nodes)} nodes.",
            "final_result": final_result
        }
    
    async def initialize_schema(self, db_id: str) -> None:
        """
        Initialize the database schema in memory.
        
        Args:
            db_id: The database ID to load schema for
        """
        self.logger.info(f"Initializing schema for database: {db_id}")
        
        # Get schema from schema reader
        schema_xml, fk_infos, schema_dict = self.schema_reader.generate_schema_description(
            db_id, {}, use_gold_schema=False
        )
        
        # Convert to our schema format and store in memory
        await self.schema_manager.initialize()
        
        # Load schema information
        db_info = self.schema_reader._load_single_db_info(db_id)
        
        # Convert and store each table
        for table_name, columns_info in db_info["desc_dict"].items():
            # This is a simplified conversion - you may need to enhance based on actual format
            self.logger.info(f"Loading table: {table_name}")
            # The actual implementation would convert the schema reader format
            # to TableSchema objects and store them using schema_manager
    
    async def run(self, query: str, db_id: str) -> Dict[str, Any]:
        """
        Run the orchestrator for a query.
        
        Args:
            query: The natural language query
            db_id: The database ID
            
        Returns:
            Final result
        """
        # Initialize task
        task_id = f"task_{datetime.now().timestamp()}"
        await self.task_manager.initialize(task_id, query, db_id)
        
        # Initialize schema
        await self.initialize_schema(db_id)
        
        # Create initial message for the agent
        initial_message = f"""Process this query: "{query}"
        
Database: {db_id}
Schema has been loaded.

Start by analyzing the query, then follow the workflow to generate SQL for all nodes."""
        
        # Run the agent
        messages = [TextMessage(content=initial_message, source="user")]
        
        # Process messages until complete
        max_iterations = 50
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            # Get agent response
            response = await self.agent.on_messages(messages, None)
            
            if response.chat_message:
                messages.append(response.chat_message)
                
                # Check if task is completed
                task_status = await self.task_manager.get_status()
                if task_status == TaskStatus.COMPLETED:
                    break
                
                # Check for termination
                if "final_result" in response.chat_message.content.lower():
                    break
            
            # Prevent infinite loops
            if iteration >= max_iterations:
                self.logger.warning("Max iterations reached")
                break
        
        # Get final result
        final_result = await self.memory.get("final_result")
        
        return final_result if final_result else {"error": "Task did not complete successfully"}