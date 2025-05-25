"""
Complete Text-to-SQL Workflow Implementation

This module provides a complete text-to-SQL workflow using all 4 specialized agents:
- QueryAnalyzerAgent: Analyzes user queries and creates query trees
- SchemaLinkerAgent: Links query intents to database schema
- SQLGeneratorAgent: Generates SQL from linked schema
- SQLEvaluatorAgent: Executes and evaluates SQL results

The workflow is orchestrated by a coordinator agent that ensures correct SQL generation.
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


class TextToSQLWorkflow:
    """
    Complete text-to-SQL workflow using 4 specialized agents.
    
    This class orchestrates the entire workflow from query analysis to SQL execution.
    """
    
    def __init__(self, 
                 data_path: str,
                 tables_json_path: str,
                 dataset_name: str = "bird",
                 llm_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the text-to-SQL workflow.
        
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
        """Create the coordinator agent."""
        coordinator_client = OpenAIChatCompletionClient(
            model="gpt-4o",
            temperature=0.1,
            timeout=120,
            api_key=os.getenv("OPENAI_API_KEY")
        )
        
        coordinator = AssistantAgent(
            name="coordinator",
            system_message='''You coordinate a text-to-SQL workflow using 4 specialized agents.

Your agents are:
1. query_analyzer - Analyzes user queries and creates query trees
2. schema_linker - Links query intent to database schema
3. sql_generator - Generates SQL from linked schema
4. sql_evaluator - Executes and evaluates SQL results

Workflow:
1. Call query_analyzer to understand the query and create a query tree
2. Call schema_linker to link the query to relevant tables/columns
3. Call sql_generator to generate SQL
4. Call sql_evaluator to execute and evaluate results
5. If the evaluator indicates issues:
   - Analyze what went wrong
   - You may need to call agents again with better guidance
   - Continue iterating until you have correct SQL
6. Once you have correct SQL with good results, provide a final answer and say "TERMINATE"

IMPORTANT: Each agent operates on nodes in a query tree. The node_id is passed between agents.
Always use the format: 'node:{node_id} - Task description' when calling agents.''',
            model_client=coordinator_client,
            tools=[self.query_analyzer, self.schema_linker, self.sql_generator, self.sql_evaluator]
        )
        
        return coordinator
    
    async def process_query(self, 
                           query: str, 
                           db_name: str,
                           task_id: Optional[str] = None,
                           use_coordinator: bool = True) -> Dict[str, Any]:
        """
        Process a text-to-SQL query through the complete workflow.
        
        Args:
            query: The natural language query
            db_name: Name of the database to query
            task_id: Optional task ID (auto-generated if not provided)
            use_coordinator: Whether to use the coordinator agent
            
        Returns:
            Dictionary containing the results including SQL, execution results, and analysis
        """
        # Generate task ID if not provided
        if not task_id:
            import time
            task_id = f"workflow_{int(time.time())}"
        
        # Initialize task
        await self.task_manager.initialize(task_id, query, db_name)
        await self.initialize_database(db_name)
        
        self.logger.info(f"Processing query: {query}")
        self.logger.info(f"Database: {db_name}")
        self.logger.info(f"Task ID: {task_id}")
        
        if use_coordinator:
            return await self._process_with_coordinator(query)
        else:
            return await self._process_sequential(query)
    
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
        
        # Run the workflow
        self.logger.info("Starting coordinator-based workflow")
        stream = team.run_stream(task=query)
        
        messages = []
        async for message in stream:
            messages.append(message)
            if hasattr(message, 'source') and message.source == 'coordinator':
                if hasattr(message, 'content') and isinstance(message.content, str):
                    self.logger.debug(f"Coordinator: {message.content[:100]}...")
        
        # Get final results
        return await self._get_workflow_results()
    
    async def _process_sequential(self, query: str) -> Dict[str, Any]:
        """Process query by calling agents sequentially with validation."""
        self.logger.info("Starting sequential workflow")
        
        # Step 1: Query Analyzer
        self.logger.info("Step 1: Analyzing query...")
        await self.query_analyzer.run(query)
        
        # Get the created node
        tree = await self.tree_manager.get_tree()
        if not tree or "rootId" not in tree:
            raise RuntimeError("Failed to create query tree")
        
        node_id = tree["rootId"]
        self.logger.info(f"Created query tree with root node: {node_id}")
        
        # Validate query analysis
        node = await self.tree_manager.get_node(node_id)
        if not node or not node.intent:
            raise RuntimeError("Query analysis failed - no intent generated")
        
        # Step 2: Schema Linker
        self.logger.info("Step 2: Linking to schema...")
        task = f"node:{node_id} - Link query to database schema"
        await self.schema_linker.run(task)
        
        # Validate schema linking
        node = await self.tree_manager.get_node(node_id)
        if not node or not node.mapping or not node.mapping.tables:
            raise RuntimeError("Schema linking failed - no tables mapped")
        
        # Step 3: SQL Generator
        self.logger.info("Step 3: Generating SQL...")
        task = f"node:{node_id} - Generate SQL query"
        await self.sql_generator.run(task)
        
        # Validate SQL generation
        node = await self.tree_manager.get_node(node_id)
        if not node or not node.sql:
            raise RuntimeError("SQL generation failed - no SQL generated")
        
        # Validate SQL syntax
        sql_validation = self._validate_sql(node.sql)
        if not sql_validation["is_valid"]:
            self.logger.warning(f"Generated SQL may have issues: {sql_validation['issues']}")
        
        # Step 4: SQL Evaluator
        self.logger.info("Step 4: Evaluating SQL...")
        task = f"node:{node_id} - Analyze SQL execution results"
        await self.sql_evaluator.run(task)
        
        # Validate execution and evaluation
        node = await self.tree_manager.get_node(node_id)
        if not node or not node.executionResult:
            raise RuntimeError("SQL evaluation failed - no execution result")
        
        # Check if SQL executed successfully
        if node.executionResult.error:
            self.logger.error(f"SQL execution failed: {node.executionResult.error}")
            
            # Try to fix and retry once
            fixed_results = await self._attempt_sql_fix(node_id, node.sql, node.executionResult.error)
            if fixed_results:
                self.logger.info("SQL was fixed and executed successfully")
            else:
                raise RuntimeError(f"SQL execution failed and could not be fixed: {node.executionResult.error}")
        
        # Validate evaluation results
        analysis_key = f"node_{node_id}_analysis"
        analysis = await self.memory.get(analysis_key)
        
        evaluation_validation = self._validate_evaluation(analysis, node.executionResult)
        if not evaluation_validation["is_valid"]:
            self.logger.warning(f"Evaluation concerns: {evaluation_validation['issues']}")
        
        # Final validation
        final_validation = self._validate_final_result(node, analysis)
        if not final_validation["is_valid"]:
            raise RuntimeError(f"Final validation failed: {final_validation['issues']}")
        
        self.logger.info("✓ Workflow completed successfully with valid results")
        return await self._get_workflow_results()
    
    async def _get_workflow_results(self) -> Dict[str, Any]:
        """Extract and format the workflow results."""
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree:
            return {"error": "No query tree found"}
        
        results = {
            "query_tree": tree,
            "nodes": {},
            "final_results": []
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
            
            # Add to final results if it has SQL
            if node_data.get("sql"):
                results["final_results"].append(node_result)
        
        return results
    
    def _validate_sql(self, sql: str) -> Dict[str, Any]:
        """
        Validate generated SQL for basic syntax and structure.
        
        Args:
            sql: The SQL string to validate
            
        Returns:
            Dictionary with validation results
        """
        issues = []
        
        if not sql or not sql.strip():
            return {"is_valid": False, "issues": ["SQL is empty"]}
        
        sql_upper = sql.upper().strip()
        
        # Check for basic SQL structure
        if not any(sql_upper.startswith(keyword) for keyword in ["SELECT", "WITH"]):
            issues.append("SQL does not start with SELECT or WITH")
        
        # Check for FROM clause (most queries need it)
        if "SELECT" in sql_upper and "FROM" not in sql_upper:
            issues.append("SELECT query missing FROM clause")
        
        # Check for potential syntax issues
        if sql.count("(") != sql.count(")"):
            issues.append("Mismatched parentheses")
        
        # Check for incomplete statements
        if sql_upper.endswith("WHERE") or sql_upper.endswith("AND") or sql_upper.endswith("OR"):
            issues.append("SQL appears incomplete (ends with WHERE/AND/OR)")
        
        # Check for SQL injection patterns (basic)
        dangerous_patterns = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
        for pattern in dangerous_patterns:
            if pattern in sql_upper:
                issues.append(f"SQL contains potentially dangerous operation: {pattern}")
        
        return {
            "is_valid": len(issues) == 0,
            "issues": issues,
            "sql_length": len(sql),
            "has_select": "SELECT" in sql_upper,
            "has_from": "FROM" in sql_upper
        }
    
    def _validate_evaluation(self, analysis: Optional[Dict[str, Any]], 
                           execution_result: ExecutionResult) -> Dict[str, Any]:
        """
        Validate the evaluation results from SQLEvaluatorAgent.
        
        Args:
            analysis: Analysis results from the evaluator
            execution_result: SQL execution results
            
        Returns:
            Dictionary with validation results
        """
        issues = []
        
        # Check if analysis exists
        if not analysis:
            issues.append("No analysis results found")
            return {"is_valid": False, "issues": issues}
        
        # Check required analysis fields
        required_fields = ["answers_intent", "result_quality", "result_summary"]
        for field in required_fields:
            if field not in analysis:
                issues.append(f"Missing analysis field: {field}")
        
        # Validate answers_intent
        if "answers_intent" in analysis:
            valid_intent_values = ["yes", "no", "partially"]
            if analysis["answers_intent"] not in valid_intent_values:
                issues.append(f"Invalid answers_intent value: {analysis['answers_intent']}")
        
        # Validate result_quality
        if "result_quality" in analysis:
            valid_quality_values = ["excellent", "good", "acceptable", "poor"]
            if analysis["result_quality"] not in valid_quality_values:
                issues.append(f"Invalid result_quality value: {analysis['result_quality']}")
        
        # Check confidence score if present
        if "confidence_score" in analysis:
            confidence = analysis["confidence_score"]
            if not isinstance(confidence, (int, float)) or not (0 <= confidence <= 1):
                issues.append(f"Invalid confidence_score: {confidence}")
        
        # Cross-validate with execution results
        if execution_result.error and analysis.get("answers_intent") == "yes":
            issues.append("Analysis says 'yes' but SQL execution failed")
        
        if execution_result.rowCount == 0 and analysis.get("result_quality") in ["excellent", "good"]:
            issues.append("Analysis says quality is good but no rows returned")
        
        return {
            "is_valid": len(issues) == 0,
            "issues": issues,
            "has_analysis": analysis is not None,
            "answers_intent": analysis.get("answers_intent") if analysis else None,
            "result_quality": analysis.get("result_quality") if analysis else None
        }
    
    def _validate_final_result(self, node: QueryNode, 
                              analysis: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Perform final validation of the complete workflow result.
        
        Args:
            node: The query tree node with results
            analysis: Analysis results
            
        Returns:
            Dictionary with validation results
        """
        issues = []
        
        # Check node has all required components
        if not node.intent:
            issues.append("Node missing intent")
        
        if not node.sql:
            issues.append("Node missing SQL")
        
        if not node.executionResult:
            issues.append("Node missing execution result")
        
        if not node.mapping or not node.mapping.tables:
            issues.append("Node missing schema mapping")
        
        # Check execution was successful
        if node.executionResult and node.executionResult.error:
            issues.append(f"SQL execution failed: {node.executionResult.error}")
        
        # Check analysis quality
        if analysis:
            answers_intent = analysis.get("answers_intent")
            result_quality = analysis.get("result_quality")
            
            # Require good evaluation for success
            if answers_intent not in ["yes", "partially"]:
                issues.append(f"Query does not answer user intent: {answers_intent}")
            
            if result_quality not in ["excellent", "good", "acceptable"]:
                issues.append(f"Result quality is poor: {result_quality}")
        else:
            issues.append("No evaluation analysis available")
        
        # Success criteria
        is_successful = (
            len(issues) == 0 and
            node.sql is not None and
            node.executionResult is not None and
            not node.executionResult.error and
            analysis is not None and
            analysis.get("answers_intent") in ["yes", "partially"]
        )
        
        return {
            "is_valid": len(issues) == 0,
            "is_successful": is_successful,
            "issues": issues,
            "has_sql": node.sql is not None,
            "has_execution": node.executionResult is not None,
            "has_analysis": analysis is not None,
            "execution_success": node.executionResult and not node.executionResult.error,
            "answers_intent": analysis.get("answers_intent") if analysis else None
        }
    
    async def _attempt_sql_fix(self, node_id: str, original_sql: str, error: str) -> bool:
        """
        Attempt to fix SQL that failed execution.
        
        Args:
            node_id: The node ID with the failed SQL
            original_sql: The original SQL that failed
            error: The error message from execution
            
        Returns:
            True if SQL was fixed and executed successfully, False otherwise
        """
        try:
            self.logger.info(f"Attempting to fix SQL error: {error}")
            
            # Try regenerating SQL with error context
            task = f"node:{node_id} - Fix SQL error: {error}. Original SQL: {original_sql}"
            await self.sql_generator.run(task)
            
            # Check if new SQL was generated
            node = await self.tree_manager.get_node(node_id)
            if not node or not node.sql or node.sql == original_sql:
                return False
            
            # Try executing the new SQL
            task = f"node:{node_id} - Analyze SQL execution results"
            await self.sql_evaluator.run(task)
            
            # Check if execution succeeded
            node = await self.tree_manager.get_node(node_id)
            if node and node.executionResult and not node.executionResult.error:
                self.logger.info("SQL fix successful")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error during SQL fix attempt: {e}")
            return False
    
    async def display_query_tree(self) -> None:
        """Display the current query tree structure."""
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree:
            print("No query tree found")
            return
        
        print("\n" + "="*60)
        print("QUERY TREE STRUCTURE")
        print("="*60)
        
        for node_id, node_data in tree["nodes"].items():
            print(f"\nNode: {node_id}")
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
    
    async def display_final_results(self) -> None:
        """Display the final SQL and execution results."""
        tree = await self.tree_manager.get_tree()
        if not tree or "nodes" not in tree:
            print("No results found")
            return
        
        print("\n" + "="*60)
        print("FINAL RESULTS")
        print("="*60)
        
        # Find nodes with SQL
        for node_id, node_data in tree["nodes"].items():
            if 'sql' in node_data and node_data['sql']:
                print(f"\nNode: {node_id}")
                print(f"Intent: {node_data.get('intent', 'N/A')}")
                print(f"\nSQL:\n{node_data['sql']}")
                
                if 'executionResult' in node_data and node_data['executionResult']:
                    result = node_data['executionResult']
                    print(f"\nExecution Result:")
                    print(f"  Rows returned: {result.get('rowCount', 0)}")
                    
                    if result.get('data') and len(result['data']) > 0:
                        print(f"\nSample data (first 5 rows):")
                        for i, row in enumerate(result['data'][:5]):
                            print(f"  {row}")
                    
                    # Check for analysis
                    analysis_key = f"node_{node_id}_analysis"
                    analysis = await self.memory.get(analysis_key)
                    if analysis:
                        print(f"\nEvaluation:")
                        print(f"  Answers intent: {analysis.get('answers_intent', 'N/A')}")
                        print(f"  Result quality: {analysis.get('result_quality', 'N/A')}")
                        print(f"  Summary: {analysis.get('result_summary', 'N/A')}")


# Convenience function for quick workflow execution
async def run_text_to_sql(query: str, 
                         db_name: str,
                         data_path: str = "/home/norman/work/text-to-sql/MAC-SQL/data/bird",
                         dataset_name: str = "bird",
                         use_coordinator: bool = True,
                         display_results: bool = True) -> Dict[str, Any]:
    """
    Quick function to run a text-to-SQL query.
    
    Args:
        query: Natural language query
        db_name: Database name
        data_path: Path to database files
        dataset_name: Dataset name (bird, spider, etc.)
        use_coordinator: Whether to use coordinator agent
        display_results: Whether to display results
        
    Returns:
        Dictionary containing workflow results
    """
    # Load environment variables
    load_dotenv()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.getLogger('autogen_core').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    
    # Initialize workflow
    tables_json_path = str(Path(data_path) / "dev_tables.json")
    workflow = TextToSQLWorkflow(
        data_path=data_path,
        tables_json_path=tables_json_path,
        dataset_name=dataset_name
    )
    
    # Process query
    results = await workflow.process_query(
        query=query,
        db_name=db_name,
        use_coordinator=use_coordinator
    )
    
    # Display results if requested
    if display_results:
        await workflow.display_query_tree()
        await workflow.display_final_results()
        
        # Display validation summary
        print("\n" + "="*60)
        print("VALIDATION SUMMARY")
        print("="*60)
        
        if results.get("final_results"):
            final_result = results["final_results"][0]
            node_id = final_result["node_id"]
            
            # Get node for validation
            node = await workflow.tree_manager.get_node(node_id)
            analysis = final_result.get("analysis")
            
            if node:
                # SQL validation
                if node.sql:
                    sql_validation = workflow._validate_sql(node.sql)
                    status = "✓ VALID" if sql_validation["is_valid"] else "✗ ISSUES"
                    print(f"SQL Validation: {status}")
                    if sql_validation["issues"]:
                        for issue in sql_validation["issues"]:
                            print(f"  - {issue}")
                
                # Evaluation validation
                if node.executionResult:
                    eval_validation = workflow._validate_evaluation(analysis, node.executionResult)
                    status = "✓ VALID" if eval_validation["is_valid"] else "✗ ISSUES"
                    print(f"Evaluation Validation: {status}")
                    if eval_validation["issues"]:
                        for issue in eval_validation["issues"]:
                            print(f"  - {issue}")
                
                # Final validation
                final_validation = workflow._validate_final_result(node, analysis)
                success_status = "✓ SUCCESS" if final_validation["is_successful"] else "✗ FAILED"
                print(f"Overall Result: {success_status}")
                if final_validation["issues"]:
                    for issue in final_validation["issues"]:
                        print(f"  - {issue}")
                
                # Summary stats
                print(f"\nValidation Details:")
                print(f"  SQL Generated: {'✓' if final_validation['has_sql'] else '✗'}")
                print(f"  SQL Executed: {'✓' if final_validation['execution_success'] else '✗'}")
                print(f"  Has Analysis: {'✓' if final_validation['has_analysis'] else '✗'}")
                print(f"  Answers Intent: {final_validation['answers_intent'] or 'Unknown'}")
    
    return results


# Example usage and testing
if __name__ == "__main__":
    async def test_workflow():
        """Test the complete workflow."""
        # Example queries
        test_queries = [
            "What is the highest eligible free rate for K-12 students in schools located in Alameda County?",
            "Find the top 5 schools with the highest average math SAT scores",
            "How many schools are there in each county?"
        ]
        
        db_name = "california_schools"
        
        for i, query in enumerate(test_queries[:1]):  # Test first query only
            print(f"\n{'='*80}")
            print(f"TEST {i+1}: {query}")
            print('='*80)
            
            try:
                results = await run_text_to_sql(
                    query=query,
                    db_name=db_name,
                    use_coordinator=False,  # Use sequential for testing
                    display_results=True
                )
                
                print(f"\n✓ Test {i+1} completed successfully")
                
            except Exception as e:
                print(f"✗ Test {i+1} failed: {e}")
                import traceback
                traceback.print_exc()
    
    # Run test
    asyncio.run(test_workflow())