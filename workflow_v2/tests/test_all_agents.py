"""
Comprehensive test for all 4 agents in the workflow_v2 system.

Tests:
- QueryAnalyzerAgent: Analyzes queries and creates query trees
- SchemaLinkerAgent: Links database schema to query nodes  
- SQLGeneratorAgent: Generates SQL from linked schema
- SQLEvaluatorAgent: Evaluates SQL execution results
"""

import asyncio
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import all required components
from keyvalue_memory import KeyValueMemory
from task_context_manager import TaskContextManager
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from node_history_manager import NodeHistoryManager

# Import all 4 agents
from query_analyzer_agent import QueryAnalyzerAgent
from schema_linker_agent import SchemaLinkerAgent
from sql_generator_agent import SQLGeneratorAgent
from sql_evaluator_agent import SQLEvaluatorAgent

# Import memory types
from memory_content_types import (
    TaskContext, TableSchema, ColumnInfo, ExecutionResult
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TestAllAgents:
    """Test all 4 agents with a sample database and query"""
    
    def __init__(self):
        """Initialize test environment"""
        # Load environment variables
        load_dotenv()
        
        # Check for API key
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY not found in environment")
        
        # Initialize memory and managers
        self.memory = KeyValueMemory()
        self.task_manager = TaskContextManager(self.memory)
        self.schema_manager = DatabaseSchemaManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.history_manager = NodeHistoryManager(self.memory)
        
        # LLM config
        self.llm_config = {
            "model_name": "gpt-4o-mini",  # Using mini for testing
            "temperature": 0.1,
            "timeout": 30
        }
        
        # Initialize agents
        self.query_analyzer = QueryAnalyzerAgent(self.memory, self.llm_config)
        self.schema_linker = SchemaLinkerAgent(self.memory, self.llm_config)
        self.sql_generator = SQLGeneratorAgent(self.memory, self.llm_config)
        self.sql_evaluator = SQLEvaluatorAgent(self.memory, self.llm_config)
    
    async def setup_test_database(self):
        """Setup a simple test database schema"""
        logger.info("Setting up test database schema...")
        
        # Create sample tables
        tables = {
            "employees": TableSchema(
                name="employees",
                columns={
                    "employee_id": ColumnInfo(
                        dataType="INTEGER",
                        isPrimaryKey=True,
                        isForeignKey=False,
                        nullable=False
                    ),
                    "name": ColumnInfo(
                        dataType="VARCHAR(100)",
                        isPrimaryKey=False,
                        isForeignKey=False,
                        nullable=False
                    ),
                    "department": ColumnInfo(
                        dataType="VARCHAR(50)",
                        isPrimaryKey=False,
                        isForeignKey=False,
                        nullable=True
                    ),
                    "salary": ColumnInfo(
                        dataType="DECIMAL(10,2)",
                        isPrimaryKey=False,
                        isForeignKey=False,
                        nullable=False
                    ),
                    "hire_date": ColumnInfo(
                        dataType="DATE",
                        isPrimaryKey=False,
                        isForeignKey=False,
                        nullable=False
                    )
                }
            ),
            "departments": TableSchema(
                name="departments",
                columns={
                    "dept_id": ColumnInfo(
                        dataType="INTEGER",
                        isPrimaryKey=True,
                        isForeignKey=False,
                        nullable=False
                    ),
                    "dept_name": ColumnInfo(
                        dataType="VARCHAR(50)",
                        isPrimaryKey=False,
                        isForeignKey=False,
                        nullable=False
                    ),
                    "budget": ColumnInfo(
                        dataType="DECIMAL(12,2)",
                        isPrimaryKey=False,
                        isForeignKey=False,
                        nullable=True
                    )
                }
            )
        }
        
        # Store tables in schema manager
        for table_name, table_schema in tables.items():
            await self.schema_manager.add_table(table_schema)
        
        # Add sample data for context
        sample_data = {
            "employees": [
                {"employee_id": 1, "name": "John Doe", "department": "Engineering", "salary": 85000, "hire_date": "2020-01-15"},
                {"employee_id": 2, "name": "Jane Smith", "department": "Sales", "salary": 75000, "hire_date": "2019-06-01"},
                {"employee_id": 3, "name": "Bob Johnson", "department": "Engineering", "salary": 95000, "hire_date": "2018-03-20"}
            ],
            "departments": [
                {"dept_id": 1, "dept_name": "Engineering", "budget": 1000000},
                {"dept_id": 2, "dept_name": "Sales", "budget": 500000},
                {"dept_id": 3, "dept_name": "HR", "budget": 300000}
            ]
        }
        
        # Store sample data (for SQL evaluator)
        await self.memory.set("sample_data", sample_data)
        
        logger.info(f"Created {len(tables)} tables with sample data")
    
    async def test_query_analyzer(self, query: str, evidence: str = None):
        """Test 1: QueryAnalyzerAgent"""
        logger.info("\n" + "="*60)
        logger.info("TEST 1: QueryAnalyzerAgent")
        logger.info("="*60)
        
        # Initialize task context
        await self.task_manager.initialize("test_task_1", query, "test_db", evidence)
        
        # Run query analyzer
        tool = self.query_analyzer.get_tool()
        result = await tool.run(
            tool._args_type(task=query),
            cancellation_token=None
        )
        
        # Check results
        tree_stats = await self.tree_manager.get_tree_stats()
        root_id = await self.tree_manager.get_root_id()
        root_node = None
        if root_id:
            root_node = await self.tree_manager.get_node(root_id)
        
        logger.info(f"Query analyzed: {query}")
        if evidence:
            logger.info(f"Evidence: {evidence}")
        logger.info(f"Tree stats: {tree_stats}")
        if root_node:
            logger.info(f"Root node created: {root_node.nodeId}")
            logger.info(f"Root intent: {root_node.intent}")
        
        return root_node
    
    async def test_schema_linker(self, node_id: str):
        """Test 2: SchemaLinkerAgent"""
        logger.info("\n" + "="*60)
        logger.info("TEST 2: SchemaLinkerAgent")
        logger.info("="*60)
        
        # Set current node
        await self.tree_manager.set_current_node_id(node_id)
        
        # Run schema linker
        tool = self.schema_linker.get_tool()
        result = await tool.run(
            tool._args_type(task="Link schema for current query node"),
            cancellation_token=None
        )
        
        # Check results
        node = await self.tree_manager.get_node(node_id)
        if node and node.mapping:
            logger.info(f"Schema linked for node: {node_id}")
            logger.info(f"Tables: {[t.name for t in node.mapping.tables]}")
            logger.info(f"Columns: {len(node.mapping.columns)}")
            if node.mapping.joins:
                logger.info(f"Joins: {len(node.mapping.joins)}")
        
        return node
    
    async def test_sql_generator(self, node_id: str):
        """Test 3: SQLGeneratorAgent"""
        logger.info("\n" + "="*60)
        logger.info("TEST 3: SQLGeneratorAgent")
        logger.info("="*60)
        
        # Set current node
        await self.tree_manager.set_current_node_id(node_id)
        
        # Run SQL generator
        tool = self.sql_generator.get_tool()
        result = await tool.run(
            tool._args_type(task="Generate SQL for current query node"),
            cancellation_token=None
        )
        
        # Check results
        node = await self.tree_manager.get_node(node_id)
        if node and node.sql:
            logger.info(f"SQL generated for node: {node_id}")
            logger.info(f"SQL: {node.sql}")
        
        return node
    
    async def test_sql_evaluator(self, node_id: str):
        """Test 4: SQLEvaluatorAgent"""
        logger.info("\n" + "="*60)
        logger.info("TEST 4: SQLEvaluatorAgent")
        logger.info("="*60)
        
        # Set current node
        await self.tree_manager.set_current_node_id(node_id)
        
        # For testing, we'll simulate SQL execution with sample results
        node = await self.tree_manager.get_node(node_id)
        if node and node.sql:
            # Simulate execution result based on the query
            if "AVG" in node.sql.upper() and "salary" in node.sql.lower():
                exec_result = ExecutionResult(
                    data=[{"avg_salary": 85000.0}],
                    rowCount=1,
                    error=None
                )
            else:
                exec_result = ExecutionResult(
                    data=[
                        {"name": "Bob Johnson", "salary": 95000},
                        {"name": "John Doe", "salary": 85000}
                    ],
                    rowCount=2,
                    error=None
                )
            
            # Update node with execution result
            await self.tree_manager.update_node_result(node_id, exec_result, success=True)
        
        # Run SQL evaluator
        tool = self.sql_evaluator.get_tool()
        result = await tool.run(
            tool._args_type(task="Evaluate SQL results for current query node"),
            cancellation_token=None
        )
        
        # Check evaluation results
        analysis_key = f"node_{node_id}_analysis"
        analysis = await self.memory.get(analysis_key)
        if analysis:
            logger.info(f"SQL evaluated for node: {node_id}")
            logger.info(f"Answers intent: {analysis.get('answers_intent')}")
            logger.info(f"Result quality: {analysis.get('result_quality')}")
            if analysis.get('issues'):
                logger.info(f"Issues: {analysis.get('issues')}")
        
        return analysis
    
    async def run_all_tests(self):
        """Run all agent tests in sequence"""
        try:
            # Setup
            await self.setup_test_database()
            
            # Test queries
            test_cases = [
                {
                    "query": "What is the average salary of employees in the Engineering department?",
                    "evidence": "Department names are stored in the 'department' column of employees table"
                },
                {
                    "query": "List the top 2 highest paid employees with their salaries",
                    "evidence": None
                }
            ]
            
            for i, test_case in enumerate(test_cases):
                logger.info(f"\n{'#'*60}")
                logger.info(f"TEST CASE {i+1}: {test_case['query']}")
                logger.info(f"{'#'*60}")
                
                # Test 1: Query Analyzer
                root_node = await self.test_query_analyzer(
                    test_case['query'], 
                    test_case['evidence']
                )
                
                if not root_node:
                    logger.error("Query analyzer failed to create root node")
                    continue
                
                # Test 2: Schema Linker
                linked_node = await self.test_schema_linker(root_node.nodeId)
                
                if not linked_node or not linked_node.mapping:
                    logger.error("Schema linker failed to link schema")
                    continue
                
                # Test 3: SQL Generator
                sql_node = await self.test_sql_generator(root_node.nodeId)
                
                if not sql_node or not sql_node.sql:
                    logger.error("SQL generator failed to generate SQL")
                    continue
                
                # Test 4: SQL Evaluator
                evaluation = await self.test_sql_evaluator(root_node.nodeId)
                
                if not evaluation:
                    logger.error("SQL evaluator failed to evaluate SQL")
                    continue
                
                logger.info(f"\n{'='*60}")
                logger.info(f"TEST CASE {i+1} COMPLETED SUCCESSFULLY")
                logger.info(f"{'='*60}")
            
            # Final summary
            logger.info("\n" + "#"*60)
            logger.info("ALL TESTS COMPLETED")
            logger.info("#"*60)
            
            # Get final tree stats
            tree_stats = await self.tree_manager.get_tree_stats()
            logger.info(f"Final tree stats: {tree_stats}")
            
        except Exception as e:
            logger.error(f"Test failed with error: {str(e)}", exc_info=True)
            raise


async def main():
    """Main test runner"""
    print("Starting comprehensive agent tests...")
    print("This will test all 4 agents in sequence")
    print("-" * 60)
    
    tester = TestAllAgents()
    await tester.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())