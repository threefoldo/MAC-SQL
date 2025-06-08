"""
Test script for SQLGeneratorAgent with execution tools.

This script tests:
1. SQL generation with execution feedback
2. Tool usage by the LLM (execute_sql)
3. Execution error recovery and SQL iteration
"""

import asyncio
import logging
from keyvalue_memory import KeyValueMemory
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from sql_generator_agent import SQLGeneratorAgent
from memory_content_types import QueryNode, NodeStatus
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def setup_test_environment():
    """Setup memory with test data"""
    memory = KeyValueMemory()
    
    # Setup task context properly
    from task_context_manager import TaskContextManager
    task_manager = TaskContextManager(memory)
    await task_manager.initialize(
        task_id="test_task_1",
        original_query="List the zip codes of charter schools in Fresno",
        database_name="california_schools",
        evidence=None
    )
    
    # Setup simple schema
    schema_manager = DatabaseSchemaManager(memory)
    await schema_manager.initialize()
    
    # Set database metadata for SQL execution
    await schema_manager.set_database_description("California schools database")
    
    # Store metadata for SQL execution
    await memory.set("schema_metadata", {
        "database_name": "california_schools",
        "data_path": "/home/norman/work/text-to-sql/MAC-SQL/data/bird",
        "dataset_name": "bird"
    })
    
    # Add test tables to schema
    test_schema = {
        "schools": {
            "description": "School information",
            "columns": [
                {"name": "CDSCode", "type": "TEXT", "description": "School code"},
                {"name": "School", "type": "TEXT", "description": "School name"}, 
                {"name": "Zip", "type": "TEXT", "description": "Zip code"},
                {"name": "County", "type": "TEXT", "description": "County name"},
                {"name": "Charter", "type": "INTEGER", "description": "1 if charter school, 0 otherwise"}
            ]
        },
        "frpm": {
            "description": "Free/Reduced Price Meal data",
            "columns": [
                {"name": "CDSCode", "type": "TEXT", "description": "School code"},
                {"name": "School Name", "type": "TEXT", "description": "School name"},
                {"name": "County Name", "type": "TEXT", "description": "County name"},
                {"name": "Charter School (Y/N)", "type": "INTEGER", "description": "1 if charter, 0 if not"},
                {"name": "District Name", "type": "TEXT", "description": "District name"}
            ]
        }
    }
    
    # Add tables to schema manager
    from memory_content_types import TableSchema, ColumnInfo
    
    # Add schools table
    schools_table = TableSchema(
        name="schools",
        columns={
            "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=False),
            "School": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "Zip": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "County": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "Charter": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
        }
    )
    await schema_manager.add_table(schools_table)
    
    # Add frpm table
    frpm_table = TableSchema(
        name="frpm",
        columns={
            "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=False),
            "School Name": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "County Name": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "Charter School (Y/N)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "District Name": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
        }
    )
    await schema_manager.add_table(frpm_table)
    
    # Setup query tree with test node
    tree_manager = QueryTreeManager(memory)
    await tree_manager.initialize("Root query")
    
    # Create a test node with common SQL errors
    test_node = QueryNode(
        nodeId="test_node_1",
        status=NodeStatus.CREATED,
        intent="List the zip codes of charter schools in Fresno County",
        schema_linking={
            "selected_tables": {
                "table": [
                    {
                        "name": "schools",
                        "alias": "s",
                        "columns": {
                            "column": [
                                {"name": "Zip", "usage": "select"},
                                {"name": "County", "usage": "filter"},
                                {"name": "Charter", "usage": "filter"}
                            ]
                        }
                    }
                ]
            }
        }
    )
    
    await tree_manager.add_node(test_node)
    await tree_manager.set_current_node_id("test_node_1")
    
    return memory


async def test_sql_generation_with_errors():
    """Test SQL generation with execution and error recovery"""
    logger.info("=== Testing SQL Generation with Execution Tools ===")
    
    # Setup
    memory = await setup_test_environment()
    
    # Create SQL generator agent
    sql_generator = SQLGeneratorAgent(
        memory=memory,
        llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1
        },
        debug=True
    )
    
    # Test cases that should trigger validation errors
    test_cases = [
        {
            "name": "Backtick Error Test",
            "node_update": {
                "intent": "List schools where county = `Fresno`",  # Backtick error
            }
        },
        {
            "name": "SQLite Function Error Test", 
            "node_update": {
                "intent": "List schools opened in 1980",
                "schema_linking": {
                    "selected_tables": {
                        "table": [{
                            "name": "schools",
                            "columns": {
                                "column": [
                                    {"name": "School", "usage": "select"},
                                    {"name": "OpenDate", "usage": "filter"}
                                ]
                            }
                        }]
                    }
                }
            }
        },
        {
            "name": "Column Not Found Test",
            "node_update": {
                "intent": "List school addresses",  # 'address' column doesn't exist
            }
        }
    ]
    
    tree_manager = QueryTreeManager(memory)
    
    for test_case in test_cases:
        logger.info(f"\n--- Running: {test_case['name']} ---")
        
        # Update node with test case
        await tree_manager.update_node("test_node_1", test_case["node_update"])
        
        # Reset node status
        await tree_manager.update_node("test_node_1", {"status": NodeStatus.CREATED})
        
        # Run SQL generator
        try:
            result = await sql_generator.tool.run(
                task=f"Generate SQL for: {test_case['node_update']['intent']}",
                cancellation_token=None
            )
            
            # Check generated SQL
            node = await tree_manager.get_node("test_node_1")
            if node and node.sql:
                logger.info(f"Generated SQL: {node.sql}")
                
                # The LLM should have used execution tools - check logs
                if node.generation:
                    logger.info(f"Generation details: {json.dumps(node.generation, indent=2)}")
            else:
                logger.warning("No SQL generated")
                
        except Exception as e:
            logger.error(f"Error in test case: {str(e)}", exc_info=True)
            
        # Small delay between tests
        await asyncio.sleep(2)


async def test_direct_tool_usage():
    """Test the SQL generator tools directly"""
    logger.info("\n=== Testing SQL Generator Tools Directly ===")
    
    memory = await setup_test_environment()
    from sql_generator_tools import SQLGeneratorTools
    
    tools = SQLGeneratorTools(memory, logger)
    
    # Test 1: Check table columns
    logger.info("\n--- Test: check_table_columns ---")
    result = await tools.check_table_columns("schools")
    logger.info(f"Table 'schools' info: {json.dumps(result, indent=2)}")
    
    # Test 2: Check non-existent table
    result = await tools.check_table_columns("students")
    logger.info(f"Table 'students' info: {json.dumps(result, indent=2)}")
    
    # Test 3: Execute SQL with errors
    logger.info("\n--- Test: execute_sql with backtick error ---")
    bad_sql = "SELECT * FROM schools WHERE County = `Fresno`"
    result = await tools.execute_sql(bad_sql)
    logger.info(f"Execution result: {json.dumps(result, indent=2)}")
    
    # Test 4: Execute SQL with SQLite incompatibility
    logger.info("\n--- Test: execute_sql with EXTRACT function ---")
    bad_sql = "SELECT * FROM schools WHERE EXTRACT(YEAR FROM OpenDate) = 1980"
    result = await tools.execute_sql(bad_sql)
    logger.info(f"Execution result: {json.dumps(result, indent=2)}")
    
    # Test 5: Execute valid SQL
    logger.info("\n--- Test: execute_sql with valid query ---")
    good_sql = "SELECT Zip FROM schools WHERE Charter = 1 AND County = 'Fresno' LIMIT 5"
    result = await tools.execute_sql(good_sql)
    logger.info(f"Execution result: {json.dumps(result, indent=2)}")
    
    # Test 6: List all tables
    logger.info("\n--- Test: list_all_tables ---")
    result = await tools.list_all_tables()
    logger.info(f"All tables: {json.dumps(result, indent=2)}")


async def main():
    """Run all tests"""
    try:
        # Test direct tool usage first
        await test_direct_tool_usage()
        
        # Then test SQL generation with LLM
        logger.info("\n" + "="*60 + "\n")
        await test_sql_generation_with_errors()
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())