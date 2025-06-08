"""
Simple test for SQL Generator tools without requiring OpenAI API.
Tests the execute_sql functionality directly.
"""

import asyncio
import logging
from keyvalue_memory import KeyValueMemory
from database_schema_manager import DatabaseSchemaManager
from task_context_manager import TaskContextManager
from sql_generator_tools import SQLGeneratorTools
from memory_content_types import TableSchema, ColumnInfo
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def setup_environment():
    """Setup test environment with california_schools schema"""
    memory = KeyValueMemory()
    
    # Setup task context
    task_manager = TaskContextManager(memory)
    await task_manager.initialize(
        task_id="test_task",
        original_query="Test SQL execution",
        database_name="california_schools",
        evidence=None
    )
    
    # Setup schema
    schema_manager = DatabaseSchemaManager(memory)
    await schema_manager.initialize()
    await schema_manager.set_database_description("California schools database")
    
    # Store metadata for SQL execution
    schema_summary = await schema_manager.get_schema_summary()
    if schema_summary:
        schema_summary["metadata"] = {
            "data_path": "/home/norman/work/text-to-sql/MAC-SQL/data/bird",
            "dataset_name": "bird"
        }
        # Store with the database name key that execute_sql looks for
        await memory.set("schema_summary", schema_summary)
    
    return memory


async def test_execute_sql():
    """Test the execute_sql tool with various queries"""
    memory = await setup_environment()
    tools = SQLGeneratorTools(memory, logger)
    
    print("\n" + "="*60)
    print("Testing SQL Execution Tool")
    print("="*60)
    
    test_queries = [
        {
            "name": "Simple SELECT",
            "sql": "SELECT CDSCode, School, County FROM schools WHERE County = 'Alameda' LIMIT 5"
        },
        {
            "name": "Aggregation",
            "sql": "SELECT COUNT(*) as total_schools FROM schools WHERE Charter = 1"
        },
        {
            "name": "JOIN query",
            "sql": """
                SELECT s.School, sat.AvgScrMath
                FROM schools s
                JOIN satscores sat ON s.CDSCode = sat.cds
                WHERE s.County = 'Los Angeles'
                ORDER BY sat.AvgScrMath DESC
                LIMIT 10
            """
        },
        {
            "name": "Error case - bad column",
            "sql": "SELECT NonExistentColumn FROM schools"
        },
        {
            "name": "Error case - bad syntax",
            "sql": "SELECT FROM schools WHERE"
        }
    ]
    
    for test in test_queries:
        print(f"\n--- Test: {test['name']} ---")
        print(f"SQL: {test['sql'].strip()}")
        
        result = await tools.execute_sql(test['sql'])
        
        print(f"\nResult:")
        print(f"Status: {result['status']}")
        print(f"Row count: {result['row_count']}")
        
        if result['status'] == 'success':
            print(f"Columns: {result['columns']}")
            if result['data']:
                print(f"First 3 rows:")
                for i, row in enumerate(result['data'][:3]):
                    print(f"  {i+1}: {row}")
        else:
            print(f"Error: {result['error']}")
        
        print("-" * 40)


async def test_schema_tools():
    """Test schema inspection tools"""
    memory = await setup_environment()
    
    # Add some tables to the schema for testing
    schema_manager = DatabaseSchemaManager(memory)
    
    # Add schools table
    schools_table = TableSchema(
        name="schools",
        columns={
            "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=False),
            "School": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "County": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "City": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "Charter": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
        }
    )
    await schema_manager.add_table(schools_table)
    
    # Add satscores table
    satscores_table = TableSchema(
        name="satscores",
        columns={
            "cds": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=False),
            "sname": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "AvgScrMath": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
            "AvgScrRead": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
        }
    )
    await schema_manager.add_table(satscores_table)
    
    tools = SQLGeneratorTools(memory, logger)
    
    print("\n" + "="*60)
    print("Testing Schema Inspection Tools")
    print("="*60)
    
    # Test list_all_tables
    print("\n--- All Tables ---")
    tables = await tools.list_all_tables()
    print(f"Found {tables['count']} tables:")
    for table in tables['tables']:
        print(f"  - {table['name']} ({table['column_count']} columns)")
    
    # Test check_table_columns
    print("\n--- Table: schools ---")
    schools_info = await tools.check_table_columns("schools")
    if schools_info['exists']:
        print(f"Columns ({schools_info['column_count']}):")
        for col in schools_info['columns'][:5]:  # First 5 columns
            print(f"  - {col['name']} ({col['type']})")
    
    # Test check_column_exists
    print("\n--- Column Check ---")
    result = await tools.check_column_exists("schools", "County")
    print(f"schools.County exists: {result['exists']}")
    
    result = await tools.check_column_exists("schools", "InvalidColumn")
    print(f"schools.InvalidColumn exists: {result['exists']}")
    if 'similar_columns' in result and result['similar_columns']:
        print(f"Similar columns: {result['similar_columns']}")


async def main():
    """Run all tests"""
    await test_execute_sql()
    await test_schema_tools()
    
    print("\n✅ All tests completed!")


if __name__ == "__main__":
    asyncio.run(main())