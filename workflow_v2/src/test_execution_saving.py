"""
Test to verify that execute_sql saves results to shared memory properly.
"""

import asyncio
import logging
from keyvalue_memory import KeyValueMemory
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from task_context_manager import TaskContextManager
from sql_generator_tools import SQLGeneratorTools
from memory_content_types import TableSchema, ColumnInfo

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_execution_result_saving():
    """Test that execute_sql saves successful results to shared memory"""
    print("\n" + "="*60)
    print("Testing SQL Execution Result Saving to Shared Memory")
    print("="*60)
    
    memory = KeyValueMemory()
    
    # Setup task context
    task_manager = TaskContextManager(memory)
    await task_manager.initialize(
        task_id="save_test",
        original_query="Test SQL result saving",
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
        await memory.set("schema_summary", schema_summary)
    
    # Setup query tree with a current node
    tree_manager = QueryTreeManager(memory)
    root_id = await tree_manager.initialize("Test query for result saving")
    await tree_manager.set_current_node_id(root_id)
    
    print(f"Created query tree with root node: {root_id}")
    
    # Create tools instance
    tools = SQLGeneratorTools(memory, logger)
    
    # Test SQL that should succeed
    test_sql = "SELECT School, County FROM schools WHERE County = 'Alameda' LIMIT 3"
    
    print(f"\nExecuting SQL: {test_sql}")
    result = await tools.execute_sql(test_sql)
    
    print(f"\nExecution result:")
    print(f"Status: {result['status']}")
    print(f"Row count: {result['row_count']}")
    print(f"Columns: {result['columns']}")
    
    # Check if results were saved to shared memory
    print(f"\n--- Checking Shared Memory ---")
    
    # Get the raw tree data to see what was actually stored
    tree = await tree_manager.get_tree()
    if tree and "nodes" in tree and root_id in tree["nodes"]:
        node_data = tree["nodes"][root_id]
        print(f"Raw node data keys: {list(node_data.keys())}")
        
        # Check for executionResult in raw data
        if "executionResult" in node_data:
            exec_result = node_data["executionResult"]
            print(f"✅ Execution result saved in raw data!")
            print(f"  Row count: {exec_result.get('rowCount', 'not set')}")
            print(f"  Error: {exec_result.get('error', 'not set')}")
            print(f"  Data rows: {len(exec_result.get('data', [])) if exec_result.get('data') else 0}")
            
            if exec_result.get('data'):
                print(f"  First row: {exec_result['data'][0]}")
        else:
            print(f"❌ No executionResult found in raw node data")
        
        # Check SQL field
        if "sql" in node_data:
            print(f"✅ SQL saved: {node_data['sql']}")
        else:
            print(f"❌ No SQL found in node data")
            
        # Check status
        print(f"Node status: {node_data.get('status', 'not set')}")
        
    # Also check the QueryNode object
    node = await tree_manager.get_node(root_id)
    if node:
        print(f"\nQueryNode object:")
        print(f"  Node ID: {node.nodeId}")
        print(f"  Status: {node.status}")
        print(f"  Generation: {node.generation}")
    else:
        print(f"❌ Could not retrieve QueryNode {root_id}")
    
    # Test with a failing SQL
    print(f"\n--- Testing Failed SQL ---")
    bad_sql = "SELECT invalid_column FROM nonexistent_table"
    result = await tools.execute_sql(bad_sql)
    
    print(f"Failed SQL result:")
    print(f"Status: {result['status']}")
    print(f"Error: {result.get('error', 'None')}")
    
    # Check that failed SQL doesn't overwrite good results
    node = await tree_manager.get_node(root_id)
    if node and hasattr(node, 'executionResult') and node.executionResult:
        print(f"✅ Previous successful result preserved after failed execution")
    

async def main():
    await test_execution_result_saving()
    print("\n✅ Test completed!")


if __name__ == "__main__":
    asyncio.run(main())