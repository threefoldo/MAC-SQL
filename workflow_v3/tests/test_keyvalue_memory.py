"""
Test suite for KeyValueMemory class.

This module tests the functionality of the KeyValueMemory class for the text-to-SQL workflow.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import autogen_core components
try:
    from autogen_core import CancellationToken
    from autogen_core.memory import Memory, MemoryContent, MemoryQueryResult, MemoryMimeType
except ImportError:
    print("Error: autogen_core is not installed. Please install it with: pip install autogen-core")
    sys.exit(1)

# Import our KeyValueMemory class
from keyvalue_memory import KeyValueMemory


class TestKeyValueMemory:
    """Test cases for KeyValueMemory class."""
    
    async def test_basic_operations(self):
        """Test basic operations of the KeyValueMemory class."""
        print("\n" + "="*60)
        print("Testing Basic Operations")
        print("="*60)
        
        # Initialize memory
        memory = KeyValueMemory(name="test_memory")
        print(f"KeyValueMemory initialized: {memory}, name: {memory.name}")
        
        # Test 1: Set and get a string variable
        print("\n--- Test 1: Set and get a string variable ---")
        await memory.set("greeting", "Hello, world!")
        greeting = await memory.get("greeting")
        print(f"Set 'greeting' to 'Hello, world!'")
        print(f"Retrieved 'greeting': {greeting}")
        assert greeting == "Hello, world!", "String retrieval failed"
        
        # Test 2: Set and get a JSON variable
        print("\n--- Test 2: Set and get a JSON variable ---")
        user_data = {
            "name": "Alice",
            "role": "Data Scientist",
            "skills": ["SQL", "Python", "Machine Learning"]
        }
        await memory.set("user_data", user_data)
        retrieved_user_data = await memory.get("user_data")
        print(f"Set 'user_data' to: {json.dumps(user_data, indent=2)}")
        print(f"Retrieved 'user_data': {json.dumps(retrieved_user_data, indent=2)}")
        assert retrieved_user_data == user_data, "JSON retrieval failed"
        
        # Test 3: Update an existing variable
        print("\n--- Test 3: Update an existing variable ---")
        await memory.set("greeting", "Hello, updated world!")
        updated_greeting = await memory.get("greeting")
        print(f"Updated 'greeting' to 'Hello, updated world!'")
        print(f"Retrieved updated 'greeting': {updated_greeting}")
        assert updated_greeting == "Hello, updated world!", "Variable update failed"
        
        # Test 4: Get variable with details
        print("\n--- Test 4: Get variable with details ---")
        greeting_details = await memory.get_with_details("greeting")
        print(f"Variable details:")
        print(f"  Content: {greeting_details.content}")
        print(f"  Mime Type: {greeting_details.mime_type}")
        print(f"  Metadata: {greeting_details.metadata}")
        assert greeting_details.content == "Hello, updated world!", "Details retrieval failed"
        assert greeting_details.metadata.get("variable_name") == "greeting", "Metadata incorrect"
        
        # Test 5: Set variable with custom metadata
        print("\n--- Test 5: Set variable with custom metadata ---")
        await memory.set(
            "sql_query", 
            "SELECT * FROM users WHERE role = 'admin'",
            metadata={"created_by": "test_notebook", "priority": "high"}
        )
        sql_query_details = await memory.get_with_details("sql_query")
        print(f"SQL Query with custom metadata:")
        print(f"  Content: {sql_query_details.content}")
        print(f"  Metadata: {sql_query_details.metadata}")
        assert sql_query_details.metadata.get("created_by") == "test_notebook", "Custom metadata failed"
        assert sql_query_details.metadata.get("priority") == "high", "Custom metadata failed"
        
        # Test 6: Query for content by metadata
        print("\n--- Test 6: Query using MemoryContent ---")
        query_content = MemoryContent(
            content="",
            mime_type=MemoryMimeType.TEXT,
            metadata={"created_by": "test_notebook"}
        )
        result = await memory.query(query_content)
        print(f"Query by metadata 'created_by': Found {len(result.results)} results")
        found_sql_query = False
        for idx, item in enumerate(result.results):
            content_preview = str(item.content)[:50] + '...' if len(str(item.content)) > 50 else str(item.content)
            print(f"  Result {idx+1}: {content_preview} (metadata: {item.metadata})")
            if item.metadata.get("variable_name") == "sql_query":
                found_sql_query = True
        assert found_sql_query, "Query by metadata failed to find sql_query"
        
        # Test 7: Clear the memory
        print("\n--- Test 7: Clear the memory ---")
        await memory.clear()
        cleared_greeting = await memory.get("greeting")
        print(f"After clearing, 'greeting' value: {cleared_greeting}")
        assert cleared_greeting is None, "Memory clear failed"
        
        # Test 8: Add binary data
        print("\n--- Test 8: Add binary data ---")
        binary_data = b"\x00\x01\x02\x03\x04\x05"
        await memory.set("binary_example", binary_data)
        retrieved_binary = await memory.get("binary_example")
        print(f"Set binary data: {binary_data}")
        print(f"Retrieved binary data: {retrieved_binary}")
        binary_details = await memory.get_with_details("binary_example")
        print(f"Binary mime type: {binary_details.mime_type}")
        assert retrieved_binary == binary_data, "Binary data retrieval failed"
        assert binary_details.mime_type == MemoryMimeType.BINARY, "Binary mime type incorrect"
        
        # Test 9: Test with multiple values in store
        print("\n--- Test 9: Multiple values in store ---")
        await memory.set("var1", "Value 1")
        await memory.set("var2", "Value 2")
        await memory.set("var3", "Value 3")
        await memory.set("var1", "Updated Value 1")
        
        latest_var1 = await memory.get("var1")
        print(f"Latest value of var1: {latest_var1}")
        assert latest_var1 == "Updated Value 1", "Variable override failed"
        
        # Test 10: Cancellation token
        print("\n--- Test 10: Cancellation token ---")
        token = CancellationToken()
        token.cancel()
        
        # Set a value normally first
        await memory.set("cancelled_var", "This value is set normally")
        
        # Try to query with cancelled token
        result = await memory.query("cancelled_var", token)
        print(f"Query with cancelled token returned {len(result.results)} results (should be 0 due to cancellation)")
        assert len(result.results) == 0, "Cancelled query should return no results"
        
        # Verify the value exists when queried normally
        normal_result = await memory.get("cancelled_var")
        print(f"Normal query returned: {normal_result}")
        assert normal_result == "This value is set normally", "Normal query should work"
        
        print("\n✅ All basic tests passed!")
    
    async def test_advanced_use_cases(self):
        """Test advanced use cases for the KeyValueMemory class."""
        print("\n" + "="*60)
        print("Testing Advanced Use Cases")
        print("="*60)
        
        memory = KeyValueMemory(name="test_memory_advanced")
        await memory.clear()
        
        # Test 1: Simulate storing and retrieving SQL workflow state
        print("\n--- Advanced Test 1: SQL workflow state ---")
        
        # Store database connection info
        db_config = {
            "db_id": "spider_dev",
            "connection_string": "sqlite:///path/to/database.db",
            "timeout": 30
        }
        await memory.set("db_config", db_config)
        
        # Store schema information
        schema_info = """<database_schema>
<table name="users">
    <column name="id" type="INTEGER" primary_key="true" />
    <column name="username" type="TEXT" />
    <column name="email" type="TEXT" />
</table>
<table name="products">
    <column name="id" type="INTEGER" primary_key="true" />
    <column name="name" type="TEXT" />
    <column name="price" type="REAL" />
</table>
</database_schema>"""
        await memory.set("schema_info", schema_info)
        
        # Store user query
        await memory.set("user_query", "Show me all users who have purchased products over $100")
        
        # Store generated SQL
        sql = """
SELECT users.username, users.email, products.name, products.price
FROM users
JOIN orders ON users.id = orders.user_id
JOIN products ON orders.product_id = products.id
WHERE products.price > 100
ORDER BY products.price DESC
"""
        await memory.set("generated_sql", sql)
        
        # Retrieve workflow state
        retrieved_query = await memory.get("user_query")
        retrieved_sql = await memory.get("generated_sql")
        retrieved_schema = await memory.get("schema_info")
        
        print(f"User Query: {retrieved_query}")
        print(f"Generated SQL: {retrieved_sql}")
        print(f"Schema Preview: {retrieved_schema[:60]}...")
        
        assert retrieved_query == "Show me all users who have purchased products over $100"
        assert "SELECT users.username" in retrieved_sql
        assert "<database_schema>" in retrieved_schema
        
        # Test 2: Storing execution history with structured metadata
        print("\n--- Advanced Test 2: Execution history ---")
        
        # Add execution results with timestamp metadata
        for i in range(3):
            execution_result = {
                "status": "success" if i != 1 else "error",
                "records": i * 5,
                "execution_time": 0.5 + (i * 0.2)
            }
            
            if i == 1:
                execution_result["error"] = "Column 'order_date' not found"
                
            await memory.set(
                f"execution_result_{i}", 
                execution_result,
                metadata={
                    "timestamp": f"2023-05-20T14:{i}0:00Z",
                    "execution_id": f"exec_{i}",
                    "status": execution_result["status"]
                }
            )
        
        # Query for error executions
        error_query = MemoryContent(
            content="",
            mime_type=MemoryMimeType.JSON,
            metadata={"status": "error"}
        )
        error_results = await memory.query(error_query)
        
        print(f"Found {len(error_results.results)} result(s) with error status")
        error_count = 0
        for idx, result in enumerate(error_results.results):
            if result.metadata.get("variable_name", "").startswith("execution_result_"):
                content = json.loads(result.content) if isinstance(result.content, str) else result.content
                if content.get("status") == "error":
                    error_count += 1
                    print(f"  Error execution: {json.dumps(content, indent=2)}")
                    print(f"  Metadata: {result.metadata}")
        
        assert error_count >= 1, "Should find at least one error execution"
        
        # Test 3: Simulating variable overrides with history
        print("\n--- Advanced Test 3: Variable overrides with history ---")
        
        # Store a sequence of SQL refinements
        sql_versions = [
            "SELECT * FROM users",
            "SELECT id, username FROM users WHERE active = true",
            "SELECT id, username, email FROM users WHERE active = true ORDER BY username"
        ]
        
        for i, sql_version in enumerate(sql_versions):
            await memory.set(
                "current_sql",
                sql_version,
                metadata={
                    "version": i + 1,
                    "timestamp": f"2023-05-20T15:{i}0:00Z",
                    "refinement_reason": ["Initial query", "Added filters", "Improved sorting"][i]
                }
            )
        
        # Get the current SQL (should be the latest version)
        current_sql = await memory.get("current_sql")
        current_sql_details = await memory.get_with_details("current_sql")
        
        print(f"Current SQL: {current_sql}")
        print(f"Version: {current_sql_details.metadata.get('version')}")
        print(f"Refinement reason: {current_sql_details.metadata.get('refinement_reason')}")
        
        assert current_sql == sql_versions[-1], "Should get the latest SQL version"
        assert current_sql_details.metadata.get('version') == 3, "Should be version 3"
        assert current_sql_details.metadata.get('refinement_reason') == "Improved sorting"
        
        # Test 4: Store debug information
        print("\n--- Advanced Test 4: Debug information ---")
        
        # Store debug logs in memory
        debug_logs = [
            "Started schema selection at 2023-05-20T15:00:00Z",
            "Found 5 tables in schema",
            "Selected 3 relevant tables for query",
            "Generated SQL with JOIN between users and orders",
            "Execution completed in 1.2 seconds"
        ]
        
        components = ["SchemaSelector", "SchemaSelector", "SchemaSelector", "SQLGenerator", "SQLExecutor"]
        
        for i, log in enumerate(debug_logs):
            await memory.set(
                f"debug_log_{i}",
                log,
                metadata={
                    "log_level": "DEBUG",
                    "component": components[i]
                }
            )
        
        # Query for logs from the SQLGenerator component
        sql_generator_query = MemoryContent(
            content="",
            mime_type=MemoryMimeType.TEXT,
            metadata={"component": "SQLGenerator"}
        )
        sql_generator_logs = await memory.query(sql_generator_query)
        
        print(f"\nSQLGenerator logs:")
        sql_gen_count = 0
        for log in sql_generator_logs.results:
            if log.metadata.get("component") == "SQLGenerator" and \
               log.metadata.get("variable_name", "").startswith("debug_log_"):
                sql_gen_count += 1
                print(f"  {log.content}")
        
        assert sql_gen_count >= 1, "Should find at least one SQLGenerator log"
        
        print("\n✅ All advanced tests passed!")


async def main():
    """Run all tests."""
    tester = TestKeyValueMemory()
    
    # Run basic tests
    await tester.test_basic_operations()
    
    # Run advanced tests
    await tester.test_advanced_use_cases()
    
    print("\n" + "="*60)
    print("✅ ALL TESTS PASSED!")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())