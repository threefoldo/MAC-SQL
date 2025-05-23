"""
Layer 2: Test KeyValueMemory Implementation
"""

import asyncio
import pytest
from datetime import datetime
from typing import Dict, Any, List

from memory import KeyValueMemory
from autogen_core.memory import MemoryContent, MemoryMimeType


class TestKeyValueMemory:
    """Test KeyValueMemory storage and retrieval."""
    
    async def test_basic_operations(self):
        """Test basic set/get operations."""
        memory = KeyValueMemory()
        
        # Test string storage
        await memory.set("test_key", "test_value")
        value = await memory.get("test_key")
        assert value == "test_value"
        
        # Test dict storage
        test_dict = {"name": "John", "age": 30, "city": "New York"}
        await memory.set("user_data", test_dict)
        retrieved = await memory.get("user_data")
        assert retrieved == test_dict
        
        # Test list storage
        test_list = [1, 2, 3, "four", {"five": 5}]
        await memory.set("mixed_list", test_list)
        retrieved = await memory.get("mixed_list")
        assert retrieved == test_list
        
        # Test overwrite
        await memory.set("test_key", "new_value")
        value = await memory.get("test_key")
        assert value == "new_value"
        
        print("✅ Basic operations tests passed")
    
    async def test_complex_nested_data(self):
        """Test storing complex nested structures."""
        memory = KeyValueMemory()
        
        # Complex nested structure
        complex_data = {
            "taskContext": {
                "taskId": "task_123",
                "status": "processing",
                "metadata": {
                    "created": datetime.now().isoformat(),
                    "tags": ["important", "urgent"]
                }
            },
            "queryTree": {
                "rootId": "node_root",
                "nodes": {
                    "node_root": {
                        "intent": "Main query",
                        "childIds": ["node_1", "node_2"]
                    },
                    "node_1": {
                        "intent": "Subquery 1",
                        "mapping": {
                            "tables": ["customers", "orders"],
                            "columns": ["id", "name", "total"]
                        }
                    }
                }
            },
            "statistics": {
                "nodeCount": 3,
                "executedCount": 1,
                "avgExecutionTime": 0.5
            }
        }
        
        await memory.set("workflow_state", complex_data)
        retrieved = await memory.get("workflow_state")
        
        # Verify nested access
        assert retrieved["taskContext"]["taskId"] == "task_123"
        assert retrieved["queryTree"]["nodes"]["node_1"]["mapping"]["tables"][0] == "customers"
        assert retrieved["statistics"]["avgExecutionTime"] == 0.5
        
        print("✅ Complex nested data tests passed")
    
    async def test_mime_type_detection(self):
        """Test automatic MIME type detection."""
        memory = KeyValueMemory()
        
        # String -> TEXT
        await memory.set("text_data", "Hello, world!")
        content = await memory.get_with_details("text_data")
        assert content.mime_type == MemoryMimeType.TEXT
        
        # Dict -> JSON
        await memory.set("json_data", {"key": "value"})
        content = await memory.get_with_details("json_data")
        assert content.mime_type == MemoryMimeType.JSON
        
        # Numbers -> JSON
        await memory.set("number_data", 42)
        content = await memory.get_with_details("number_data")
        assert content.mime_type == MemoryMimeType.JSON
        
        # Bytes -> BINARY
        await memory.set("binary_data", b"binary content")
        content = await memory.get_with_details("binary_data")
        assert content.mime_type == MemoryMimeType.BINARY
        
        print("✅ MIME type detection tests passed")
    
    async def test_metadata_handling(self):
        """Test metadata storage with content."""
        memory = KeyValueMemory()
        
        # Set with metadata
        metadata = {
            "created_by": "test_user",
            "version": "1.0",
            "tags": ["test", "important"]
        }
        
        await memory.set("data_with_meta", "content", metadata=metadata)
        
        # Get with details
        content = await memory.get_with_details("data_with_meta")
        assert content.metadata["variable_name"] == "data_with_meta"
        assert content.metadata["created_by"] == "test_user"
        assert content.metadata["version"] == "1.0"
        assert "test" in content.metadata["tags"]
        
        print("✅ Metadata handling tests passed")
    
    async def test_query_operations(self):
        """Test query functionality."""
        memory = KeyValueMemory()
        
        # Add multiple items
        await memory.set("user_1", {"name": "Alice", "age": 25})
        await memory.set("user_2", {"name": "Bob", "age": 30})
        await memory.set("config", {"theme": "dark", "lang": "en"})
        
        # Query by key
        result = await memory.query("user_1")
        assert len(result.results) == 1
        assert result.results[0].content["name"] == "Alice"
        
        # Query with MemoryContent (metadata match)
        query_content = MemoryContent(
            content="",
            mime_type=MemoryMimeType.JSON,
            metadata={"variable_name": "user_2"}
        )
        result = await memory.query(query_content)
        assert len(result.results) > 0
        
        print("✅ Query operations tests passed")
    
    async def test_clear_operation(self):
        """Test memory clearing."""
        memory = KeyValueMemory()
        
        # Add data
        await memory.set("key1", "value1")
        await memory.set("key2", "value2")
        await memory.set("key3", {"data": "value3"})
        
        # Verify data exists
        assert await memory.get("key1") == "value1"
        
        # Clear memory
        await memory.clear()
        
        # Verify data is gone
        assert await memory.get("key1") is None
        assert await memory.get("key2") is None
        assert await memory.get("key3") is None
        
        print("✅ Clear operation tests passed")
    
    async def test_missing_keys(self):
        """Test behavior with missing keys."""
        memory = KeyValueMemory()
        
        # Get non-existent key
        value = await memory.get("non_existent")
        assert value is None
        
        # Get with details for non-existent key
        content = await memory.get_with_details("non_existent")
        assert content is None
        
        # Query non-existent key
        result = await memory.query("non_existent")
        assert len(result.results) == 0
        
        print("✅ Missing keys tests passed")
    
    async def test_large_data_storage(self):
        """Test storing large data structures."""
        memory = KeyValueMemory()
        
        # Create large dataset
        large_data = {
            f"table_{i}": {
                "columns": [f"col_{j}" for j in range(50)],
                "data": [{"row": k, "values": list(range(50))} for k in range(100)]
            }
            for i in range(10)
        }
        
        # Store and retrieve
        await memory.set("large_dataset", large_data)
        retrieved = await memory.get("large_dataset")
        
        # Verify structure
        assert len(retrieved) == 10
        assert len(retrieved["table_0"]["columns"]) == 50
        assert len(retrieved["table_5"]["data"]) == 100
        
        print("✅ Large data storage tests passed")
    
    async def test_concurrent_operations(self):
        """Test concurrent memory operations."""
        memory = KeyValueMemory()
        
        # Define concurrent tasks
        async def write_task(key: str, value: Any):
            await memory.set(key, value)
            return await memory.get(key)
        
        # Run concurrent writes
        tasks = [
            write_task(f"concurrent_{i}", {"value": i, "data": f"test_{i}"})
            for i in range(20)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Verify all writes succeeded
        for i, result in enumerate(results):
            assert result["value"] == i
            assert result["data"] == f"test_{i}"
        
        print("✅ Concurrent operations tests passed")
    
    async def test_update_patterns(self):
        """Test common update patterns."""
        memory = KeyValueMemory()
        
        # Initialize data
        await memory.set("counter", 0)
        await memory.set("list_data", [])
        await memory.set("dict_data", {})
        
        # Update counter
        counter = await memory.get("counter")
        await memory.set("counter", counter + 1)
        assert await memory.get("counter") == 1
        
        # Append to list
        list_data = await memory.get("list_data")
        list_data.append("item1")
        await memory.set("list_data", list_data)
        assert len(await memory.get("list_data")) == 1
        
        # Update dict
        dict_data = await memory.get("dict_data")
        dict_data["new_key"] = "new_value"
        await memory.set("dict_data", dict_data)
        retrieved = await memory.get("dict_data")
        assert retrieved["new_key"] == "new_value"
        
        print("✅ Update patterns tests passed")


async def run_all_tests():
    """Run all KeyValueMemory tests."""
    print("="*60)
    print("LAYER 2: KEYVALUEMEMORY TESTING")
    print("="*60)
    
    tester = TestKeyValueMemory()
    
    tests = [
        tester.test_basic_operations,
        tester.test_complex_nested_data,
        tester.test_mime_type_detection,
        tester.test_metadata_handling,
        tester.test_query_operations,
        tester.test_clear_operation,
        tester.test_missing_keys,
        tester.test_large_data_storage,
        tester.test_concurrent_operations,
        tester.test_update_patterns
    ]
    
    for test in tests:
        try:
            await test()
        except Exception as e:
            print(f"❌ {test.__name__} failed: {str(e)}")
            raise
    
    print("\n✅ All Layer 2 tests passed!")


if __name__ == "__main__":
    asyncio.run(run_all_tests())