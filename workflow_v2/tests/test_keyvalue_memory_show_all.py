"""
Test the show_all functionality of KeyValueMemory.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from keyvalue_memory import KeyValueMemory
from memory_content_types import TaskContext, TaskStatus, TableSchema, ColumnInfo
from datetime import datetime


async def test_show_all():
    """Test the show_all function with different formats"""
    memory = KeyValueMemory()
    
    # Add some test data
    await memory.set("user_query", "Find all schools in California")
    await memory.set("task_id", "test_123")
    await memory.set("row_count", 42)
    await memory.set("results", {"schools": ["School A", "School B", "School C"]})
    
    # Test data structure
    task_context = TaskContext(
        taskId="test_123",
        originalQuery="Find all schools in California",
        databaseName="california_schools",
        startTime=datetime.now().isoformat(),
        status=TaskStatus.PROCESSING
    )
    await memory.set("taskContext", task_context.to_dict())
    
    print("=== Testing show_all with 'summary' format ===")
    summary = await memory.show_all(format="summary")
    print(summary)
    print()
    
    print("=== Testing show_all with 'table' format ===")
    table = await memory.show_all(format="table")
    print(table)
    print()
    
    print("=== Testing show_all with 'detailed' format ===")
    detailed = await memory.show_all(format="detailed")
    print(detailed)
    print()
    
    print("=== Testing show_all with 'json' format ===")
    json_data = await memory.show_all(format="json")
    print(f"JSON keys: {list(json_data.keys())}")
    print(f"Sample data - user_query: {json_data['user_query']['value']}")
    print()
    
    print("=== Testing get_keys ===")
    keys = await memory.get_keys()
    print(f"All keys: {keys}")


if __name__ == "__main__":
    asyncio.run(test_show_all())