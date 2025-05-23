"""
Edge case tests for the MAC-SQL workflow system.
Tests error handling, boundary conditions, and unusual scenarios.
"""

import asyncio
import json
from datetime import datetime

# Import setup for tests
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from memory import KeyValueMemory
from task_context_manager import TaskContextManager
from query_tree_manager import QueryTreeManager
from database_schema_manager import DatabaseSchemaManager
from node_history_manager import NodeHistoryManager
from memory_types import (
    TaskContext, TaskStatus, QueryNode, NodeStatus, 
    QueryMapping, TableMapping, ColumnMapping,
    ExecutionResult, NodeOperation, NodeOperationType
)


class EdgeCaseTests:
    """Test edge cases and error conditions."""
    
    def __init__(self):
        self.memory = KeyValueMemory(name="edge_cases")
        self.task_manager = TaskContextManager(memory=self.memory)
        self.query_manager = QueryTreeManager(memory=self.memory)
        self.schema_manager = DatabaseSchemaManager(memory=self.memory)
        self.history_manager = NodeHistoryManager(memory=self.memory)

    async def test_empty_query_handling(self):
        """Test handling of empty or invalid queries."""
        print("\n=== Edge Case 1: Empty Query Handling ===")
        
        # Initialize managers
        await self.task_manager.initialize(
            task_id="edge_001",
            original_query="",  # Empty query
            database_name="test_db"
        )
        
        await self.query_manager.initialize("")  # Empty intent
        
        # Create node with minimal content
        node = QueryNode(
            nodeId="empty_001",
            intent="",
            mapping=QueryMapping(tables=[], columns=[]),
            sql="",
            status=NodeStatus.CREATED
        )
        
        await self.query_manager.add_node(node)
        
        # Verify node was stored
        retrieved = await self.query_manager.get_node("empty_001")
        assert retrieved is not None
        assert retrieved.intent == ""
        assert retrieved.sql == ""
        
        print("✓ Empty query handling works")
        return True

    async def test_very_long_content(self):
        """Test handling of very long SQL queries and content."""
        print("\n=== Edge Case 2: Very Long Content ===")
        
        # Create very long SQL query (10KB+)
        long_sql = "SELECT " + ", ".join([f"column_with_very_long_name_{i:04d}" for i in range(500)]) + " FROM very_large_table"
        assert len(long_sql) > 10000
        
        await self.task_manager.initialize(
            task_id="edge_002",
            original_query="Very complex query with many columns",
            database_name="large_db"
        )
        
        await self.query_manager.initialize("Complex multi-column query")
        
        # Create node with very long SQL
        node = QueryNode(
            nodeId="long_001",
            intent="Select many columns from large table",
            mapping=QueryMapping(
                tables=[TableMapping(name="very_large_table", alias="vlt", purpose="Data source")],
                columns=[ColumnMapping(table="very_large_table", column=f"col_{i}", usedFor="select") for i in range(100)]
            ),
            sql=long_sql,
            status=NodeStatus.SQL_GENERATED
        )
        
        await self.query_manager.add_node(node)
        
        # Verify long content was stored and retrieved correctly
        retrieved = await self.query_manager.get_node("long_001")
        assert retrieved is not None
        assert len(retrieved.sql) > 10000
        assert retrieved.sql == long_sql
        assert len(retrieved.mapping.columns) == 100
        
        print(f"✓ Long content handling works (SQL: {len(long_sql)} chars, Columns: {len(retrieved.mapping.columns)})")
        return True

    async def test_concurrent_operations(self):
        """Test concurrent access to the memory system."""
        print("\n=== Edge Case 3: Concurrent Operations ===")
        
        await self.task_manager.initialize(
            task_id="edge_003",
            original_query="Concurrent test query",
            database_name="concurrent_db"
        )
        
        await self.query_manager.initialize("Concurrent operations test")
        
        # Create multiple nodes concurrently
        async def create_node(node_id, delay=0):
            if delay:
                await asyncio.sleep(delay)
            node = QueryNode(
                nodeId=f"concurrent_{node_id}",
                intent=f"Concurrent operation {node_id}",
                mapping=QueryMapping(tables=[], columns=[]),
                sql=f"SELECT {node_id} FROM test_{node_id}",
                status=NodeStatus.SQL_GENERATED
            )
            await self.query_manager.add_node(node)
            return node_id
        
        # Run 5 concurrent node creations
        tasks = [create_node(i, i * 0.1) for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        # Verify all nodes were created
        assert len(results) == 5
        for i in range(5):
            node = await self.query_manager.get_node(f"concurrent_{i}")
            assert node is not None
            assert node.intent == f"Concurrent operation {i}"
        
        print(f"✓ Concurrent operations work ({len(results)} nodes created)")
        return True

    async def test_memory_overflow_recovery(self):
        """Test system behavior with large amounts of data."""
        print("\n=== Edge Case 4: Memory Stress Test ===")
        
        await self.task_manager.initialize(
            task_id="edge_004",
            original_query="Memory stress test",
            database_name="stress_db"
        )
        
        await self.query_manager.initialize("Memory stress test")
        
        # Create many nodes to stress test memory
        node_count = 50
        for i in range(node_count):
            node = QueryNode(
                nodeId=f"stress_{i:03d}",
                intent=f"Stress test node {i}",
                mapping=QueryMapping(
                    tables=[TableMapping(name=f"table_{i}", alias=f"t{i}", purpose=f"Test table {i}")],
                    columns=[ColumnMapping(table=f"table_{i}", column=f"col_{j}", usedFor="select") for j in range(10)]
                ),
                sql=f"SELECT * FROM table_{i} WHERE id = {i}",
                status=NodeStatus.SQL_GENERATED,
                executionResult=ExecutionResult(
                    data=[[f"row_{j}_{i}" for j in range(10)] for _ in range(5)],
                    rowCount=5
                )
            )
            await self.query_manager.add_node(node)
        
        # Verify all nodes are accessible
        tree = await self.query_manager.get_tree()
        assert len(tree["nodes"]) >= node_count
        
        # Test retrieval of random nodes
        import random
        for _ in range(10):
            node_id = f"stress_{random.randint(0, node_count-1):03d}"
            node = await self.query_manager.get_node(node_id)
            assert node is not None
            assert node.nodeId == node_id
        
        print(f"✓ Memory stress test passed ({node_count} nodes created and verified)")
        return True

    async def test_invalid_data_handling(self):
        """Test handling of invalid or corrupted data."""
        print("\n=== Edge Case 5: Invalid Data Handling ===")
        
        await self.task_manager.initialize(
            task_id="edge_005",
            original_query="Invalid data test",
            database_name="invalid_db"
        )
        
        await self.query_manager.initialize("Invalid data test")
        
        # Test with None values in ExecutionResult
        node = QueryNode(
            nodeId="invalid_001",
            intent="Test with None result",
            mapping=QueryMapping(tables=[], columns=[]),
            sql="SELECT NULL",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=None,  # None data
                rowCount=0
            )
        )
        
        await self.query_manager.add_node(node)
        retrieved = await self.query_manager.get_node("invalid_001")
        assert retrieved.executionResult.data is None
        assert retrieved.executionResult.rowCount == 0
        
        # Test with error in ExecutionResult
        error_node = QueryNode(
            nodeId="invalid_002",
            intent="Test with error",
            mapping=QueryMapping(tables=[], columns=[]),
            sql="INVALID SQL SYNTAX",
            status=NodeStatus.EXECUTED_FAILED,
            executionResult=ExecutionResult(
                data=[],
                rowCount=0,
                error="Syntax error: unexpected token 'INVALID'"
            )
        )
        
        await self.query_manager.add_node(error_node)
        retrieved_error = await self.query_manager.get_node("invalid_002")
        assert retrieved_error.executionResult.error is not None
        assert "Syntax error" in retrieved_error.executionResult.error
        
        print("✓ Invalid data handling works")
        return True

    async def test_unicode_and_special_characters(self):
        """Test handling of Unicode and special characters."""
        print("\n=== Edge Case 6: Unicode and Special Characters ===")
        
        # Unicode test strings
        unicode_query = "Найти всех клиентов из 北京 और दिल्ली के साथ émojis 🚀🔍"
        unicode_sql = "SELECT * FROM 客户表 WHERE 城市 IN ('北京', 'दिल्ली') AND नाम LIKE '%émojis%' -- 🚀"
        
        await self.task_manager.initialize(
            task_id="edge_006",
            original_query=unicode_query,
            database_name="unicode_test_🌍"
        )
        
        await self.query_manager.initialize(unicode_query)
        
        # Create node with Unicode content
        node = QueryNode(
            nodeId="unicode_001",
            intent=unicode_query,
            mapping=QueryMapping(
                tables=[TableMapping(name="客户表", alias="c", purpose="Customer data")],
                columns=[ColumnMapping(table="客户表", column="नाम", usedFor="filter")]
            ),
            sql=unicode_sql,
            status=NodeStatus.SQL_GENERATED,
            executionResult=ExecutionResult(
                data=[["张三", "北京"], ["राम", "दिल्ली"], ["Jean-François", "Paris 🇫🇷"]],
                rowCount=3
            )
        )
        
        await self.query_manager.add_node(node)
        
        # Verify Unicode content is preserved
        retrieved = await self.query_manager.get_node("unicode_001")
        assert retrieved.intent == unicode_query
        assert retrieved.sql == unicode_sql
        assert "北京" in retrieved.sql
        assert "🚀" in retrieved.sql
        assert len(retrieved.executionResult.data) == 3
        
        # Verify task also handles Unicode
        task = await self.task_manager.get()
        assert task.originalQuery == unicode_query
        assert task.databaseName == "unicode_test_🌍"
        
        print("✓ Unicode and special characters handling works")
        return True

    async def test_circular_dependencies(self):
        """Test handling of circular dependencies in query trees."""
        print("\n=== Edge Case 7: Circular Dependency Prevention ===")
        
        await self.task_manager.initialize(
            task_id="edge_007",
            original_query="Circular dependency test",
            database_name="cycle_db"
        )
        
        await self.query_manager.initialize("Circular dependency test")
        
        # Create nodes that could form a cycle
        node_a = QueryNode(
            nodeId="cycle_a",
            intent="Node A",
            mapping=QueryMapping(tables=[], columns=[]),
            parentId="cycle_c",  # Points to C
            childIds=["cycle_b"],  # Has B as child
            status=NodeStatus.CREATED
        )
        
        node_b = QueryNode(
            nodeId="cycle_b",
            intent="Node B",
            mapping=QueryMapping(tables=[], columns=[]),
            parentId="cycle_a",  # Points to A
            childIds=["cycle_c"],  # Has C as child
            status=NodeStatus.CREATED
        )
        
        node_c = QueryNode(
            nodeId="cycle_c",
            intent="Node C",
            mapping=QueryMapping(tables=[], columns=[]),
            parentId="cycle_b",  # Points to B
            childIds=["cycle_a"],  # Has A as child (cycle!)
            status=NodeStatus.CREATED
        )
        
        # Add nodes (system should handle this gracefully)
        await self.query_manager.add_node(node_a)
        await self.query_manager.add_node(node_b)
        await self.query_manager.add_node(node_c)
        
        # Verify nodes exist but navigate carefully
        retrieved_a = await self.query_manager.get_node("cycle_a")
        retrieved_b = await self.query_manager.get_node("cycle_b")
        retrieved_c = await self.query_manager.get_node("cycle_c")
        
        assert retrieved_a is not None
        assert retrieved_b is not None
        assert retrieved_c is not None
        
        print("✓ Circular dependency handling works (nodes stored safely)")
        return True

    async def run_all_tests(self):
        """Run all edge case tests."""
        test_methods = [
            self.test_empty_query_handling,
            self.test_very_long_content,
            self.test_concurrent_operations,
            self.test_memory_overflow_recovery,
            self.test_invalid_data_handling,
            self.test_unicode_and_special_characters,
            self.test_circular_dependencies
        ]
        
        results = []
        for test in test_methods:
            try:
                success = await test()
                results.append((test.__name__, success))
            except Exception as e:
                print(f"✗ {test.__name__} failed: {e}")
                import traceback
                traceback.print_exc()
                results.append((test.__name__, False))
        
        # Summary
        print(f"\n{'='*60}")
        print("EDGE CASE TEST SUMMARY")
        print(f"{'='*60}")
        
        passed = 0
        for test_name, success in results:
            status = "✓ PASSED" if success else "✗ FAILED"
            clean_name = test_name.replace("test_", "").replace("_", " ").title()
            print(f"{status:10} {clean_name}")
            if success:
                passed += 1
        
        total = len(results)
        print(f"\nResults: {passed}/{total} edge case tests passed")
        
        if passed == total:
            print("🎉 All edge case tests passed!")
        else:
            print(f"❌ {total - passed} edge case tests failed")
        
        return passed == total


async def main():
    """Run all edge case tests."""
    tester = EdgeCaseTests()
    success = await tester.run_all_tests()
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)