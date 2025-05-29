"""
Test cases for NodeHistoryManager - TESTING_PLAN.md Layer 1.4.

Tests node history storage and tracking with QueryNode structure consistency:
- Store history at correct memory location ('nodeHistory')
- Track node operations with QueryNode structure
- Record different operation types
- Support advanced history analysis
- Essential information filtering
"""

import asyncio
import pytest
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from keyvalue_memory import KeyValueMemory
from node_history_manager import NodeHistoryManager
from memory_content_types import NodeOperation, NodeOperationType, QueryNode, NodeStatus, ExecutionResult


class TestNodeHistoryManager:
    """Test node history management with QueryNode structure consistency."""
    
    @pytest.fixture
    async def memory(self):
        """Create a fresh KeyValueMemory instance."""
        mem = KeyValueMemory()
        yield mem
        await mem.clear()
    
    @pytest.fixture
    async def manager(self, memory):
        """Create a NodeHistoryManager instance."""
        mgr = NodeHistoryManager(memory)
        await mgr.initialize()
        return mgr
    
    @pytest.fixture
    def sample_node(self):
        """Create a sample QueryNode for testing."""
        return QueryNode(
            nodeId="test_node_001",
            intent="Find all schools in California",
            status=NodeStatus.CREATED,
            evidence="Focus on California schools",
            schema_linking={
                "selected_tables": ["schools"],
                "column_mapping": {"school_name": "sname", "location": "city"},
                "foreign_keys": []
            },
            generation={
                "sql": "SELECT sname, city FROM schools WHERE state = 'CA'",
                "sql_type": "SELECT",
                "confidence": 0.92,
                "verbose_explanation": "This explanation should be filtered out"
            },
            evaluation={
                "execution_result": {
                    "data": [{"sname": f"School {i}", "city": f"City {i}"} for i in range(10)],
                    "rowCount": 10
                },
                "success": True,
                "quality_score": 0.95,
                "detailed_analysis": "This detailed analysis should be filtered out"
            }
        )
    
    @pytest.mark.asyncio
    async def test_memory_location_and_initialization(self, memory, manager):
        """Test history storage at correct memory location."""
        # CRITICAL: Verify storage at correct memory location
        raw_data = await memory.get("nodeHistory")  # Must be exact key
        assert raw_data is not None
        assert isinstance(raw_data, list)
        assert len(raw_data) == 0  # Should start empty
        
        print("✅ Node history memory location and initialization tests passed")
    
    @pytest.mark.asyncio
    async def test_record_create_operation(self, memory, manager, sample_node):
        """Test recording node creation with QueryNode structure and essential filtering."""
        await manager.record_create(sample_node)
        
        # Verify in memory
        raw_data = await memory.get("nodeHistory")
        assert len(raw_data) == 1
        
        create_op = raw_data[0]
        assert create_op["nodeId"] == "test_node_001"
        assert create_op["operation"] == NodeOperationType.CREATE.value
        assert create_op["data"]["intent"] == "Find all schools in California"
        assert create_op["data"]["status"] == NodeStatus.CREATED.value
        assert create_op["data"]["evidence"] == "Focus on California schools"
        
        # Verify essential information filtering - verbose content removed
        data_str = str(create_op["data"])
        assert "verbose_explanation" not in data_str  # Should be filtered out
        assert "detailed_analysis" not in data_str    # Should be filtered out
        
        # Verify schema linking preserved (essential parts only)
        assert "schema_linking" in create_op["data"]
        schema = create_op["data"]["schema_linking"]
        assert "selected_tables" in schema
        assert "column_mapping" in schema
        assert "foreign_keys" in schema
        
        # Verify generation data preserved (essential parts only) 
        assert "generation" in create_op["data"]
        generation = create_op["data"]["generation"]
        assert "sql" in generation
        assert "sql_type" in generation 
        assert "confidence" in generation
        assert "verbose_explanation" not in generation  # Filtered out
        
        # Verify evaluation data preserved with result limiting
        assert "evaluation" in create_op["data"]
        evaluation = create_op["data"]["evaluation"]
        assert "execution_result" in evaluation
        assert "success" in evaluation
        assert "quality_score" in evaluation
        assert "detailed_analysis" not in evaluation  # Filtered out
        
        # Check result limiting (10 rows -> 5 rows max)
        exec_result = evaluation["execution_result"]
        assert len(exec_result["data"]) == 5  # Limited from 10 to 5
        assert exec_result["rowCount"] == 10  # Original count preserved
        
        print("✅ Record create operation with essential filtering tests passed")
    
    @pytest.mark.asyncio
    async def test_record_generate_sql_operation(self, memory, manager, sample_node):
        """Test recording SQL generation with essential information."""
        await manager.record_create(sample_node)
        
        # Update node for SQL generation
        sample_node.status = NodeStatus.SQL_GENERATED
        sample_node.generation["sql"] = "SELECT sname FROM schools WHERE state = 'CA' LIMIT 10"
        sample_node.generation["confidence"] = 0.98
        
        await manager.record_generate_sql(sample_node)
        
        # Verify both operations stored
        raw_data = await memory.get("nodeHistory")
        assert len(raw_data) == 2
        
        sql_gen_op = raw_data[1]
        assert sql_gen_op["nodeId"] == "test_node_001"
        assert sql_gen_op["operation"] == NodeOperationType.GENERATE_SQL.value
        assert "LIMIT 10" in sql_gen_op["data"]["generation"]["sql"]
        assert sql_gen_op["data"]["generation"]["confidence"] == 0.98
        assert "verbose_explanation" not in str(sql_gen_op["data"])
        
        print("✅ Record generate SQL operation tests passed")
    
    @pytest.mark.asyncio
    async def test_record_execute_operation(self, memory, manager, sample_node):
        """Test recording execution with result limiting."""
        await manager.record_create(sample_node)
        
        # Update node for execution
        sample_node.status = NodeStatus.EXECUTED_SUCCESS
        large_result = {
            "data": [{"id": i, "name": f"Item {i}"} for i in range(12)],  # 12 rows
            "rowCount": 12
        }
        sample_node.evaluation["execution_result"] = large_result
        
        await manager.record_execute(sample_node)
        
        # Verify execution recorded with result limiting
        raw_data = await memory.get("nodeHistory")
        assert len(raw_data) == 2
        
        exec_op = raw_data[1]
        assert exec_op["nodeId"] == "test_node_001"
        assert exec_op["operation"] == NodeOperationType.EXECUTE.value
        
        # Check result limiting (should be max 5 rows)
        exec_result = exec_op["data"]["evaluation"]["execution_result"]
        assert len(exec_result["data"]) == 5  # Limited from 12 to 5
        assert exec_result["rowCount"] == 12  # Original count preserved
        
        print("✅ Record execute operation tests passed")
    
    @pytest.mark.asyncio
    async def test_record_execute_with_error(self, memory, manager, sample_node):
        """Test recording failed execution."""
        await manager.record_create(sample_node)
        
        # Update node for failed execution
        sample_node.status = NodeStatus.EXECUTED_FAILED
        sample_node.evaluation["execution_result"] = {"data": [], "rowCount": 0}
        
        await manager.record_execute(sample_node, error="Table not found")
        
        # Verify error recorded
        raw_data = await memory.get("nodeHistory")
        exec_op = raw_data[1]
        assert exec_op["data"]["error"] == "Table not found"
        
        print("✅ Record execute with error tests passed")
    
    @pytest.mark.asyncio
    async def test_record_revise_operation(self, memory, manager, sample_node):
        """Test recording node revision."""
        await manager.record_create(sample_node)
        
        # Update node for revision
        sample_node.intent = "Find all high-performing schools in California"
        sample_node.status = NodeStatus.REVISED
        
        await manager.record_revise(sample_node, reason="Improve query specificity")
        
        # Verify revision recorded
        raw_data = await memory.get("nodeHistory")
        assert len(raw_data) == 2
        
        revise_op = raw_data[1]
        assert revise_op["nodeId"] == "test_node_001"
        assert revise_op["operation"] == NodeOperationType.REVISE.value
        assert revise_op["data"]["intent"] == "Find all high-performing schools in California"
        assert revise_op["data"]["reason"] == "Improve query specificity"
        
        print("✅ Record revise operation tests passed")
    
    @pytest.mark.asyncio
    async def test_record_delete_operation(self, memory, manager, sample_node):
        """Test recording node deletion."""
        await manager.record_create(sample_node)
        await manager.record_delete(sample_node, reason="Query no longer needed")
        
        # Verify deletion recorded
        raw_data = await memory.get("nodeHistory")
        assert len(raw_data) == 2
        
        delete_op = raw_data[1]
        assert delete_op["nodeId"] == "test_node_001"
        assert delete_op["operation"] == NodeOperationType.DELETE.value
        assert delete_op["data"]["reason"] == "Query no longer needed"
        
        print("✅ Record delete operation tests passed")
    
    @pytest.mark.asyncio
    async def test_get_current_node_state(self, memory, manager, sample_node):
        """Test reconstructing current node state from history."""
        # Record initial creation
        await manager.record_create(sample_node)
        
        # Update and record SQL generation
        sample_node.status = NodeStatus.SQL_GENERATED
        sample_node.generation["sql"] = "SELECT * FROM schools WHERE state = 'CA'"
        await manager.record_generate_sql(sample_node)
        
        # Update and record execution
        sample_node.status = NodeStatus.EXECUTED_SUCCESS
        sample_node.evaluation["execution_result"] = {"data": [{"id": 1}], "rowCount": 1}
        await manager.record_execute(sample_node)
        
        # Get current state
        current_node = await manager.get_current_node_state("test_node_001")
        assert current_node is not None
        assert current_node.nodeId == "test_node_001"
        assert current_node.status == NodeStatus.EXECUTED_SUCCESS
        assert "SELECT * FROM schools WHERE state = 'CA'" in current_node.generation["sql"]
        
        print("✅ Get current node state tests passed")
    
    @pytest.mark.asyncio
    async def test_node_attempts_summary(self, memory, manager, sample_node):
        """Test getting complete node attempts summary."""
        # Create initial attempt
        await manager.record_create(sample_node)
        await manager.record_generate_sql(sample_node)
        await manager.record_execute(sample_node, error="Table not found")
        
        # Create revision (new attempt)
        sample_node.intent = "Corrected query"
        await manager.record_revise(sample_node, reason="Fix table name")
        sample_node.generation["sql"] = "SELECT * FROM correct_table"
        await manager.record_generate_sql(sample_node)
        sample_node.evaluation["execution_result"] = {"data": [{"id": 1}], "rowCount": 1}
        await manager.record_execute(sample_node)
        
        # Get attempts summary
        summary = await manager.get_node_attempts_summary("test_node_001")
        assert summary["node_id"] == "test_node_001"
        assert summary["total_attempts"] >= 2  # May have more due to how attempts are counted
        
        # Check that we have attempts
        attempts = summary["attempts"]
        assert len(attempts) >= 2
        
        # Check first attempt (failed)
        first_attempt = attempts[0]
        assert first_attempt["attempt_number"] == 1
        assert first_attempt["final_status"] == "executed_failed"
        assert first_attempt["execution_result"]["error"] == "Table not found"
        
        # Check final attempt (successful)
        final_attempt = attempts[-1]
        assert final_attempt["final_status"] == "executed_success"
        assert final_attempt["execution_result"]["success"] is True
        
        print("✅ Node attempts summary tests passed")
    
    @pytest.mark.asyncio
    async def test_sql_evolution_tracking(self, memory, manager, sample_node):
        """Test tracking SQL evolution through attempts."""
        await manager.record_create(sample_node)
        
        # First SQL generation - update the node first
        sample_node.generation["sql"] = "SELECT * FROM schools"
        sample_node.generation["confidence"] = 0.8
        await manager.record_generate_sql(sample_node)
        
        # Second SQL generation (after revision)
        await manager.record_revise(sample_node, reason="Optimize query")
        sample_node.generation["sql"] = "SELECT id, name FROM schools WHERE active = 1"
        sample_node.generation["confidence"] = 0.95
        await manager.record_generate_sql(sample_node)
        
        # Get SQL evolution
        sql_evolution = await manager.get_node_sql_evolution("test_node_001")
        assert len(sql_evolution) == 2
        
        # Check first generation - it should contain the SQL from the node at recording time
        first_sql = sql_evolution[0]
        assert first_sql["attempt"] == 1
        assert first_sql["sql"] != ""  # Should have some SQL
        assert first_sql["confidence"] == 0.8
        
        # Check second generation
        second_sql = sql_evolution[1]
        assert second_sql["attempt"] == 2
        assert "active = 1" in second_sql["sql"]
        assert second_sql["confidence"] == 0.95
        
        print("✅ SQL evolution tracking tests passed")
    
    @pytest.mark.asyncio
    async def test_execution_history(self, memory, manager, sample_node):
        """Test tracking execution history."""
        await manager.record_create(sample_node)
        
        # First execution (failed)
        await manager.record_execute(sample_node, error="Syntax error")
        
        # Second execution (successful)
        sample_node.evaluation["execution_result"] = {"data": [{"id": 1}], "rowCount": 1}
        await manager.record_execute(sample_node)
        
        # Get execution history
        exec_history = await manager.get_node_execution_history("test_node_001")
        assert len(exec_history) == 2
        
        # Check first execution (failed)
        first_exec = exec_history[0]
        assert first_exec["success"] is False
        assert first_exec["error"] == "Syntax error"
        
        # Check second execution (successful)
        second_exec = exec_history[1]
        assert second_exec["success"] is True
        assert second_exec["result"] is not None
        assert second_exec["result"]["rowCount"] == 1
        
        print("✅ Execution history tests passed")
    
    @pytest.mark.asyncio
    async def test_history_summary_statistics(self, memory, manager, sample_node):
        """Test comprehensive history summary statistics."""
        # Create multiple nodes with different outcomes
        nodes = []
        for i in range(3):
            node = QueryNode(
                nodeId=f"node_{i}",
                intent=f"Query {i}",
                status=NodeStatus.CREATED
            )
            nodes.append(node)
            await manager.record_create(node)
            await manager.record_generate_sql(node)
            
            # Mix of successful and failed executions
            if i % 2 == 0:
                await manager.record_execute(node)  # Success
            else:
                await manager.record_execute(node, error="Failed")  # Failure
        
        # Get history summary
        summary = await manager.get_history_summary()
        
        assert summary["total_operations"] == 9  # 3 nodes × 3 operations each
        assert summary["unique_nodes"] == 3
        assert summary["operation_counts"]["create"] == 3
        assert summary["operation_counts"]["generate_sql"] == 3
        assert summary["operation_counts"]["execute"] == 3
        
        exec_stats = summary["execution_stats"]
        assert exec_stats["total_executions"] == 3
        assert exec_stats["successful_executions"] == 2  # nodes 0 and 2
        assert exec_stats["failed_executions"] == 1    # node 1
        assert exec_stats["success_rate"] == 2/3  # 66.67%
        
        print("✅ History summary statistics tests passed")
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, memory, manager):
        """Test concurrent node operations and memory consistency."""
        # Define concurrent tasks
        async def process_node(node_id: str):
            node = QueryNode(
                nodeId=node_id,
                intent=f"Concurrent query {node_id}",
                status=NodeStatus.CREATED
            )
            await manager.record_create(node)
            
            node.status = NodeStatus.SQL_GENERATED
            node.generation = {"sql": f"SELECT * FROM table_{node_id}", "confidence": 0.8}
            await manager.record_generate_sql(node)
            
            node.status = NodeStatus.EXECUTED_SUCCESS
            node.evaluation = {"execution_result": {"data": [], "rowCount": 0}}
            await manager.record_execute(node)
        
        # Run concurrent operations
        tasks = [process_node(f"concurrent_{i}") for i in range(3)]
        await asyncio.gather(*tasks)
        
        # Verify all operations recorded
        all_operations = await manager.get_all_operations()
        assert len(all_operations) == 9  # 3 nodes × 3 operations each
        
        # Verify operations are properly distributed
        node_ops = {}
        for op in all_operations:
            node_id = op.nodeId
            if node_id not in node_ops:
                node_ops[node_id] = 0
            node_ops[node_id] += 1
        
        assert len(node_ops) == 3  # 3 different nodes
        for count in node_ops.values():
            assert count == 3  # Each node has 3 operations
        
        # Test node state reconstruction after concurrent operations
        for i in range(3):
            node_id = f"concurrent_{i}"
            current_state = await manager.get_current_node_state(node_id)
            assert current_state is not None
            assert current_state.nodeId == node_id
            assert current_state.status == NodeStatus.EXECUTED_SUCCESS
            assert f"table_{node_id}" in current_state.generation["sql"]
        
        print("✅ Concurrent operations and state reconstruction tests passed")
    
    @pytest.mark.asyncio
    async def test_essential_information_filtering(self, memory, manager):
        """Test that essential information filtering works correctly."""
        # Create a node with both essential and verbose information
        verbose_node = QueryNode(
            nodeId="verbose_test",
            intent="Test essential filtering",
            status=NodeStatus.CREATED,
            evidence="This is essential evidence",
            schema_linking={
                "selected_tables": ["users", "orders"],
                "column_mapping": {"user_id": "uid", "order_date": "odate"},
                "foreign_keys": [{"from": "orders.user_id", "to": "users.id"}],
                "verbose_explanation": "This long explanation should be filtered out from memory",
                "detailed_reasoning": "More verbose content that wastes memory"
            },
            generation={
                "sql": "SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id",
                "sql_type": "SELECT",
                "confidence": 0.92,
                "explanation": "This explanation should be removed to save memory",
                "step_by_step_reasoning": "Verbose reasoning that should be filtered",
                "considerations": "More verbose content"
            },
            evaluation={
                "execution_result": {
                    "data": [{"name": f"User {i}", "count": i+1} for i in range(8)],  # 8 rows
                    "rowCount": 8
                },
                "success": True,
                "quality_score": 0.95,
                "detailed_analysis": "Very long detailed analysis that should be removed",
                "performance_metrics": "More verbose content that wastes space",
                "suggestions": "Suggestions should be filtered out"
            }
        )
        
        # Record the node
        await manager.record_create(verbose_node)
        
        # Get the stored operation
        raw_data = await memory.get("nodeHistory")
        stored_op = raw_data[0]
        stored_data = stored_op["data"]
        
        # Verify essential information is preserved
        assert stored_data["nodeId"] == "verbose_test"
        assert stored_data["intent"] == "Test essential filtering"
        assert stored_data["evidence"] == "This is essential evidence"
        
        # Schema linking - essential parts preserved
        schema = stored_data["schema_linking"]
        assert "selected_tables" in schema
        assert "column_mapping" in schema
        assert "foreign_keys" in schema
        assert "verbose_explanation" not in schema  # Filtered out
        assert "detailed_reasoning" not in schema   # Filtered out
        
        # Generation - essential parts preserved
        generation = stored_data["generation"]
        assert "sql" in generation
        assert "sql_type" in generation
        assert "confidence" in generation
        assert "explanation" not in generation              # Filtered out
        assert "step_by_step_reasoning" not in generation   # Filtered out
        assert "considerations" not in generation           # Filtered out
        
        # Evaluation - essential parts preserved, results limited
        evaluation = stored_data["evaluation"]
        assert "execution_result" in evaluation
        assert "success" in evaluation
        assert "quality_score" in evaluation
        assert "detailed_analysis" not in evaluation    # Filtered out
        assert "performance_metrics" not in evaluation  # Filtered out
        assert "suggestions" not in evaluation          # Filtered out
        
        # Result limiting - 8 rows reduced to 5
        exec_result = evaluation["execution_result"]
        assert len(exec_result["data"]) == 5  # Limited from 8 to 5
        assert exec_result["rowCount"] == 8   # Original count preserved
        
        print("✅ Essential information filtering tests passed")
    
    @pytest.mark.asyncio
    async def test_complete_node_lifecycle_tracking(self, memory, manager):
        """Test complete node lifecycle with multiple revisions and error recovery."""
        # Initial node creation
        node = QueryNode(
            nodeId="lifecycle_test",
            intent="Find top performing students",
            status=NodeStatus.CREATED,
            evidence="Looking for students with high GPA"
        )
        await manager.record_create(node)
        
        # First SQL attempt (fails)
        node.status = NodeStatus.SQL_GENERATED
        node.generation = {
            "sql": "SELECT * FROM wrong_table WHERE gpa > 3.5",
            "sql_type": "SELECT",
            "confidence": 0.7
        }
        await manager.record_generate_sql(node)
        
        # First execution (fails)
        node.status = NodeStatus.EXECUTED_FAILED
        node.evaluation = {
            "execution_result": {"data": [], "rowCount": 0, "error": "Table not found"},
            "success": False,
            "error_type": "table_not_found"
        }
        await manager.record_execute(node, error="Table 'wrong_table' doesn't exist")
        
        # First revision
        node.intent = "Find top performing students from correct table"
        node.status = NodeStatus.REVISED
        await manager.record_revise(node, reason="Fix table name")
        
        # Second SQL attempt (better)
        node.status = NodeStatus.SQL_GENERATED
        node.generation = {
            "sql": "SELECT name, gpa FROM students WHERE gpa > 3.5",
            "sql_type": "SELECT",
            "confidence": 0.9
        }
        await manager.record_generate_sql(node)
        
        # Second execution (success)
        node.status = NodeStatus.EXECUTED_SUCCESS
        node.evaluation = {
            "execution_result": {
                "data": [{"name": f"Student {i}", "gpa": 3.6 + i*0.1} for i in range(3)],
                "rowCount": 3
            },
            "success": True,
            "quality_score": 0.95
        }
        await manager.record_execute(node)
        
        # Get complete lifecycle summary
        lifecycle = await manager.get_node_lifecycle("lifecycle_test")
        assert lifecycle["nodeId"] == "lifecycle_test"
        assert lifecycle["created"] is not None
        assert lifecycle["sql_generated"] is not None
        assert lifecycle["executed"] is not None
        assert lifecycle["revised_count"] == 1
        assert lifecycle["total_operations"] == 6
        
        # Get attempts summary
        attempts = await manager.get_node_attempts_summary("lifecycle_test")
        assert attempts["total_attempts"] >= 2
        assert attempts["final_status"] == "executed_success"
        
        # Verify first attempt failed
        first_attempt = attempts["attempts"][0]
        assert first_attempt["final_status"] == "executed_failed"
        assert "wrong_table" in first_attempt["sql_generated"]["sql"]
        
        # Verify final attempt succeeded
        final_attempt = attempts["attempts"][-1]
        assert final_attempt["final_status"] == "executed_success"
        assert "students" in final_attempt["sql_generated"]["sql"]
        
        # Get SQL evolution
        sql_evolution = await manager.get_node_sql_evolution("lifecycle_test")
        assert len(sql_evolution) == 2
        assert "wrong_table" in sql_evolution[0]["sql"]
        assert "students" in sql_evolution[1]["sql"]
        assert sql_evolution[1]["confidence"] > sql_evolution[0]["confidence"]
        
        print("✅ Complete node lifecycle tracking tests passed")


if __name__ == "__main__":
    asyncio.run(pytest.main([__file__, "-v", "-s"]))