"""
Tests for TaskContextManager - Layer 1.1 Memory Manager Testing.

Based on TESTING_PLAN.md requirements, this module tests:
1. Initialize task with all required fields (taskId, originalQuery, databaseName, startTime, status, evidence)
2. Update task status correctly (INITIALIZING → PROCESSING → COMPLETED/FAILED)
3. Store/retrieve task context from correct memory location ('task_context' key)
4. Handle missing or invalid task data gracefully
5. Verify data persistence across operations
6. TaskStatus enum conversions work correctly
7. Optional evidence field handled properly
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from task_context_manager import TaskContextManager
from keyvalue_memory import KeyValueMemory
from memory_content_types import TaskContext, TaskStatus


@pytest.mark.asyncio
class TestTaskContextManager:
    """Test TaskContextManager functionality."""
    
    @pytest.fixture
    async def memory(self):
        """Create a mock KeyValueMemory instance."""
        mock_memory = Mock(spec=KeyValueMemory)
        mock_memory.get = AsyncMock()
        mock_memory.set = AsyncMock()
        return mock_memory
    
    @pytest.fixture
    async def manager(self, memory):
        """Create a TaskContextManager instance with mock memory."""
        return TaskContextManager(memory)
    
    async def test_initialize_with_all_required_fields(self, manager, memory):
        """Test initializing a new task context with all required fields."""
        # Test data
        task_id = "test-task-123"
        query = "SELECT * FROM users"
        db_name = "test_db"
        evidence = "Users are identified by user_id"
        
        # Initialize task context
        with patch('task_context_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2025-01-01T12:00:00"
            
            result = await manager.initialize(
                task_id=task_id,
                original_query=query,
                database_name=db_name,
                evidence=evidence
            )
        
        # Verify all required fields are present
        assert isinstance(result, TaskContext)
        assert result.taskId == task_id
        assert result.originalQuery == query
        assert result.databaseName == db_name
        assert result.status == TaskStatus.INITIALIZING
        assert result.evidence == evidence
        assert result.startTime == "2025-01-01T12:00:00"
        
        # CRITICAL: Verify memory location is exactly 'taskContext'
        memory.set.assert_called_once()
        call_args = memory.set.call_args[0]
        assert call_args[0] == "taskContext"  # Must be exact key per implementation
        
        # Verify complete data structure is stored
        stored_data = call_args[1]
        assert isinstance(stored_data, dict)
        assert stored_data["taskId"] == task_id
        assert stored_data["originalQuery"] == query
        assert stored_data["databaseName"] == db_name
        assert stored_data["status"] == "initializing"
        assert stored_data["evidence"] == evidence
        assert stored_data["startTime"] == "2025-01-01T12:00:00"
    
    async def test_initialize_without_evidence(self, manager, memory):
        """Test initializing without evidence - optional field handling."""
        result = await manager.initialize(
            task_id="test-123",
            original_query="SELECT count(*) FROM orders",
            database_name="sales_db"
        )
        
        # Verify optional evidence field is handled properly
        assert result.evidence is None
        
        # Verify stored at correct location
        memory.set.assert_called_once()
        call_args = memory.set.call_args[0]
        assert call_args[0] == "taskContext"
        
        # Verify evidence is None in stored data
        stored_data = call_args[1]
        assert stored_data["evidence"] is None
    
    async def test_get_existing_context(self, manager, memory):
        """Test retrieving task context from correct memory location."""
        # Mock existing context
        context_data = {
            'taskId': 'test-123',
            'originalQuery': 'SELECT * FROM products',
            'databaseName': 'inventory_db',
            'startTime': '2025-01-01T10:00:00',
            'status': 'processing',
            'evidence': 'Product IDs are unique'
        }
        memory.get.return_value = context_data
        
        # Get context
        result = await manager.get()
        
        # Verify correct retrieval from 'taskContext' location
        memory.get.assert_called_once_with("taskContext")
        
        # Verify TaskStatus enum conversion works correctly
        assert isinstance(result, TaskContext)
        assert result.taskId == 'test-123'
        assert result.status == TaskStatus.PROCESSING  # String -> Enum conversion
        assert result.evidence == 'Product IDs are unique'
    
    async def test_get_no_context(self, manager, memory):
        """Test getting context when none exists."""
        memory.get.return_value = None
        
        result = await manager.get()
        
        assert result is None
        memory.get.assert_called_once_with("taskContext")
    
    async def test_update_status_lifecycle(self, manager, memory):
        """Test status update lifecycle: INITIALIZING → PROCESSING → COMPLETED/FAILED."""
        # Mock existing context in INITIALIZING state
        context_data = {
            'taskId': 'test-123',
            'originalQuery': 'SELECT * FROM users',
            'databaseName': 'app_db',
            'startTime': '2025-01-01T10:00:00',
            'status': 'initializing',
            'evidence': None
        }
        memory.get.return_value = context_data.copy()
        
        # Test INITIALIZING → PROCESSING
        await manager.update_status(TaskStatus.PROCESSING)
        
        # Verify correct memory operations
        assert memory.get.call_count == 1
        memory.get.assert_called_with("taskContext")
        
        assert memory.set.call_count == 1
        set_args = memory.set.call_args[0]
        assert set_args[0] == "taskContext"
        assert set_args[1]['status'] == 'processing'
        
        # Reset mocks and test PROCESSING → COMPLETED
        memory.get.reset_mock()
        memory.set.reset_mock()
        context_data['status'] = 'processing'
        memory.get.return_value = context_data.copy()
        
        await manager.update_status(TaskStatus.COMPLETED)
        
        set_args = memory.set.call_args[0]
        assert set_args[0] == "taskContext"
        assert set_args[1]['status'] == 'completed'
        
        # Reset mocks and test PROCESSING → FAILED
        memory.get.reset_mock()
        memory.set.reset_mock()
        memory.get.return_value = context_data.copy()
        
        await manager.update_status(TaskStatus.FAILED)
        
        set_args = memory.set.call_args[0]
        assert set_args[0] == "taskContext"
        assert set_args[1]['status'] == 'failed'
    
    async def test_update_status_no_context(self, manager, memory):
        """Test graceful handling when updating status with no context."""
        memory.get.return_value = None
        
        # Should handle gracefully without errors
        await manager.update_status(TaskStatus.COMPLETED)
        
        # Verify it tried to get from correct location
        memory.get.assert_called_once_with("taskContext")
        # Should not attempt to set when no context exists
        memory.set.assert_not_called()
    
    async def test_getter_methods(self, manager, memory):
        """Test all getter methods."""
        # Mock context
        context_data = {
            'taskId': 'test-getter-123',
            'originalQuery': 'SELECT name FROM employees',
            'databaseName': 'hr_db',
            'startTime': '2025-01-01T14:00:00',
            'status': 'processing',
            'evidence': 'Employee names are in the name column'
        }
        memory.get.return_value = context_data
        
        # Test each getter
        assert await manager.get_task_id() == 'test-getter-123'
        assert await manager.get_original_query() == 'SELECT name FROM employees'
        assert await manager.get_database_name() == 'hr_db'
        assert await manager.get_status() == TaskStatus.PROCESSING
        assert await manager.get_evidence() == 'Employee names are in the name column'
        
        # Each getter should call memory.get once
        assert memory.get.call_count == 5
    
    async def test_getter_methods_no_context(self, manager, memory):
        """Test getter methods when no context exists."""
        memory.get.return_value = None
        
        assert await manager.get_task_id() is None
        assert await manager.get_original_query() is None
        assert await manager.get_database_name() is None
        assert await manager.get_status() is None
        assert await manager.get_evidence() is None
    
    async def test_status_check_methods(self, manager, memory):
        """Test is_completed and is_failed methods."""
        # Test completed status
        memory.get.return_value = {
            'taskId': 'test-123',
            'originalQuery': 'SELECT * FROM orders',
            'databaseName': 'sales_db',
            'startTime': '2025-01-01T10:00:00',
            'status': 'completed',
            'evidence': None
        }
        
        assert await manager.is_completed() is True
        assert await manager.is_failed() is False
        
        # Test failed status
        memory.get.return_value['status'] = 'failed'
        assert await manager.is_completed() is False
        assert await manager.is_failed() is True
        
        # Test other status
        memory.get.return_value['status'] = 'processing'
        assert await manager.is_completed() is False
        assert await manager.is_failed() is False
        
        # Test no context
        memory.get.return_value = None
        assert await manager.is_completed() is False
        assert await manager.is_failed() is False
    
    async def test_mark_status_methods(self, manager, memory):
        """Test mark_as_processing, mark_as_completed, and mark_as_failed."""
        # Mock existing context
        context_data = {
            'taskId': 'test-mark-123',
            'originalQuery': 'SELECT * FROM products',
            'databaseName': 'inventory_db',
            'startTime': '2025-01-01T10:00:00',
            'status': 'initializing',
            'evidence': None
        }
        memory.get.return_value = context_data.copy()
        
        # Test mark_as_processing
        await manager.mark_as_processing()
        assert memory.set.call_args[0][1]['status'] == 'processing'
        
        # Test mark_as_completed
        memory.get.return_value = context_data.copy()
        await manager.mark_as_completed()
        assert memory.set.call_args[0][1]['status'] == 'completed'
        
        # Test mark_as_failed
        memory.get.return_value = context_data.copy()
        await manager.mark_as_failed()
        assert memory.set.call_args[0][1]['status'] == 'failed'
        
        # Verify all calls
        assert memory.set.call_count == 3
        assert memory.get.call_count == 3


    async def test_missing_required_fields(self, manager, memory):
        """Test handling of missing required fields gracefully."""
        # Test missing task_id
        with pytest.raises((ValueError, TypeError)):
            await manager.initialize(
                original_query="SELECT * FROM users",
                database_name="test_db"
            )
        
        # Test missing original_query
        with pytest.raises((ValueError, TypeError)):
            await manager.initialize(
                task_id="test-123",
                database_name="test_db"
            )
        
        # Test missing database_name
        with pytest.raises((ValueError, TypeError)):
            await manager.initialize(
                task_id="test-123",
                original_query="SELECT * FROM users"
            )
    
    async def test_invalid_task_data(self, manager, memory):
        """Test handling of invalid task data gracefully."""
        # Mock corrupted data in memory
        invalid_data = {
            'taskId': 'test-123',
            'originalQuery': 'SELECT * FROM users',
            # Missing required fields
            'status': 'invalid_status'  # Invalid status value
        }
        memory.get.return_value = invalid_data
        
        # Should handle gracefully
        result = await manager.get()
        # Implementation should either return None or handle the error appropriately
        # The exact behavior depends on the implementation
    
    async def test_data_persistence_verification(self, manager, memory):
        """Test that data persists correctly across operations."""
        # Initialize task
        task_data = {
            'taskId': 'persist-test-123',
            'originalQuery': 'SELECT * FROM orders WHERE total > 1000',
            'databaseName': 'sales_db',
            'startTime': '2025-01-01T10:00:00',
            'status': 'initializing',
            'evidence': 'Orders have a total column'
        }
        
        # First operation: Initialize
        with patch('task_context_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = task_data['startTime']
            await manager.initialize(
                task_id=task_data['taskId'],
                original_query=task_data['originalQuery'],
                database_name=task_data['databaseName'],
                evidence=task_data['evidence']
            )
        
        # Verify initial data was stored correctly
        initial_call = memory.set.call_args[0]
        assert initial_call[0] == "taskContext"
        stored_data = initial_call[1]
        
        # Second operation: Update status
        memory.get.return_value = stored_data
        await manager.update_status(TaskStatus.PROCESSING)
        
        # Verify all original data is preserved
        updated_call = memory.set.call_args[0]
        updated_data = updated_call[1]
        assert updated_data['taskId'] == task_data['taskId']
        assert updated_data['originalQuery'] == task_data['originalQuery']
        assert updated_data['databaseName'] == task_data['databaseName']
        assert updated_data['evidence'] == task_data['evidence']
        assert updated_data['startTime'] == task_data['startTime']
        # Only status should change
        assert updated_data['status'] == 'processing'


@pytest.mark.asyncio
class TestTaskContextManagerIntegration:
    """Integration tests with real KeyValueMemory for data persistence."""
    
    async def test_full_lifecycle_with_persistence(self):
        """Test complete lifecycle verifying data persistence at each step."""
        memory = KeyValueMemory()
        manager = TaskContextManager(memory)
        
        # Initialize
        task_id = "integration-test-123"
        query = "SELECT customer_id, order_count FROM customers WHERE order_count > 10"
        db_name = "sales_database"
        evidence = "Order count is stored in the order_count column"
        
        context = await manager.initialize(
            task_id=task_id,
            original_query=query,
            database_name=db_name,
            evidence=evidence
        )
        
        # Verify data is stored at correct location
        stored_data = await memory.get("taskContext")
        assert stored_data is not None
        assert stored_data['taskId'] == task_id
        
        # Verify initialization
        assert context.status == TaskStatus.INITIALIZING
        assert await manager.get_task_id() == task_id
        assert await manager.get_evidence() == evidence
        
        # Update to processing
        await manager.mark_as_processing()
        
        # Verify data persistence after status update
        stored_data = await memory.get("taskContext")
        assert stored_data['status'] == 'processing'
        assert stored_data['taskId'] == task_id  # Original data preserved
        assert stored_data['originalQuery'] == query
        assert stored_data['evidence'] == evidence
        
        assert await manager.get_status() == TaskStatus.PROCESSING
        assert not await manager.is_completed()
        assert not await manager.is_failed()
        
        # Complete the task
        await manager.mark_as_completed()
        
        # Verify final data persistence
        stored_data = await memory.get("taskContext")
        assert stored_data['status'] == 'completed'
        
        assert await manager.get_status() == TaskStatus.COMPLETED
        assert await manager.is_completed()
        assert not await manager.is_failed()
        
        # Verify all data is preserved throughout lifecycle
        final_context = await manager.get()
        assert final_context.taskId == task_id
        assert final_context.originalQuery == query
        assert final_context.databaseName == db_name
        assert final_context.evidence == evidence
        assert final_context.status == TaskStatus.COMPLETED