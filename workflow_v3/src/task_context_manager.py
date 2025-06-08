"""
Task context manager for text-to-SQL tree orchestration.

This module provides easy access to task context data stored in KeyValueMemory.
"""

import logging
from typing import Optional
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from memory_content_types import TaskContext, TaskStatus


class TaskContextManager:
    """Manages task context data in memory."""
    
    def __init__(self, memory: KeyValueMemory):
        """
        Initialize the task context manager.
        
        Args:
            memory: The KeyValueMemory instance to use for storage
        """
        self.memory = memory
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def initialize(self, task_id: str, original_query: str, database_name: str, evidence: Optional[str] = None) -> TaskContext:
        """
        Initialize a new task context.
        
        Args:
            task_id: Unique identifier for the task
            original_query: The original user query
            database_name: Name of the database to query
            evidence: Optional evidence/hints for the query
            
        Returns:
            The initialized TaskContext
        """
        task_context = TaskContext(
            taskId=task_id,
            originalQuery=original_query,
            databaseName=database_name,
            startTime=datetime.now().isoformat(),
            status=TaskStatus.INITIALIZING,
            evidence=evidence
        )
        
        await self.memory.set("taskContext", task_context.to_dict())
        self.logger.info(f"Initialized task context for task {task_id}")
        if evidence:
            self.logger.info(f"Evidence provided: {evidence}")
        
        return task_context
    
    async def get(self) -> Optional[TaskContext]:
        """
        Get the current task context.
        
        Returns:
            The current TaskContext or None if not found
        """
        data = await self.memory.get("taskContext")
        if data:
            return TaskContext.from_dict(data)
        return None
    
    async def update_status(self, status: TaskStatus) -> None:
        """
        Update the task status.
        
        Args:
            status: The new status to set
        """
        task_context = await self.get()
        if task_context:
            task_context.status = status
            await self.memory.set("taskContext", task_context.to_dict())
            self.logger.info(f"Updated task status to {status.value}")
        else:
            self.logger.warning("No task context found to update")
    
    async def get_task_id(self) -> Optional[str]:
        """Get the current task ID."""
        task_context = await self.get()
        return task_context.taskId if task_context else None
    
    async def get_original_query(self) -> Optional[str]:
        """Get the original query."""
        task_context = await self.get()
        return task_context.originalQuery if task_context else None
    
    async def get_database_name(self) -> Optional[str]:
        """Get the database name."""
        task_context = await self.get()
        return task_context.databaseName if task_context else None
    
    async def get_status(self) -> Optional[TaskStatus]:
        """Get the current task status."""
        task_context = await self.get()
        return task_context.status if task_context else None
    
    async def get_evidence(self) -> Optional[str]:
        """Get the evidence/hints for the query."""
        task_context = await self.get()
        return task_context.evidence if task_context else None
    
    async def is_completed(self) -> bool:
        """Check if the task is completed."""
        status = await self.get_status()
        return status == TaskStatus.COMPLETED if status else False
    
    async def is_failed(self) -> bool:
        """Check if the task has failed."""
        status = await self.get_status()
        return status == TaskStatus.FAILED if status else False
    
    async def mark_as_processing(self) -> None:
        """Mark the task as processing."""
        await self.update_status(TaskStatus.PROCESSING)
    
    async def mark_as_completed(self) -> None:
        """Mark the task as completed."""
        await self.update_status(TaskStatus.COMPLETED)
    
    async def mark_as_failed(self) -> None:
        """Mark the task as failed."""
        await self.update_status(TaskStatus.FAILED)