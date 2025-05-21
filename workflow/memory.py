"""
Memory implementations for the text-to-SQL workflow.

This module contains memory implementations for the text-to-SQL workflow,
including a key-value store memory implementation for the TaskOrchestrator.
"""

import logging
from typing import Dict, List, Optional, Any, Union

from autogen_core import CancellationToken
from autogen_core.memory import Memory
from autogen_core.memory import MemoryContent, MemoryQueryResult
from autogen_core.memory import MemoryMimeType
from autogen_core.memory import UpdateContextResult
from autogen_core.model_context import ChatCompletionContext

# Type definition for content
ContentType = Union[str, dict, bytes, Any]

# --- New KeyValueMemory Implementation ---
class KeyValueMemory(Memory):
    """
    An in-memory implementation of the autogen_core.Memory protocol,
    acting like a key-value store for the TaskOrchestrator's needs.
    Variables are stored as MemoryContent items with a 'variable_name' in metadata.
    """
    def __init__(self, name: str = "kv_memory", **kwargs):
        self.name = name
        self._store: List[MemoryContent] = []
        logging.debug(f"[{self.__class__.__name__}] Initialized.")

    async def add(self, content: MemoryContent, cancellation_token: Optional[CancellationToken] = None) -> None:
        """
        Add a new content item to the memory store.
        
        Args:
            content: The content to add to memory
            cancellation_token: Optional token to cancel the operation
        """
        if cancellation_token and cancellation_token.is_cancelled:
            logging.warning(f"[{self.__class__.__name__}] Add operation cancelled.")
            return
        self._store.append(content)
        logging.debug(f"[{self.__class__.__name__}] Added content with metadata: {content.metadata}. Store size: {len(self._store)}")

    async def query(
        self,
        query_input: Union[str, MemoryContent],  # Renamed 'query' to 'query_input' to avoid conflict
        cancellation_token: Optional[CancellationToken] = None,
        **kwargs: Any,
    ) -> MemoryQueryResult:
        """
        Query the memory store for relevant memories.
        
        Args:
            query_input: Either a string (variable name) or MemoryContent to match
            cancellation_token: Optional token to cancel the operation
            
        Returns:
            MemoryQueryResult with matching memory items
        """
        if cancellation_token and cancellation_token.is_cancelled:
            logging.warning(f"[{self.__class__.__name__}] Query operation cancelled.")
            return MemoryQueryResult(results=[])

        results: List[MemoryContent] = []
        if isinstance(query_input, str):
            key_to_find = query_input
            # Iterate in reverse to find the latest entry for a variable_name
            for item in reversed(self._store):
                if item.metadata and item.metadata.get("variable_name") == key_to_find:
                    results.append(item)
                    # For variable lookup, we typically want only the most recent one.
                    break
            logging.debug(f"[{self.__class__.__name__}] Querying for variable_name='{key_to_find}', found {len(results)} item(s) (returning latest).")

        elif isinstance(query_input, MemoryContent):
            # Example: simple metadata or content based query
            for item in self._store:
                match = False
                if query_input.metadata and item.metadata and query_input.metadata.items() <= item.metadata.items():  # Subset match
                    match = True
                elif isinstance(query_input.content, str) and isinstance(item.content, str) and query_input.content in item.content:
                    match = True
                if match:
                    results.append(item)
            logging.debug(f"[{self.__class__.__name__}] Querying with MemoryContent, found {len(results)} item(s).")
        return MemoryQueryResult(results=results)

    async def update_context(self, model_context: ChatCompletionContext) -> UpdateContextResult:
        """
        Update a model context with relevant memories.
        
        Args:
            model_context: The model context to update
            
        Returns:
            UpdateContextResult containing the memories used to update the context
        """
        # This specific orchestrator does not heavily rely on this method.
        # A more general implementation might retrieve relevant memories and add them to model_context.messages
        logging.debug(f"[{self.__class__.__name__}] update_context called. No direct modification to model_context for this example.")
        # Return all text items as a basic example of "relevant memories"
        text_memories = [item for item in self._store if item.mime_type in [MemoryMimeType.TEXT, MemoryMimeType.TEXT.value]]
        return UpdateContextResult(memories=MemoryQueryResult(results=text_memories))

    async def clear(self) -> None:
        """Clear all entries from the memory store."""
        self._store.clear()
        logging.info(f"[{self.__class__.__name__}] Memory cleared.")

    async def close(self) -> None:
        """Clean up any resources used by the memory store."""
        logging.debug(f"[{self.__class__.__name__}] Close called (no-op for in-memory).")

    # --- Simplified convenience methods for TaskOrchestrator ---
    async def set(
        self,
        key: str,
        value: ContentType,
        mime_type: Optional[Union[MemoryMimeType, str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Set a key-value pair in the memory store.
        
        Args:
            key: The key to use for storing the value
            value: The value to store
            mime_type: Optional MIME type for the content
            metadata: Optional additional metadata to store with the content
        """
        if mime_type is None:
            if isinstance(value, str): 
                mime_type = MemoryMimeType.TEXT
            elif isinstance(value, dict): 
                mime_type = MemoryMimeType.JSON
            elif isinstance(value, (int, float, bool)): 
                mime_type = MemoryMimeType.JSON  # Store simple types as JSON
            elif isinstance(value, bytes): 
                mime_type = MemoryMimeType.BINARY
            elif hasattr(value, "__class__") and value.__class__.__name__ == "Image": 
                mime_type = MemoryMimeType.IMAGE
            else:
                # Default to TEXT if unsure, or raise error
                logging.warning(f"Cannot auto-detect MIME type for value of type {type(value)}. Defaulting to TEXT/PLAIN.")
                mime_type = MemoryMimeType.TEXT

        item_metadata = {"variable_name": key}
        if metadata: 
            item_metadata.update(metadata)

        content_item = MemoryContent(content=value, mime_type=mime_type, metadata=item_metadata)
        await self.add(content_item)
        logging.debug(f"[{self.__class__.__name__}] Set key '{key}'.")

    async def get(self, key: str) -> Optional[ContentType]:
        """
        Get a value by key from the memory store.
        
        Args:
            key: The key to retrieve the value for
            
        Returns:
            The value associated with the key, or None if not found
        """
        query_result = await self.query(key)
        if query_result.results:
            return query_result.results[0].content
        return None

    async def get_with_details(self, key: str) -> Optional[MemoryContent]:
        """
        Get a value with its full details by key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            MemoryContent object containing the value and metadata, or None if not found
        """
        query_result = await self.query(key)
        if query_result.results:
            return query_result.results[0]
        return None