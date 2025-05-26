"""
Memory-enabled Agent Tool for text-to-SQL tree orchestration.

This module implements a memory-enabled agent tool that provides
pre-processing (memory reading) and post-processing (memory updating) callbacks
that can be customized for each agent.
"""

import logging
import re
from typing import Any, Callable, Dict, List, Mapping, Optional, Type, Union

from autogen_core import CancellationToken, Component, ComponentModel
from autogen_core.memory import Memory, MemoryContent
from autogen_core.tools import BaseTool
from autogen_agentchat.agents import BaseChatAgent
from autogen_agentchat.base import TaskResult
from pydantic import BaseModel, Field
from typing_extensions import Self, Annotated

# Type aliases for callback functions
MemoryReaderCallbackType = Callable[[Memory, str, CancellationToken], Dict[str, Any]]
MemoryParserCallbackType = Callable[[Memory, str, TaskResult, CancellationToken], None]


class MemoryAgentToolArgs(BaseModel):
    """Input arguments for the MemoryAgentTool."""
    
    task: Annotated[str, "The task to be executed."]


class MemoryAgentToolConfig(BaseModel):
    """Configuration for the MemoryAgentTool."""
    
    agent: ComponentModel
    memory: Optional[ComponentModel] = None


class MemoryAgentTool(BaseTool[MemoryAgentToolArgs, TaskResult], Component[MemoryAgentToolConfig]):
    """
    A BaseTool implementation that integrates with memory.
    
    This tool adds memory capabilities to an agent, providing:
    1. A pre-processing step to read from memory before running the agent
    2. A post-processing step to update memory after the agent completes
    
    Each agent can have custom reader and parser callback functions that define
    how to interact with the memory store.
    
    Args:
        agent (BaseChatAgent): The agent to be used for running tasks
        memory (Memory): The memory instance to use for storing/retrieving context
        reader_callback: A function that reads from memory before agent execution
        parser_callback: A function that updates memory after agent execution
        
    Example:
        ```python
        # Define custom reader and parser functions
        async def schema_reader(memory, task, cancellation_token):
            # Read relevant schema information from memory
            schema_content = await memory.get("current_schema")
            if schema_content:
                return {"schema": schema_content}
            return {}
            
        async def sql_result_parser(memory, task, result, cancellation_token):
            # Extract SQL from result and store in memory
            sql = extract_sql_from_text(result.last_message().content)
            if sql:
                await memory.set("last_generated_sql", sql)
                
        # Create memory-enabled agent tool
        sql_generator = MemoryAgentTool(
            agent=sql_generator_agent,
            memory=kv_memory,
            reader_callback=schema_reader,
            parser_callback=sql_result_parser
        )
        ```
    """
    
    component_config_schema = MemoryAgentToolConfig
    component_provider_override = "tree_orchestrator.memory_agent_tool.MemoryAgentTool"
    
    def __init__(
        self, 
        agent: BaseChatAgent,
        memory: Optional[Memory] = None,
        reader_callback: Optional[MemoryReaderCallbackType] = None,
        parser_callback: Optional[MemoryParserCallbackType] = None
    ) -> None:
        """
        Initialize the memory-enabled agent tool.
        
        Args:
            agent: The agent to be wrapped with memory capabilities
            memory: The memory instance to use
            reader_callback: Function to read from memory before agent execution
            parser_callback: Function to update memory after agent execution
        """
        self._agent = agent
        self._memory = memory
        self._reader_callback = reader_callback
        self._parser_callback = parser_callback
        
        # Initialize the parent class
        super().__init__(
            args_type=MemoryAgentToolArgs,
            return_type=TaskResult,
            name=agent.name,
            description=agent.description
        )
        
        logging.debug(f"[{self.__class__.__name__}] Initialized with agent '{agent.name}', "
                     f"memory: {memory is not None}, "
                     f"reader: {reader_callback is not None}, "
                     f"parser: {parser_callback is not None}")
    
    async def run(self, args: MemoryAgentToolArgs, cancellation_token: CancellationToken) -> TaskResult:
        """
        Run the agent with memory integration.
        
        This method:
        1. Calls the reader_callback to get context from memory
        2. Enhances the task with memory context if available
        3. Runs the agent with the enhanced task
        4. Calls the parser_callback to update memory with results
        5. Returns the original task result
        
        Args:
            args: The task arguments
            cancellation_token: Token for cancellation support
            
        Returns:
            The result of the task execution
        """
        task = args.task
        memory_context = {}
        
        # Step 1: Read from memory if both memory and reader callback are available
        if self._memory and self._reader_callback:
            try:
                memory_context = await self._reader_callback(self._memory, task, cancellation_token)
                logging.debug(f"[{self.__class__.__name__}] Read memory context: {list(memory_context.keys())}")
            except Exception as e:
                logging.error(f"[{self.__class__.__name__}] Error reading from memory: {str(e)}")
        
        # Step 2: Enhance task with memory context if available
        enhanced_task = task
        if memory_context:
            # Simple approach: add context as formatted text
            formatted_context = self._format_memory_context(memory_context)
            if formatted_context:
                enhanced_task = f"{formatted_context}\n\n{task}"
                logging.debug(f"[{self.__class__.__name__}] Enhanced task with memory context")
        
        # Step 3: Run the agent with enhanced task
        result = await self._agent.run(task=enhanced_task, cancellation_token=cancellation_token)
        
        # Step 4: Update memory if both memory and parser callback are available
        if self._memory and self._parser_callback:
            try:
                await self._parser_callback(self._memory, task, result, cancellation_token)
                logging.debug(f"[{self.__class__.__name__}] Updated memory with task result")
            except Exception as e:
                logging.error(f"[{self.__class__.__name__}] Error updating memory: {str(e)}")
        
        return result
    
    def _format_memory_context(self, context: Dict[str, Any]) -> str:
        """
        Format memory context as text to prepend to the task.
        
        Args:
            context: Dictionary of context from memory
            
        Returns:
            Formatted context string
        """
        if not context:
            return ""
            
        parts = ["I'm providing you with context from previous interactions:"]
        
        for key, value in context.items():
            if isinstance(value, str):
                parts.append(f"### {key.replace('_', ' ').title()}\n{value}")
            elif isinstance(value, dict):
                # For dictionaries, format as JSON-like text
                dict_parts = [f"### {key.replace('_', ' ').title()}"]
                for k, v in value.items():
                    dict_parts.append(f"- {k}: {v}")
                parts.append("\n".join(dict_parts))
            else:
                # For other types, use simple string representation
                parts.append(f"### {key.replace('_', ' ').title()}\n{str(value)}")
        
        return "\n\n".join(parts)
    
    def _to_config(self) -> MemoryAgentToolConfig:
        """Convert to configuration."""
        config = MemoryAgentToolConfig(
            agent=self._agent.dump_component(),
        )
        
        if self._memory and hasattr(self._memory, "dump_component"):
            config.memory = self._memory.dump_component()
            
        return config
    
    @classmethod
    def _from_config(cls, config: MemoryAgentToolConfig) -> Self:
        """Create from configuration."""
        agent = BaseChatAgent.load_component(config.agent)
        memory = None
        
        if config.memory:
            memory = Memory.load_component(config.memory)
            
        return cls(agent=agent, memory=memory)