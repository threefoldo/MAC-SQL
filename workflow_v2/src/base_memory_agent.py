"""
Base class for memory-enabled agents in the text-to-SQL tree orchestration.

This module provides a base class that standardizes how agents are created
and integrated with KeyValueMemory using the MemoryAgentTool pattern.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from autogen_core import CancellationToken
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.base import TaskResult
from autogen_ext.models.openai import OpenAIChatCompletionClient

from keyvalue_memory import KeyValueMemory
from memory_agent_tool import MemoryAgentTool


class BaseMemoryAgent(ABC):
    """
    Base class for agents that use memory in the text-to-SQL tree orchestration.
    
    This class provides the standard pattern for creating agents that:
    1. Use AutoGen's AssistantAgent for LLM interactions
    2. Integrate with KeyValueMemory for state management
    3. Are wrapped with MemoryAgentTool for use in tree orchestration
    """
    
    # Override in subclasses
    agent_name: str = "base_agent"
    
    def __init__(
        self, 
        memory: KeyValueMemory,
        llm_config: Optional[Dict[str, Any]] = None,
        debug: bool = False
    ):
        """
        Initialize the memory-enabled agent.
        
        Args:
            memory: The KeyValueMemory instance for state management
            llm_config: LLM configuration dictionary with keys:
                - model_name: The LLM model to use (default: "gpt-4o")
                - temperature: Temperature for LLM responses (default: 0.1)
                - timeout: Timeout for LLM calls in seconds (default: 120)
                - max_tokens: Maximum tokens in response (optional)
                - top_p: Nucleus sampling parameter (optional)
                - frequency_penalty: Frequency penalty (optional)
                - presence_penalty: Presence penalty (optional)
            debug: Whether to enable debug logging
        """
        self.memory = memory
        self.debug = debug
        
        # Default LLM configuration
        default_llm_config = {
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 120
        }
        
        # Merge user config with defaults
        self.llm_config = {**default_llm_config, **(llm_config or {})}
        
        
        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)
        if debug:
            self.logger.setLevel(logging.DEBUG)
        
        # Initialize components
        self._initialize_managers()
        self._create_model_client()
        self._create_agent()
        self._create_tool()
        
        self.logger.info(f"Initialized {self.agent_name} with model {self.llm_config['model_name']}")
    
    def _create_model_client(self):
        """Create the OpenAI model client"""
        # Build client config from llm_config
        client_config = {
            "model": self.llm_config["model_name"],
            "temperature": self.llm_config["temperature"],
            "timeout": self.llm_config["timeout"]
        }
        
        # Add optional parameters if present
        optional_params = ["max_tokens", "top_p", "frequency_penalty", "presence_penalty"]
        for param in optional_params:
            if param in self.llm_config:
                client_config[param] = self.llm_config[param]
        
        self.model_client = OpenAIChatCompletionClient(**client_config)
    
    def _create_agent(self):
        """Create the AutoGen AssistantAgent"""
        self.assistant = AssistantAgent(
            name=self.agent_name,
            system_message=self._build_system_message(),
            model_client=self.model_client
        )
        self.logger.debug(f"Created AssistantAgent: {self.agent_name}")
    
    def _create_tool(self):
        """Create the MemoryAgentTool wrapper"""
        self.tool = MemoryAgentTool(
            agent=self.assistant,
            memory=self.memory,
            reader_callback=self._reader_callback,
            parser_callback=self._parser_callback
        )
        self.logger.debug(f"Created MemoryAgentTool for {self.agent_name}")
    
    @abstractmethod
    def _initialize_managers(self):
        """
        Initialize any manager classes needed by the agent.
        Override in subclasses to set up DatabaseSchemaManager, QueryTreeManager, etc.
        """
        pass
    
    @abstractmethod
    def _build_system_message(self) -> str:
        """
        Build the system message for the agent.
        Override in subclasses to provide agent-specific instructions.
        
        Returns:
            The system message string
        """
        pass
    
    @abstractmethod
    async def _reader_callback(
        self, 
        memory: KeyValueMemory, 
        task: str, 
        cancellation_token: Optional[CancellationToken]
    ) -> Dict[str, Any]:
        """
        Read context from memory before agent execution.
        Override in subclasses to implement memory reading logic.
        
        Args:
            memory: The memory instance
            task: The task string from the coordinator
            cancellation_token: Token for cancellation
            
        Returns:
            Dictionary of context to provide to the agent
        """
        pass
    
    @abstractmethod
    async def _parser_callback(
        self, 
        memory: KeyValueMemory, 
        task: str,
        result: TaskResult,
        cancellation_token: Optional[CancellationToken]
    ) -> None:
        """
        Parse agent results and update memory after execution.
        Override in subclasses to implement result parsing logic.
        
        Args:
            memory: The memory instance
            task: The original task string
            result: The agent's execution result
            cancellation_token: Token for cancellation
        """
        pass
    
    def get_tool(self) -> MemoryAgentTool:
        """
        Get the wrapped tool for use in tree orchestration.
        
        Returns:
            The MemoryAgentTool instance
        """
        return self.tool
    
    async def run(self, goal: str) -> TaskResult:
        """
        Run the agent directly (mainly for testing).
        
        Args:
            task: The task to execute
            
        Returns:
            The task result
        """
        from autogen_core import default_subscription
        
        # Create a simple task object
        class SimpleTask:
            def __init__(self, task_str):
                self.goal = task_str
        
        return await self.tool.run(SimpleTask(goal), cancellation_token=None)


