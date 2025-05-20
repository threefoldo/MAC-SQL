"""
Configuration for the Context Understanding Agent.
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class ContextUnderstandingConfig:
    """Configuration for the Context Understanding Agent."""
    
    name: str = "ContextUnderstandingAgent"
    description: str = "Analyzes user queries against database schemas"
    
    # LLM configuration
    llm_config: Dict[str, Any] = field(default_factory=lambda: {
        "model": "gpt-4",
        "temperature": 0.7,
        "max_tokens": 2000,
        "timeout": 30
    })
    
    # Agent behavior configuration
    max_clarifications: int = 3
    confidence_threshold: float = 0.8
    
    # XML response configuration
    include_schema_in_response: bool = True
    validate_xml_output: bool = True
    
    # Tool configurations
    tools_config: Dict[str, Any] = field(default_factory=lambda: {
        "read_database_schema": {
            "description": "Read database schema information",
            "timeout": 10
        }
    })
    
    # Debug options
    debug: bool = False
    log_level: str = "INFO"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "llm_config": self.llm_config,
            "max_clarifications": self.max_clarifications,
            "confidence_threshold": self.confidence_threshold,
            "include_schema_in_response": self.include_schema_in_response,
            "validate_xml_output": self.validate_xml_output,
            "tools_config": self.tools_config,
            "debug": self.debug,
            "log_level": self.log_level
        }