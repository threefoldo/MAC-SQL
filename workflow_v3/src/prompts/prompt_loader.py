"""
Simple prompt loader - selects version from agent prompt files
"""

from typing import Dict, Any
from . import query_analyzer_prompts
from . import schema_linker_prompts  
from . import sql_generator_prompts
from . import sql_evaluator_prompts
from . import orchestrator_prompts

class PromptLoader:
    """Load prompts from versioned agent prompt files"""
    
    def __init__(self):
        self.agents = {
            "query_analyzer": query_analyzer_prompts,
            "schema_linker": schema_linker_prompts,
            "sql_generator": sql_generator_prompts,
            "sql_evaluator": sql_evaluator_prompts,
            "orchestrator": orchestrator_prompts
        }
    
    def get_prompt(self, agent_name: str, version: str = None) -> str:
        """Get complete prompt for agent"""
        
        # Get agent module
        agent_module = self.agents.get(agent_name)
        if not agent_module:
            raise ValueError(f"Unknown agent: {agent_name}")
        
        # Use PROMPT_TEMPLATE (ignore version parameter for backward compatibility)
        if hasattr(agent_module, 'PROMPT_TEMPLATE'):
            return agent_module.PROMPT_TEMPLATE
        
        # Fallback for old version system (deprecated)
        version = version or getattr(agent_module, 'DEFAULT_VERSION', 'v1.2')
        if hasattr(agent_module, 'VERSIONS') and version in agent_module.VERSIONS:
            return agent_module.VERSIONS[version]["template"]
        
        raise ValueError(f"No prompt found for {agent_name}")
    
    def list_versions(self, agent_name: str) -> Dict[str, Any]:
        """List all available versions for an agent"""
        agent_module = self.agents.get(agent_name)
        if not agent_module:
            return {}
        
        return agent_module.VERSIONS
    
    def get_version_info(self, agent_name: str, version: str) -> Dict[str, Any]:
        """Get metadata about a specific version"""
        agent_module = self.agents.get(agent_name)
        if not agent_module or version not in agent_module.VERSIONS:
            return {}
        
        return agent_module.VERSIONS[version]

# Usage in agent code:
# from prompts.prompt_loader import PromptLoader
# loader = PromptLoader()
# prompt = loader.get_prompt("query_analyzer", "v1.0")