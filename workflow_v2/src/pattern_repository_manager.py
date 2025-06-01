"""
Pattern Repository Manager for text-to-SQL rule-based learning.

Simple rule-based system that maintains sets of DO/DON'T rules for each agent,
similar to SQL constraints. Rules get updated as new patterns are discovered.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from keyvalue_memory import KeyValueMemory


class PatternRepositoryManager:
    """
    Simple rule-based pattern repository for text-to-SQL agents.
    
    Maintains rule sets for each agent type:
    - schema_linker_rules
    - sql_generator_rules  
    - query_analyzer_rules
    - sql_evaluator_rules
    
    Each rule set contains:
    - DO rules (good patterns to follow)
    - DON'T rules (bad patterns to avoid)
    """
    
    def __init__(self, memory: KeyValueMemory):
        """Initialize the pattern repository manager"""
        self.memory = memory
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize task context manager to get database name dynamically
        from task_context_manager import TaskContextManager
        self.task_manager = TaskContextManager(self.memory)
    
    async def get_rules_for_agent(self, agent_type: str) -> Dict[str, List[str]]:
        """Get DO/DON'T rules for a specific agent"""
        try:
            database_name = await self._get_database_name()
            rules_key = f"rules_{database_name}_{agent_type}"
            rules = await self.memory.get(rules_key, {
                "do_rules": [],
                "dont_rules": [],
                "last_updated": None
            })
            return rules
        except Exception as e:
            self.logger.error(f"Error getting rules for {agent_type}: {str(e)}", exc_info=True)
            return {"do_rules": [], "dont_rules": [], "last_updated": None}
    
    async def add_do_rule(self, agent_type: str, rule: str) -> None:
        """Add a DO rule for an agent"""
        try:
            rules = await self.get_rules_for_agent(agent_type)
            
            if rule not in rules["do_rules"]:
                rules["do_rules"].append(rule)
                rules["last_updated"] = datetime.now().isoformat()
                
                # Keep only latest 15 rules to avoid context bloat
                if len(rules["do_rules"]) > 15:
                    rules["do_rules"].pop(0)
                
                await self._store_rules(agent_type, rules)
                database_name = await self._get_database_name()
                self.logger.info(f"Added DO rule for {agent_type} on {database_name}: {rule}")
        except Exception as e:
            self.logger.error(f"Error adding DO rule: {str(e)}", exc_info=True)
    
    async def add_dont_rule(self, agent_type: str, rule: str) -> None:
        """Add a DON'T rule for an agent"""
        try:
            rules = await self.get_rules_for_agent(agent_type)
            
            if rule not in rules["dont_rules"]:
                rules["dont_rules"].append(rule)
                rules["last_updated"] = datetime.now().isoformat()
                
                # Keep only latest 15 rules to avoid context bloat
                if len(rules["dont_rules"]) > 15:
                    rules["dont_rules"].pop(0)
                
                await self._store_rules(agent_type, rules)
                database_name = await self._get_database_name()
                self.logger.info(f"Added DON'T rule for {agent_type} on {database_name}: {rule}")
        except Exception as e:
            self.logger.error(f"Error adding DON'T rule: {str(e)}", exc_info=True)
    
    async def format_rules_for_prompt(self, agent_type: str) -> str:
        """Format rules for inclusion in agent prompts"""
        try:
            rules = await self.get_rules_for_agent(agent_type)
            
            if not rules["do_rules"] and not rules["dont_rules"]:
                return ""
            
            database_name = await self._get_database_name()
            prompt_parts = [f"\n=== LEARNED RULES FOR {database_name.upper()} DATABASE ==="]
            
            if rules["do_rules"]:
                prompt_parts.append("\n✅ DO:")
                for rule in rules["do_rules"][-10:]:  # Latest 10 rules
                    prompt_parts.append(f"  • {rule}")
            
            if rules["dont_rules"]:
                prompt_parts.append("\n❌ DON'T:")
                for rule in rules["dont_rules"][-10:]:  # Latest 10 rules
                    prompt_parts.append(f"  • {rule}")
            
            prompt_parts.append("")  # Empty line for spacing
            return "\n".join(prompt_parts)
            
        except Exception as e:
            self.logger.error(f"Error formatting rules for prompt: {str(e)}", exc_info=True)
            return ""
    
    async def update_rules_from_success(self, success_analysis: Dict[str, Any]) -> None:
        """Update rules based on successful execution analysis"""
        try:
            # Extract agent rules directly from success analysis
            agent_rules = success_analysis.get("agent_rules", {})
            
            for agent_type, rules_data in agent_rules.items():
                if isinstance(rules_data, dict):
                    for rule_key, rule_content in rules_data.items():
                        if rule_key.startswith("do_rule_") and rule_content:
                            await self.add_do_rule(agent_type, rule_content)
            
            database_name = await self._get_database_name()
            self.logger.info(f"Updated rules from success analysis for {database_name}")
            
        except Exception as e:
            self.logger.error(f"Error updating rules from success: {str(e)}", exc_info=True)
    
    async def update_rules_from_failure(self, failure_analysis: Dict[str, Any]) -> None:
        """Update rules based on failure analysis"""
        try:
            # Extract agent rules directly from failure analysis
            agent_rules = failure_analysis.get("agent_rules", {})
            
            for agent_type, rules_data in agent_rules.items():
                if isinstance(rules_data, dict):
                    for rule_key, rule_content in rules_data.items():
                        if rule_key.startswith("dont_rule_") and rule_content:
                            await self.add_dont_rule(agent_type, rule_content)
            
            database_name = await self._get_database_name()
            self.logger.info(f"Updated rules from failure analysis for {database_name}")
            
        except Exception as e:
            self.logger.error(f"Error updating rules from failure: {str(e)}", exc_info=True)
    
    async def get_all_database_rules(self) -> Dict[str, Dict[str, List[str]]]:
        """Get all rules for all agents"""
        try:
            all_rules = {}
            agent_types = ["schema_linker", "sql_generator", "query_analyzer"]
            
            for agent_type in agent_types:
                all_rules[agent_type] = await self.get_rules_for_agent(agent_type)
            
            return all_rules
            
        except Exception as e:
            self.logger.error(f"Error getting all database rules: {str(e)}", exc_info=True)
            return {}
    
    async def _store_rules(self, agent_type: str, rules: Dict[str, Any]) -> None:
        """Store rules for an agent"""
        database_name = await self._get_database_name()
        rules_key = f"rules_{database_name}_{agent_type}"
        await self.memory.set(rules_key, rules)
    
    async def clear_rules(self, agent_type: str) -> None:
        """Clear all rules for an agent (useful for testing/reset)"""
        try:
            rules = {
                "do_rules": [],
                "dont_rules": [],
                "last_updated": datetime.now().isoformat()
            }
            await self._store_rules(agent_type, rules)
            database_name = await self._get_database_name()
            self.logger.info(f"Cleared all rules for {agent_type} on {database_name}")
        except Exception as e:
            self.logger.error(f"Error clearing rules: {str(e)}", exc_info=True)
    
    async def _get_database_name(self) -> str:
        """Get database name from task context"""
        try:
            task_context = await self.task_manager.get()
            return task_context.databaseName if task_context else "unknown"
        except Exception as e:
            self.logger.error(f"Error getting database name: {str(e)}", exc_info=True)
            return "unknown"