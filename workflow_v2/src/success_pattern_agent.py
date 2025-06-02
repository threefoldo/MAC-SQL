"""
Success Pattern Agent for text-to-SQL intelligent learning.

This agent analyzes successful SQL executions and maintains a repository
of success patterns for future guidance.
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from utils import parse_xml_hybrid
from pattern_repository_manager import PatternRepositoryManager


class SuccessPatternAgent(BaseMemoryAgent):
    """
    Analyzes SUCCESSFUL executions (GOOD/EXCELLENT quality) and generates DO rules.
    
    This agent is ONLY called when:
    - SQL execution succeeded without errors
    - Evaluation quality is GOOD or EXCELLENT  
    - Query correctly answers the intended question
    
    It generates DO rules that capture successful patterns for future use.
    """
    
    agent_name = "success_pattern_agent"
    
    def _initialize_managers(self):
        """Initialize any managers needed for success pattern analysis"""
        from task_context_manager import TaskContextManager
        from query_tree_manager import QueryTreeManager
        
        self.task_manager = TaskContextManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.pattern_repo = PatternRepositoryManager(self.memory)
    
    def _build_system_message(self) -> str:
        """Build the system message for success pattern analysis"""
        from prompts.success_pattern_prompts import SUCCESS_PATTERN_AGENT_PROMPT
        return SUCCESS_PATTERN_AGENT_PROMPT
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read complete context for success pattern analysis using memory managers"""
        
        try:
            # Get current node ID
            node_id = await self.tree_manager.get_current_node_id()
            if not node_id:
                self.logger.error("No current node ID found for success analysis")
                return {}
            
            # Get node with complete context
            node = await self.tree_manager.get_node(node_id)
            if not node or not node.evaluation:
                self.logger.error(f"No evaluation data found for node {node_id}")
                return {}
            
            # Get task context
            task_context = await self.task_manager.get()
            
            # Get existing rules for all agent types
            existing_rules = {}
            agent_types = ["schema_linker", "sql_generator", "query_analyzer"]
            
            for agent_type in agent_types:
                rules = await self.pattern_repo.get_rules_for_agent(agent_type)
                existing_rules[agent_type] = {
                    "do_rules": rules.get("do_rules", []),
                    "dont_rules": rules.get("dont_rules", []),
                    "rule_count": len(rules.get("do_rules", [])) + len(rules.get("dont_rules", []))
                }
            
            # Prepare comprehensive context for LLM
            context = {
                # Current successful execution details
                "original_query": node.intent,
                "evidence": node.evidence,
                "schema_linking_result": json.dumps(node.schema_linking, indent=2) if node.schema_linking else "{}",
                "generated_sql": node.generation.get("sql", "") if node.generation else "",
                "sql_explanation": node.generation.get("explanation", "") if node.generation else "",
                "execution_result": json.dumps(node.evaluation.get("execution_result", {}), indent=2),
                "evaluation_summary": json.dumps({
                    "answers_intent": node.evaluation.get("answers_intent", ""),
                    "result_quality": node.evaluation.get("result_quality", ""),
                    "confidence_score": node.evaluation.get("confidence_score", ""),
                    "result_summary": node.evaluation.get("result_summary", "")
                }, indent=2),
                
                # Existing rules context
                "existing_rules": json.dumps(existing_rules, indent=2),
                
                # Analysis metadata
                "node_id": node_id,
                "database_name": task_context.databaseName if task_context else "unknown",
                "analysis_timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"Success pattern analysis context prepared with {sum(r['rule_count'] for r in existing_rules.values())} existing rules")
            return context
            
        except Exception as e:
            self.logger.error(f"Error preparing success analysis context: {str(e)}", exc_info=True)
            return {}
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse success analysis results and update pattern repository"""
        if not result.messages:
            self.logger.warning("No messages in success analysis result")
            return
            
        last_message = result.messages[-1].content
        self.logger.info(f"Raw success analysis output: {last_message}")
        
        try:
            # Parse the XML analysis with fallback handling
            analysis_result = parse_xml_hybrid(last_message, 'success_analysis')
            
            if analysis_result:
                # Validate the analysis structure
                if self._validate_success_analysis(analysis_result):
                    # Update rules using PatternRepositoryManager
                    await self.pattern_repo.update_rules_from_success(analysis_result)
                    self.logger.info("Success pattern analysis completed and rules updated")
                else:
                    self.logger.warning("Success analysis structure validation failed")
            else:
                self.logger.warning("Failed to parse success analysis XML output")
                # Try manual extraction for critical parts
                manual_result = self._extract_success_rules_manually(last_message)
                if manual_result:
                    await self.pattern_repo.update_rules_from_success(manual_result)
                    self.logger.info("Success patterns extracted manually and rules updated")
                
        except Exception as e:
            self.logger.error(f"Error parsing success analysis results: {str(e)}", exc_info=True)
    
    def _validate_success_analysis(self, analysis: Dict[str, Any]) -> bool:
        """Validate that success analysis has the expected structure"""
        try:
            if not isinstance(analysis.get('agent_rules'), dict):
                return False
            
            agent_rules = analysis['agent_rules']
            expected_agents = ['schema_linker', 'sql_generator', 'query_analyzer']
            
            # Check that at least one agent has rules
            has_rules = False
            for agent in expected_agents:
                if agent in agent_rules and isinstance(agent_rules[agent], dict):
                    # Check if agent has any do_rule_* keys
                    for key in agent_rules[agent].keys():
                        if key.startswith('do_rule_'):
                            has_rules = True
                            break
            
            return has_rules
            
        except Exception as e:
            self.logger.warning(f"Error validating success analysis: {str(e)}")
            return False
    
    def _extract_success_rules_manually(self, output: str) -> Optional[Dict[str, Any]]:
        """Manual extraction when XML parsing fails"""
        try:
            # Extract DO rules using regex
            do_rules = re.findall(r'<do_rule_\d+>(.*?)</do_rule_\d+>', output, re.DOTALL)
            if do_rules:
                # Create minimal structure
                return {
                    'agent_rules': {
                        'general': {f'do_rule_{i+1}': rule.strip() for i, rule in enumerate(do_rules)}
                    }
                }
            return None
        except Exception as e:
            self.logger.warning(f"Manual extraction failed: {str(e)}")
            return None