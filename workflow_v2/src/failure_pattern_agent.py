"""
Failure Pattern Agent for text-to-SQL intelligent learning.

This agent analyzes failed SQL executions and maintains a repository
of failure patterns for future avoidance guidance.
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from utils import parse_xml_hybrid
from pattern_repository_manager import PatternRepositoryManager


class FailurePatternAgent(BaseMemoryAgent):
    """
    Analyzes FAILED executions (POOR/BAD quality) and generates DON'T rules.
    
    This agent is ONLY called when:
    - SQL execution failed with errors OR
    - Evaluation quality is POOR or BAD OR
    - Query does not correctly answer the intended question
    
    It generates DON'T rules that capture failure patterns to avoid in future.
    """
    
    agent_name = "failure_pattern_agent"
    
    def _initialize_managers(self):
        """Initialize any managers needed for failure pattern analysis"""
        from task_context_manager import TaskContextManager
        from query_tree_manager import QueryTreeManager
        
        self.task_manager = TaskContextManager(self.memory)
        self.tree_manager = QueryTreeManager(self.memory)
        self.pattern_repo = PatternRepositoryManager(self.memory)
    
    def _build_system_message(self) -> str:
        """Build the system message for failure pattern analysis"""
        from prompts.failure_pattern_prompts import FAILURE_PATTERN_AGENT_PROMPT
        return FAILURE_PATTERN_AGENT_PROMPT
    
    async def _reader_callback(self, memory: KeyValueMemory, task: str, cancellation_token) -> Dict[str, Any]:
        """Read complete context for failure pattern analysis using memory managers"""
        
        try:
            # Get current node ID
            node_id = await self.tree_manager.get_current_node_id()
            if not node_id:
                self.logger.error("No current node ID found for failure analysis")
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
                # Current failed execution details
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
                    "result_summary": node.evaluation.get("result_summary", ""),
                    "issues": node.evaluation.get("issues", [])
                }, indent=2),
                
                # Existing rules context
                "existing_rules": json.dumps(existing_rules, indent=2),
                
                # Analysis metadata
                "node_id": node_id,
                "database_name": task_context.databaseName if task_context else "unknown",
                "analysis_timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"Failure pattern analysis context prepared with {sum(r['rule_count'] for r in existing_rules.values())} existing rules")
            return context
            
        except Exception as e:
            self.logger.error(f"Error preparing failure analysis context: {str(e)}", exc_info=True)
            return {}
    
    async def _parser_callback(self, memory: KeyValueMemory, task: str, result, cancellation_token) -> None:
        """Parse failure analysis results and update pattern repository"""
        if not result.messages:
            self.logger.warning("No messages in failure analysis result")
            return
            
        last_message = result.messages[-1].content
        self.logger.info(f"Raw failure analysis output: {last_message}")
        
        try:
            # Parse the XML analysis with fallback handling
            analysis_result = parse_xml_hybrid(last_message, 'failure_analysis')
            
            if analysis_result:
                # Validate the analysis structure
                if self._validate_failure_analysis(analysis_result):
                    # Update rules using PatternRepositoryManager
                    await self.pattern_repo.update_rules_from_failure(analysis_result)
                    self.logger.info("Failure pattern analysis completed and rules updated")
                else:
                    self.logger.warning("Failure analysis structure validation failed")
            else:
                self.logger.warning("Failed to parse failure analysis XML output")
                # Try manual extraction for critical parts
                manual_result = self._extract_failure_rules_manually(last_message)
                if manual_result:
                    await self.pattern_repo.update_rules_from_failure(manual_result)
                    self.logger.info("Failure patterns extracted manually and rules updated")
                
        except Exception as e:
            self.logger.error(f"Error parsing failure analysis results: {str(e)}", exc_info=True)
    
    def _validate_failure_analysis(self, analysis: Dict[str, Any]) -> bool:
        """Validate that failure analysis has the expected structure"""
        try:
            if not isinstance(analysis.get('agent_rules'), dict):
                return False
            
            agent_rules = analysis['agent_rules']
            expected_agents = ['schema_linker', 'sql_generator', 'query_analyzer']
            
            # Check that at least one agent has rules
            has_rules = False
            for agent in expected_agents:
                if agent in agent_rules and isinstance(agent_rules[agent], dict):
                    # Check if agent has any dont_rule_* keys
                    for key in agent_rules[agent].keys():
                        if key.startswith('dont_rule_'):
                            has_rules = True
                            break
            
            return has_rules
            
        except Exception as e:
            self.logger.warning(f"Error validating failure analysis: {str(e)}")
            return False
    
    def _extract_failure_rules_manually(self, output: str) -> Optional[Dict[str, Any]]:
        """Manual extraction when XML parsing fails"""
        try:
            # Extract DON'T rules using regex
            dont_rules = re.findall(r'<dont_rule_\d+>(.*?)</dont_rule_\d+>', output, re.DOTALL)
            if dont_rules:
                # Create minimal structure
                return {
                    'agent_rules': {
                        'general': {f'dont_rule_{i+1}': rule.strip() for i, rule in enumerate(dont_rules)}
                    }
                }
            return None
        except Exception as e:
            self.logger.warning(f"Manual extraction failed: {str(e)}")
            return None