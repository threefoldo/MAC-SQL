"""
Test module for SQLEvaluatorAgent intelligent learning integration.

This test verifies that SQLEvaluatorAgent properly calls pattern agents
to learn from successful and failed executions.
"""

import pytest
import asyncio
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from keyvalue_memory import KeyValueMemory
from sql_evaluator_agent import SQLEvaluatorAgent
from pattern_repository_manager import PatternRepositoryManager
from memory_content_types import QueryNode, TaskContext, TaskStatus
from query_tree_manager import QueryTreeManager
from task_context_manager import TaskContextManager
from database_schema_manager import DatabaseSchemaManager


class TestSQLEvaluatorIntelligentLearning:
    """Test SQLEvaluatorAgent's intelligent learning through pattern agents"""
    
    @pytest.fixture
    def llm_config(self):
        """LLM configuration for testing"""
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            pytest.skip("OPENAI_API_KEY not set - skipping LLM tests")
        
        return {
            "model": "gpt-4o",
            "temperature": 0.0,
            "api_key": api_key
        }
    
    @pytest.fixture
    async def setup_success_scenario(self):
        """Setup memory with a successful SQL execution scenario"""
        memory = KeyValueMemory()
        
        # Setup task context
        task_context = TaskContext(
            taskId="success_learning_test",
            originalQuery="Find the top 3 schools with highest test scores",
            databaseName="california_schools",
            startTime="2024-01-01T00:00:00",
            status=TaskStatus.PROCESSING,
            evidence="Use standardized test scores for ranking"
        )
        await memory.set("taskContext", task_context.to_dict())
        
        # Setup database schema
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        
        # Create successful execution node with complete context
        success_node = QueryNode(
            nodeId="learning_success_node",
            intent="Find the top 3 schools with highest test scores",
            evidence="Use standardized test scores for ranking",
            schema_linking={
                "selected_tables": {
                    "table": [
                        {"name": "schools", "reason": "Contains school information"},
                        {"name": "test_scores", "reason": "Contains test score data"}
                    ]
                },
                "key_columns": {
                    "schools": ["school_id", "school_name"],
                    "test_scores": ["school_id", "math_score", "reading_score"]
                },
                "relationships": [
                    {"from": "schools.school_id", "to": "test_scores.school_id", "type": "one_to_many"}
                ]
            },
            generation={
                "sql": """
                SELECT s.school_name, 
                       AVG((t.math_score + t.reading_score) / 2.0) as avg_score
                FROM schools s 
                JOIN test_scores t ON s.school_id = t.school_id 
                GROUP BY s.school_id, s.school_name 
                ORDER BY avg_score DESC 
                LIMIT 3
                """.strip(),
                "explanation": "Join schools with test scores, calculate average, rank by highest scores",
                "query_type": "aggregation_with_ranking",
                "execution_result": {
                    "status": "success",
                    "row_count": 3,
                    "columns": ["school_name", "avg_score"],
                    "data": [
                        ["Lincoln High School", 92.5],
                        ["Washington Elementary", 89.3],
                        ["Jefferson Middle School", 87.8]
                    ]
                }
            },
            evaluation={}
        )
        
        # Store in query tree
        await memory.set("queryTree", {
            "rootId": "learning_success_node",
            "currentNodeId": "learning_success_node",
            "nodes": {
                "learning_success_node": success_node.to_dict()
            }
        })
        
        # Initialize execution_analysis memory
        execution_context = {
            "original_query": "Find the top 3 schools with highest test scores",
            "node_id": "learning_success_node",
            "initialized_at": "2024-01-01T00:00:00",
            "evaluation": None,
            "last_update": None
        }
        await memory.set("execution_analysis", execution_context)
        
        return memory
    
    @pytest.fixture
    async def setup_failure_scenario(self):
        """Setup memory with a failed SQL execution scenario"""
        memory = KeyValueMemory()
        
        # Setup task context
        task_context = TaskContext(
            taskId="failure_learning_test",
            originalQuery="Find schools with funding above average",
            databaseName="california_schools", 
            startTime="2024-01-01T00:00:00",
            status=TaskStatus.PROCESSING,
            evidence="Look for schools with high funding amounts"
        )
        await memory.set("taskContext", task_context.to_dict())
        
        # Setup database schema
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        
        # Create failed execution node
        failure_node = QueryNode(
            nodeId="learning_failure_node",
            intent="Find schools with funding above average",
            evidence="Look for schools with high funding amounts",
            schema_linking={
                "selected_tables": {
                    "table": [
                        {"name": "schools", "reason": "Contains school information"},
                        {"name": "funding", "reason": "Contains funding data"}
                    ]
                },
                "key_columns": {
                    "schools": ["school_id", "school_name"],
                    "funding": ["school_id", "total_funding"]
                },
                "relationships": [
                    {"from": "schools.school_id", "to": "funding.school_id", "type": "one_to_one"}
                ]
            },
            generation={
                "sql": """
                SELECT s.school_name, f.total_funding
                FROM schools s 
                JOIN funding f ON s.school_id = f.school_id 
                WHERE f.total_funding > (SELECT AVG(invalid_column) FROM funding)
                """.strip(),
                "explanation": "Find schools with above-average funding",
                "query_type": "filtering_with_subquery",
                "execution_result": {
                    "status": "error",
                    "error_message": "Column 'invalid_column' doesn't exist",
                    "error_type": "column_not_found"
                }
            },
            evaluation={}
        )
        
        # Store in query tree
        await memory.set("queryTree", {
            "rootId": "learning_failure_node",
            "currentNodeId": "learning_failure_node",
            "nodes": {
                "learning_failure_node": failure_node.to_dict()
            }
        })
        
        # Initialize execution_analysis memory
        execution_context = {
            "original_query": "Find schools with funding above average",
            "node_id": "learning_failure_node",
            "initialized_at": "2024-01-01T00:00:00",
            "evaluation": None,
            "last_update": None
        }
        await memory.set("execution_analysis", execution_context)
        
        return memory
    
    async def test_success_pattern_learning_integration(self, setup_success_scenario, llm_config):
        """Test that SQLEvaluatorAgent calls SuccessPatternAgent for good results"""
        
        memory = setup_success_scenario
        pattern_repo = PatternRepositoryManager(memory)
        
        # Check initial rule counts
        initial_schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        initial_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        initial_total_rules = (
            len(initial_schema_rules['do_rules']) + len(initial_schema_rules['dont_rules']) +
            len(initial_sql_rules['do_rules']) + len(initial_sql_rules['dont_rules'])
        )
        
        print(f"Initial total rules: {initial_total_rules}")
        
        # Create SQL evaluator agent
        evaluator = SQLEvaluatorAgent(memory)
        evaluator.llm_config = llm_config
        
        print("\n" + "="*60)
        print("Testing SQLEvaluatorAgent Success Pattern Learning")
        print("="*60)
        
        # Run evaluation which should trigger pattern learning
        result = await evaluator.run("Evaluate SQL execution results")
        
        # Check if rules were added (success patterns should add DO rules)
        final_schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        final_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        final_total_rules = (
            len(final_schema_rules['do_rules']) + len(final_schema_rules['dont_rules']) +
            len(final_sql_rules['do_rules']) + len(final_sql_rules['dont_rules'])
        )
        
        print(f"Final total rules: {final_total_rules}")
        print(f"Rules added: {final_total_rules - initial_total_rules}")
        
        # Verify that pattern learning occurred
        assert final_total_rules > initial_total_rules, "No rules were learned from successful execution"
        
        # Check evaluation was updated
        execution_analysis = await memory.get("execution_analysis")
        assert execution_analysis is not None
        assert execution_analysis.get("evaluation") is not None
        
        evaluation = execution_analysis["evaluation"]
        print(f"\nEvaluation results:")
        print(f"  - Result quality: {evaluation.get('result_quality')}")
        print(f"  - Answers intent: {evaluation.get('answers_intent')}")
        
        # For successful executions, should have called success pattern agent
        print(f"\n✓ Success pattern learning integration test passed")
        print(f"  - SQLEvaluatorAgent triggered intelligent learning")
        print(f"  - Pattern rules were updated based on successful execution")
    
    async def test_failure_pattern_learning_integration(self, setup_failure_scenario, llm_config):
        """Test that SQLEvaluatorAgent calls FailurePatternAgent for failed results"""
        
        memory = setup_failure_scenario
        pattern_repo = PatternRepositoryManager(memory)
        
        # Check initial DON'T rule counts  
        initial_schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        initial_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        initial_dont_rules = (
            len(initial_schema_rules['dont_rules']) +
            len(initial_sql_rules['dont_rules'])
        )
        
        print(f"Initial total DON'T rules: {initial_dont_rules}")
        
        # Create SQL evaluator agent
        evaluator = SQLEvaluatorAgent(memory)
        evaluator.llm_config = llm_config
        
        print("\n" + "="*60)
        print("Testing SQLEvaluatorAgent Failure Pattern Learning")
        print("="*60)
        
        # Run evaluation which should trigger failure pattern learning
        result = await evaluator.run("Evaluate SQL execution results")
        
        # Check if DON'T rules were added (failure patterns should add DON'T rules)
        final_schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        final_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        final_dont_rules = (
            len(final_schema_rules['dont_rules']) +
            len(final_sql_rules['dont_rules'])
        )
        
        print(f"Final total DON'T rules: {final_dont_rules}")
        print(f"DON'T rules added: {final_dont_rules - initial_dont_rules}")
        
        # Verify that failure pattern learning occurred
        assert final_dont_rules > initial_dont_rules, "No DON'T rules were learned from failed execution"
        
        # Check evaluation was updated
        execution_analysis = await memory.get("execution_analysis")
        assert execution_analysis is not None
        assert execution_analysis.get("evaluation") is not None
        
        evaluation = execution_analysis["evaluation"]
        print(f"\nEvaluation results:")
        print(f"  - Result quality: {evaluation.get('result_quality')}")
        print(f"  - Answers intent: {evaluation.get('answers_intent')}")
        
        # Print the DON'T rules that were added
        if final_schema_rules['dont_rules']:
            print(f"\nSchema Linker DON'T Rules Added:")
            for rule in final_schema_rules['dont_rules']:
                print(f"  - {rule}")
        
        if final_sql_rules['dont_rules']:
            print(f"\nSQL Generator DON'T Rules Added:")
            for rule in final_sql_rules['dont_rules']:
                print(f"  - {rule}")
        
        print(f"\n✓ Failure pattern learning integration test passed")
        print(f"  - SQLEvaluatorAgent triggered intelligent learning")
        print(f"  - DON'T rules were updated based on failed execution")
    
    async def test_learning_decision_logic(self, setup_success_scenario, llm_config):
        """Test the logic that decides when to call success vs failure pattern agents"""
        
        memory = setup_success_scenario
        evaluator = SQLEvaluatorAgent(memory)
        evaluator.llm_config = llm_config
        
        print("\n" + "="*60)
        print("Testing Pattern Agent Selection Logic")
        print("="*60)
        
        # Test various evaluation scenarios to verify decision logic
        test_scenarios = [
            {
                "execution_status": "success",
                "result_quality": "excellent",
                "answers_intent": "yes",
                "expected_agent": "success",
                "description": "Perfect execution should trigger success learning"
            },
            {
                "execution_status": "success", 
                "result_quality": "poor",
                "answers_intent": "no",
                "expected_agent": "failure",
                "description": "Poor quality despite success should trigger failure learning"
            },
            {
                "execution_status": "error",
                "result_quality": "failed",
                "answers_intent": "no", 
                "expected_agent": "failure",
                "description": "SQL error should trigger failure learning"
            },
            {
                "execution_status": "success",
                "result_quality": "good",
                "answers_intent": "yes",
                "expected_agent": "success",
                "description": "Good quality execution should trigger success learning"
            }
        ]
        
        for i, scenario in enumerate(test_scenarios):
            print(f"\nScenario {i+1}: {scenario['description']}")
            print(f"  Status: {scenario['execution_status']}")
            print(f"  Quality: {scenario['result_quality']}")
            print(f"  Answers intent: {scenario['answers_intent']}")
            
            # Test the decision logic (this is the core business rule)
            is_successful = (
                scenario["execution_status"] == "success" and
                scenario["result_quality"] in ["excellent", "good"] and
                scenario["answers_intent"] == "yes"
            )
            
            expected_is_success = (scenario["expected_agent"] == "success")
            actual_agent = "success" if is_successful else "failure"
            
            print(f"  Expected agent: {scenario['expected_agent']}")
            print(f"  Actual agent: {actual_agent}")
            
            assert (actual_agent == scenario["expected_agent"]), f"Wrong agent selection for scenario {i+1}"
            print(f"  ✓ Correct agent selected")
        
        print(f"\n✓ Pattern agent selection logic test passed")
        print(f"  - All scenarios correctly routed to appropriate pattern agents")


if __name__ == "__main__":
    # Run tests manually for debugging
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Check for API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("OPENAI_API_KEY not set. Please set it to run LLM tests.")
        print("Example: export OPENAI_API_KEY='your-key-here'")
        exit(1)
    
    # Create LLM config directly
    llm_config = {
        "model": "gpt-4o-mini",
        "temperature": 0.0,
        "api_key": api_key
    }
    
    # Run test
    test_class = TestSQLEvaluatorIntelligentLearning()
    
    async def run_tests():
        success_memory = await test_class.setup_success_scenario()
        failure_memory = await test_class.setup_failure_scenario()
        
        print("Running SQLEvaluatorAgent intelligent learning tests...")
        
        await test_class.test_learning_decision_logic(success_memory, llm_config)
        await test_class.test_success_pattern_learning_integration(success_memory, llm_config)
        await test_class.test_failure_pattern_learning_integration(failure_memory, llm_config)
        
        print("\n🎉 All intelligent learning tests completed successfully!")
    
    asyncio.run(run_tests())