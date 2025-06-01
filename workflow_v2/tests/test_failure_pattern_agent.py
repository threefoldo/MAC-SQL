"""
Test module for FailurePatternAgent with actual LLM calls.
"""

import pytest
import asyncio
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from keyvalue_memory import KeyValueMemory
from failure_pattern_agent import FailurePatternAgent
from pattern_repository_manager import PatternRepositoryManager
from memory_content_types import QueryNode, TaskContext, TaskStatus
from query_tree_manager import QueryTreeManager
from task_context_manager import TaskContextManager


class TestFailurePatternAgentWithLLM:
    """Test FailurePatternAgent with actual LLM calls"""
    
    @pytest.fixture
    def llm_config(self):
        """LLM configuration for testing"""
        # Check if OpenAI API key is available
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            pytest.skip("OPENAI_API_KEY not set - skipping LLM tests")
        
        return {
            "model": "gpt-4o",  # Use faster, cheaper model for testing
            "temperature": 0.0,
            "api_key": api_key
        }
    
    @pytest.fixture
    async def setup_memory_with_failure_case(self):
        """Setup memory with a failed execution case"""
        memory = KeyValueMemory()
        
        # Setup task context
        task_context = TaskContext(
            taskId="test_task_failure",
            originalQuery="Find schools with funding greater than 1000000",
            databaseName="california_schools",
            startTime="2024-01-01T00:00:00",
            status=TaskStatus.PROCESSING,
            evidence="Look for schools with high funding amounts in financial data."
        )
        await memory.set("taskContext", task_context.to_dict())
        
        # Setup failed execution node
        failure_node = QueryNode(
            nodeId="failure_node_1",
            intent="Find schools with funding greater than 1000000",
            evidence="Look for schools with high funding amounts in financial data.",
            schema_linking={
                "selected_tables": {
                    "table": [
                        {"name": "schools", "reason": "Contains basic school information"},
                        {"name": "frpm", "reason": "Contains funding data"}
                    ]
                },
                "key_columns": {
                    "schools": ["CDSCode", "School"],
                    "frpm": ["CDSCode", "Total_Funding"]
                },
                "relationships": [
                    {"from": "schools.CDSCode", "to": "frpm.CDSCode", "type": "one_to_one"}
                ]
            },
            generation={
                "sql": """
                SELECT s.School, f.Total_Funding
                FROM schools s 
                JOIN frpm f ON s.CDSCode = f.CDSCode 
                WHERE f.Total_Funding > 1000000
                ORDER BY f.Total_Funding DESC
                """.strip(),
                "explanation": "Join schools with funding data and filter for high funding amounts",
                "query_type": "filtering_with_join"
            },
            evaluation={
                "execution_result": {
                    "status": "error",
                    "error_message": "Column 'Total_Funding' doesn't exist in table 'frpm'",
                    "error_type": "column_not_found"
                },
                "answers_intent": "no",
                "result_quality": "poor",
                "confidence_score": 0.1,
                "result_summary": "SQL execution failed due to incorrect column name - 'Total_Funding' does not exist in frpm table",
                "issues": [
                    "Used non-existent column 'Total_Funding' instead of correct funding columns",
                    "Schema linking failed to identify actual funding column names in frpm table",
                    "Query cannot execute due to schema mismatch"
                ],
                "generator_context_review": {
                    "generator_reasoning": "Assumed standard naming convention for funding column without verifying actual schema",
                    "reasoning_validity": "invalid",
                    "context_notes": "Should have checked actual column names in frpm table for funding data"
                }
            }
        )
        
        # Store node in query tree with correct structure for QueryTreeManager
        await memory.set("queryTree", {
            "rootId": "failure_node_1",
            "currentNodeId": "failure_node_1",
            "nodes": {
                "failure_node_1": failure_node.to_dict()
            }
        })
        
        return memory
    
    @pytest.fixture
    async def setup_memory_with_logical_failure_case(self):
        """Setup memory with a logically incorrect execution case"""
        memory = KeyValueMemory()
        
        # Setup task context
        task_context = TaskContext(
            taskId="test_task_logical_failure",
            originalQuery="Find the school with the lowest test score average",
            databaseName="california_schools",
            startTime="2024-01-01T00:00:00",
            status=TaskStatus.PROCESSING,
            evidence="Calculate average of math and reading scores, find minimum."
        )
        await memory.set("taskContext", task_context.to_dict())
        
        # Setup logical failure node
        failure_node = QueryNode(
            nodeId="logical_failure_node_1",
            intent="Find the school with the lowest test score average",
            evidence="Calculate average of math and reading scores, find minimum.",
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
                       MIN(AVG((t.math_score + t.reading_score) / 2.0)) as min_avg_score
                FROM schools s 
                JOIN test_scores t ON s.school_id = t.school_id 
                GROUP BY s.school_id, s.school_name
                """.strip(),
                "explanation": "Use MIN with AVG to find school with lowest average score",
                "query_type": "aggregation_with_minimum"
            },
            evaluation={
                "execution_result": {
                    "status": "success",
                    "row_count": 1,
                    "columns": ["school_name", "min_avg_score"],
                    "data": [
                        ["Jefferson Middle School", 87.8]
                    ]
                },
                "answers_intent": "no",
                "result_quality": "poor",
                "confidence_score": 0.2,
                "result_summary": "Query executed but returned wrong result - MIN(AVG()) is invalid aggregation that doesn't find the school with lowest average",
                "issues": [
                    "Used MIN(AVG()) which is invalid SQL aggregation logic",
                    "Should use ORDER BY ASC LIMIT 1 instead of MIN(AVG())",
                    "Result shows incorrect aggregation behavior"
                ],
                "generator_context_review": {
                    "generator_reasoning": "Tried to use MIN() around AVG() thinking it would find minimum average per group",
                    "reasoning_validity": "invalid",
                    "context_notes": "MIN(AVG()) doesn't work as expected - should use simple ORDER BY with LIMIT"
                }
            }
        )
        
        # Store node in query tree
        await memory.set("queryTree", {
            "rootId": "logical_failure_node_1",
            "currentNodeId": "logical_failure_node_1",
            "nodes": {
                "logical_failure_node_1": failure_node.to_dict()
            }
        })
        
        return memory
    
    async def test_failure_pattern_agent_full_workflow(self, setup_memory_with_failure_case, llm_config):
        """Test complete FailurePatternAgent workflow with real LLM"""
        
        memory = setup_memory_with_failure_case
        
        # Initialize the pattern repository to check initial state
        pattern_repo = PatternRepositoryManager(memory)
        
        # Check initial state (should be empty)
        initial_schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        initial_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        initial_analyzer_rules = await pattern_repo.get_rules_for_agent("query_analyzer")
        
        print(f"Initial rule counts:")
        print(f"  schema_linker: {len(initial_schema_rules['do_rules'])} DO, {len(initial_schema_rules['dont_rules'])} DON'T")
        print(f"  sql_generator: {len(initial_sql_rules['do_rules'])} DO, {len(initial_sql_rules['dont_rules'])} DON'T")
        print(f"  query_analyzer: {len(initial_analyzer_rules['do_rules'])} DO, {len(initial_analyzer_rules['dont_rules'])} DON'T")
        
        # Create and run FailurePatternAgent
        failure_agent = FailurePatternAgent(memory, llm_config)
        
        print("\n" + "="*60)
        print("Running FailurePatternAgent with actual LLM...")
        print("="*60)
        
        # Run the agent
        result = await failure_agent.run(goal="analyze_failed_execution")
        
        print(f"\nAgent execution result: {result}")
        
        # Check that rules were added
        final_schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        final_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        final_analyzer_rules = await pattern_repo.get_rules_for_agent("query_analyzer")
        
        print(f"\nFinal rule counts:")
        print(f"  schema_linker: {len(final_schema_rules['do_rules'])} DO, {len(final_schema_rules['dont_rules'])} DON'T")
        print(f"  sql_generator: {len(final_sql_rules['do_rules'])} DO, {len(final_sql_rules['dont_rules'])} DON'T")
        print(f"  query_analyzer: {len(final_analyzer_rules['do_rules'])} DO, {len(final_analyzer_rules['dont_rules'])} DON'T")
        
        # Verify that DON'T rules were actually added
        total_initial_dont_rules = (
            len(initial_schema_rules['dont_rules']) +
            len(initial_sql_rules['dont_rules']) +
            len(initial_analyzer_rules['dont_rules'])
        )
        
        total_final_dont_rules = (
            len(final_schema_rules['dont_rules']) +
            len(final_sql_rules['dont_rules']) +
            len(final_analyzer_rules['dont_rules'])
        )
        
        print(f"\nTotal DON'T rules before: {total_initial_dont_rules}")
        print(f"Total DON'T rules after: {total_final_dont_rules}")
        print(f"DON'T rules added: {total_final_dont_rules - total_initial_dont_rules}")
        
        # Assertions
        assert total_final_dont_rules > total_initial_dont_rules, "No DON'T rules were added by the FailurePatternAgent"
        
        # Print the actual DON'T rules that were added
        print(f"\n" + "="*60)
        print("DON'T RULES ADDED BY FAILURE PATTERN AGENT")
        print("="*60)
        
        if final_schema_rules['dont_rules']:
            print(f"\nSchema Linker DON'T Rules:")
            for i, rule in enumerate(final_schema_rules['dont_rules'], 1):
                print(f"  {i}. {rule}")
        
        if final_sql_rules['dont_rules']:
            print(f"\nSQL Generator DON'T Rules:")
            for i, rule in enumerate(final_sql_rules['dont_rules'], 1):
                print(f"  {i}. {rule}")
        
        if final_analyzer_rules['dont_rules']:
            print(f"\nQuery Analyzer DON'T Rules:")
            for i, rule in enumerate(final_analyzer_rules['dont_rules'], 1):
                print(f"  {i}. {rule}")
        
        # Verify that we have DON'T rules for at least one agent type
        agents_with_dont_rules = 0
        if final_schema_rules['dont_rules']:
            agents_with_dont_rules += 1
        if final_sql_rules['dont_rules']:
            agents_with_dont_rules += 1
        if final_analyzer_rules['dont_rules']:
            agents_with_dont_rules += 1
        
        assert agents_with_dont_rules >= 1, f"Expected DON'T rules for at least 1 agent type, got {agents_with_dont_rules}"
        
        print(f"\n✓ Test passed: DON'T rules added for {agents_with_dont_rules} agent types")
    
    async def test_failure_pattern_context_reading(self, setup_memory_with_failure_case, llm_config):
        """Test that FailurePatternAgent reads context correctly for failed executions"""
        
        memory = setup_memory_with_failure_case
        failure_agent = FailurePatternAgent(memory, llm_config)
        
        # Test the reader callback directly
        context = await failure_agent._reader_callback(
            memory, 
            "analyze_failed_execution", 
            None
        )
        
        print(f"\n" + "="*60)
        print("FAILURE CONTEXT READING TEST")
        print("="*60)
        
        # Print context keys for debugging
        print(f"Context keys: {list(context.keys())}")
        
        # Verify all required context is present
        required_keys = [
            "original_query", "evidence", "schema_linking_result", 
            "generated_sql", "execution_result", "evaluation_summary",
            "existing_rules", "database_name"
        ]
        
        missing_keys = [key for key in required_keys if key not in context]
        assert not missing_keys, f"Missing required context keys: {missing_keys}"
        
        # Verify specific values for failure case
        assert context["original_query"] == "Find schools with funding greater than 1000000"
        assert context["database_name"] == "california_schools"
        assert "error" in context["execution_result"]
        assert "poor" in context["evaluation_summary"]
        
        print(f"✓ Failure context reading test passed")
        print(f"  - Original query: {context['original_query']}")
        print(f"  - Database: {context['database_name']}")
        print(f"  - Execution status: error")
        print(f"  - Quality: poor")
        
        # Verify error details are captured
        assert "Column 'Total_Funding' doesn't exist" in context["execution_result"]
        print(f"  - Error captured: Column existence error")
    
    async def test_logical_failure_pattern_analysis(self, setup_memory_with_logical_failure_case, llm_config):
        """Test FailurePatternAgent with logical error (not syntax error)"""
        
        memory = setup_memory_with_logical_failure_case
        failure_agent = FailurePatternAgent(memory, llm_config)
        pattern_repo = PatternRepositoryManager(memory)
        
        print(f"\n" + "="*60)
        print("LOGICAL FAILURE PATTERN ANALYSIS TEST")
        print("="*60)
        
        # Get initial rule counts
        initial_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        initial_dont_count = len(initial_sql_rules['dont_rules'])
        
        # Run failure analysis
        result = await failure_agent.run(goal="analyze_failed_execution")
        
        # Check that DON'T rules were added
        final_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        final_dont_count = len(final_sql_rules['dont_rules'])
        
        print(f"SQL Generator DON'T rules before: {initial_dont_count}")
        print(f"SQL Generator DON'T rules after: {final_dont_count}")
        
        # Should have added DON'T rules for SQL generation mistakes
        assert final_dont_count > initial_dont_count, "No DON'T rules added for logical SQL error"
        
        # Print the rules that were added
        if final_sql_rules['dont_rules']:
            print(f"\nSQL Generator DON'T Rules Added:")
            for i, rule in enumerate(final_sql_rules['dont_rules'], 1):
                print(f"  {i}. {rule}")
        
        print(f"\n✓ Logical failure analysis test passed")
    
    async def test_pattern_repository_manager_failure_integration(self, setup_memory_with_failure_case):
        """Test PatternRepositoryManager DON'T rule functionality"""
        
        memory = setup_memory_with_failure_case
        pattern_repo = PatternRepositoryManager(memory)
        
        print(f"\n" + "="*60)
        print("PATTERN REPOSITORY FAILURE INTEGRATION TEST")
        print("="*60)
        
        # Test adding DON'T rules manually
        await pattern_repo.add_dont_rule("schema_linker", "Test DON'T rule for schema linking failures")
        await pattern_repo.add_dont_rule("sql_generator", "Test DON'T rule for SQL generation errors")
        
        # Verify DON'T rules were added
        schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        
        assert "Test DON'T rule for schema linking failures" in schema_rules["dont_rules"]
        assert "Test DON'T rule for SQL generation errors" in sql_rules["dont_rules"]
        
        # Test DON'T rule formatting for prompts
        schema_formatted = await pattern_repo.format_rules_for_prompt("schema_linker")
        sql_formatted = await pattern_repo.format_rules_for_prompt("sql_generator")
        
        assert "❌ DON'T:" in schema_formatted
        assert "❌ DON'T:" in sql_formatted
        assert "Test DON'T rule for schema linking failures" in schema_formatted
        assert "Test DON'T rule for SQL generation errors" in sql_formatted
        
        # Test failure analysis update
        mock_failure_analysis = {
            "agent_rules": {
                "schema_linker": {
                    "dont_rule_1": "DON'T use non-existent columns",
                    "dont_rule_2": "DON'T assume column names without verification"
                },
                "sql_generator": {
                    "dont_rule_1": "DON'T use MIN(AVG()) for finding minimum averages"
                }
            }
        }
        
        await pattern_repo.update_rules_from_failure(mock_failure_analysis)
        
        # Verify the mock rules were added
        updated_schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        updated_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        
        assert "DON'T use non-existent columns" in updated_schema_rules["dont_rules"]
        assert "DON'T assume column names without verification" in updated_schema_rules["dont_rules"] 
        assert "DON'T use MIN(AVG()) for finding minimum averages" in updated_sql_rules["dont_rules"]
        
        print(f"✓ PatternRepositoryManager failure integration test passed")
        print(f"  - Added and retrieved DON'T rules successfully")
        print(f"  - DON'T rule formatting works correctly")
        print(f"  - Failure analysis integration works correctly")


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
    test_class = TestFailurePatternAgentWithLLM()
    
    async def run_tests():
        failure_memory = await test_class.setup_memory_with_failure_case()
        logical_failure_memory = await test_class.setup_memory_with_logical_failure_case()
        
        print("Running FailurePatternAgent tests with real LLM...")
        
        await test_class.test_pattern_repository_manager_failure_integration(failure_memory)
        await test_class.test_failure_pattern_context_reading(failure_memory, llm_config)
        await test_class.test_logical_failure_pattern_analysis(logical_failure_memory, llm_config)
        await test_class.test_failure_pattern_agent_full_workflow(failure_memory, llm_config)
        
        print("\n🎉 All failure pattern tests completed successfully!")
    
    asyncio.run(run_tests())