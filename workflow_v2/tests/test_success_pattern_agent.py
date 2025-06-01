"""
Test module for SuccessPatternAgent with actual LLM calls.
"""

import pytest
import asyncio
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from keyvalue_memory import KeyValueMemory
from success_pattern_agent import SuccessPatternAgent
from pattern_repository_manager import PatternRepositoryManager
from memory_content_types import QueryNode, TaskContext, TaskStatus
from query_tree_manager import QueryTreeManager
from task_context_manager import TaskContextManager


class TestSuccessPatternAgentWithLLM:
    """Test SuccessPatternAgent with actual LLM calls"""
    
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
    async def setup_memory_with_success_case(self):
        """Setup memory with a successful execution case"""
        memory = KeyValueMemory()
        
        # Setup task context
        task_context = TaskContext(
            taskId="test_task_success",
            originalQuery="Find top 3 schools with highest average test scores",
            databaseName="california_schools",
            startTime="2024-01-01T00:00:00",
            status=TaskStatus.PROCESSING,
            evidence="Use math and reading test scores. Calculate average per school."
        )
        await memory.set("taskContext", task_context.to_dict())
        
        # Setup successful execution node
        success_node = QueryNode(
            nodeId="success_node_1",
            intent="Find top 3 schools with highest average test scores",
            evidence="Use math and reading test scores. Calculate average per school.",
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
                "explanation": "Join schools with test scores, calculate average of math and reading scores per school, order by highest average, limit to top 3",
                "query_type": "aggregation_with_ranking"
            },
            evaluation={
                "execution_result": {
                    "status": "success",
                    "row_count": 3,
                    "columns": ["school_name", "avg_score"],
                    "data": [
                        ["Lincoln High School", 92.5],
                        ["Washington Elementary", 89.3],
                        ["Jefferson Middle School", 87.8]
                    ]
                },
                "answers_intent": "yes",
                "result_quality": "excellent",
                "confidence_score": 0.95,
                "result_summary": "Successfully identified top 3 schools with highest average test scores using proper aggregation and ranking",
                "generator_context_review": {
                    "generator_reasoning": "Used JOIN to combine school and test score data, calculated average across math and reading scores, applied proper GROUP BY and ORDER BY with LIMIT",
                    "reasoning_validity": "valid",
                    "context_notes": "Correctly interpreted evidence to calculate composite average score"
                }
            }
        )
        
        # Store node in query tree with correct structure for QueryTreeManager
        await memory.set("queryTree", {
            "rootId": "success_node_1",
            "currentNodeId": "success_node_1",
            "nodes": {
                "success_node_1": success_node.to_dict()
            }
        })
        
        return memory
    
    async def test_success_pattern_agent_full_workflow(self, setup_memory_with_success_case, llm_config):
        """Test complete SuccessPatternAgent workflow with real LLM"""
        
        memory = setup_memory_with_success_case
        
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
        
        # Create and run SuccessPatternAgent
        success_agent = SuccessPatternAgent(memory, llm_config)
        
        print("\n" + "="*60)
        print("Running SuccessPatternAgent with actual LLM...")
        print("="*60)
        
        # Run the agent
        result = await success_agent.run(goal="analyze_successful_execution")
        
        print(f"\nAgent execution result: {result}")
        
        # Check that rules were added
        final_schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        final_sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        final_analyzer_rules = await pattern_repo.get_rules_for_agent("query_analyzer")
        
        print(f"\nFinal rule counts:")
        print(f"  schema_linker: {len(final_schema_rules['do_rules'])} DO, {len(final_schema_rules['dont_rules'])} DON'T")
        print(f"  sql_generator: {len(final_sql_rules['do_rules'])} DO, {len(final_sql_rules['dont_rules'])} DON'T")
        print(f"  query_analyzer: {len(final_analyzer_rules['do_rules'])} DO, {len(final_analyzer_rules['dont_rules'])} DON'T")
        
        # Verify that rules were actually added
        total_initial_rules = (
            len(initial_schema_rules['do_rules']) + len(initial_schema_rules['dont_rules']) +
            len(initial_sql_rules['do_rules']) + len(initial_sql_rules['dont_rules']) +
            len(initial_analyzer_rules['do_rules']) + len(initial_analyzer_rules['dont_rules'])
        )
        
        total_final_rules = (
            len(final_schema_rules['do_rules']) + len(final_schema_rules['dont_rules']) +
            len(final_sql_rules['do_rules']) + len(final_sql_rules['dont_rules']) +
            len(final_analyzer_rules['do_rules']) + len(final_analyzer_rules['dont_rules'])
        )
        
        print(f"\nTotal rules before: {total_initial_rules}")
        print(f"Total rules after: {total_final_rules}")
        print(f"Rules added: {total_final_rules - total_initial_rules}")
        
        # Assertions
        assert total_final_rules > total_initial_rules, "No rules were added by the SuccessPatternAgent"
        
        # Print the actual rules that were added
        print(f"\n" + "="*60)
        print("RULES ADDED BY SUCCESS PATTERN AGENT")
        print("="*60)
        
        if final_schema_rules['do_rules']:
            print(f"\nSchema Linker DO Rules:")
            for i, rule in enumerate(final_schema_rules['do_rules'], 1):
                print(f"  {i}. {rule}")
        
        if final_sql_rules['do_rules']:
            print(f"\nSQL Generator DO Rules:")
            for i, rule in enumerate(final_sql_rules['do_rules'], 1):
                print(f"  {i}. {rule}")
        
        if final_analyzer_rules['do_rules']:
            print(f"\nQuery Analyzer DO Rules:")
            for i, rule in enumerate(final_analyzer_rules['do_rules'], 1):
                print(f"  {i}. {rule}")
        
        # Verify that we have rules for multiple agent types
        agents_with_rules = 0
        if final_schema_rules['do_rules']:
            agents_with_rules += 1
        if final_sql_rules['do_rules']:
            agents_with_rules += 1
        if final_analyzer_rules['do_rules']:
            agents_with_rules += 1
        
        assert agents_with_rules >= 2, f"Expected rules for at least 2 agent types, got {agents_with_rules}"
        
        print(f"\n✓ Test passed: Rules added for {agents_with_rules} agent types")
    
    async def test_pattern_repository_context_reading(self, setup_memory_with_success_case, llm_config):
        """Test that SuccessPatternAgent reads context correctly"""
        
        memory = setup_memory_with_success_case
        success_agent = SuccessPatternAgent(memory, llm_config)
        
        # Test the reader callback directly
        context = await success_agent._reader_callback(
            memory, 
            "analyze_successful_execution", 
            None
        )
        
        print(f"\n" + "="*60)
        print("CONTEXT READING TEST")
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
        
        # Verify specific values
        assert context["original_query"] == "Find top 3 schools with highest average test scores"
        assert context["database_name"] == "california_schools"
        assert "success" in context["execution_result"]
        assert "excellent" in context["evaluation_summary"]
        
        print(f"✓ Context reading test passed")
        print(f"  - Original query: {context['original_query']}")
        print(f"  - Database: {context['database_name']}")
        print(f"  - Execution status: success")
        print(f"  - Quality: excellent")
    
    async def test_pattern_repository_manager_integration(self, setup_memory_with_success_case):
        """Test PatternRepositoryManager functionality"""
        
        memory = setup_memory_with_success_case
        pattern_repo = PatternRepositoryManager(memory)
        
        print(f"\n" + "="*60)
        print("PATTERN REPOSITORY MANAGER TEST")
        print("="*60)
        
        # Test adding rules manually
        await pattern_repo.add_do_rule("schema_linker", "Test DO rule for schema linking")
        await pattern_repo.add_dont_rule("sql_generator", "Test DON'T rule for SQL generation")
        
        # Verify rules were added
        schema_rules = await pattern_repo.get_rules_for_agent("schema_linker")
        sql_rules = await pattern_repo.get_rules_for_agent("sql_generator")
        
        assert "Test DO rule for schema linking" in schema_rules["do_rules"]
        assert "Test DON'T rule for SQL generation" in sql_rules["dont_rules"]
        
        # Test rule formatting for prompts
        schema_formatted = await pattern_repo.format_rules_for_prompt("schema_linker")
        sql_formatted = await pattern_repo.format_rules_for_prompt("sql_generator")
        
        assert "✅ DO:" in schema_formatted
        assert "❌ DON'T:" in sql_formatted
        assert "Test DO rule for schema linking" in schema_formatted
        assert "Test DON'T rule for SQL generation" in sql_formatted
        
        print(f"✓ PatternRepositoryManager test passed")
        print(f"  - Added and retrieved DO/DON'T rules successfully")
        print(f"  - Rule formatting works correctly")


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
    test_class = TestSuccessPatternAgentWithLLM()
    
    async def run_tests():
        memory = await test_class.setup_memory_with_success_case()
        
        print("Running SuccessPatternAgent tests with real LLM...")
        
        await test_class.test_pattern_repository_manager_integration(memory)
        await test_class.test_pattern_repository_context_reading(memory, llm_config)
        await test_class.test_success_pattern_agent_full_workflow(memory, llm_config)
        
        print("\n🎉 All tests completed successfully!")
    
    asyncio.run(run_tests())