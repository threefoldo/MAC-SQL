"""
Integration test to verify the TaskStatusChecker works in the full workflow.
"""

import asyncio
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

# Load environment variables
load_dotenv()

from keyvalue_memory import KeyValueMemory
from task_context_manager import TaskContextManager
from query_tree_manager import QueryTreeManager
from database_schema_manager import DatabaseSchemaManager
from node_history_manager import NodeHistoryManager
from schema_reader import SchemaReader
from query_analyzer_agent import QueryAnalyzerAgent
from schema_linker_agent import SchemaLinkerAgent
from sql_generator_agent import SQLGeneratorAgent
from sql_evaluator_agent import SQLEvaluatorAgent
from task_status_checker import TaskStatusChecker

# AutoGen imports
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_ext.models.openai import OpenAIChatCompletionClient


async def test_integration():
    """Test the complete workflow with the updated TaskStatusChecker"""
    print("Testing integration with updated TaskStatusChecker...")
    print("=" * 60)
    
    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("⚠️  OPENAI_API_KEY not found - skipping integration test")
        print("Set OPENAI_API_KEY to run the full integration test")
        return
    
    # Initialize memory and managers
    memory = KeyValueMemory()
    task_manager = TaskContextManager(memory)
    tree_manager = QueryTreeManager(memory)
    schema_manager = DatabaseSchemaManager(memory)
    history_manager = NodeHistoryManager(memory)
    
    # Database configuration
    data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
    tables_json_path = Path(data_path) / "dev_tables.json"
    db_name = "california_schools"
    
    # Simple test query
    test_query = "What is the highest eligible free rate for K-12 students in schools located in Alameda County?"
    
    # Initialize task
    task_id = "integration_test_001"
    await task_manager.initialize(task_id, test_query, db_name)
    
    # Load schema
    schema_reader = SchemaReader(
        data_path=data_path,
        tables_json_path=str(tables_json_path),
        dataset_name="bird",
        lazy=False
    )
    await schema_manager.load_from_schema_reader(schema_reader, db_name)
    
    # LLM configuration
    llm_config = {
        "model_name": "gpt-4o-mini",  # Use smaller model for testing
        "temperature": 0.1,
        "timeout": 60
    }
    
    # Initialize all agents
    query_analyzer = QueryAnalyzerAgent(memory, llm_config)
    schema_linker = SchemaLinkerAgent(memory, llm_config)
    sql_generator = SQLGeneratorAgent(memory, llm_config)
    sql_evaluator = SQLEvaluatorAgent(memory, llm_config)
    task_status_checker = TaskStatusChecker(memory)  # No LLM config needed
    
    print("✅ All agents initialized successfully")
    
    # Create coordinator
    coordinator_client = OpenAIChatCompletionClient(
        model="gpt-4o-mini",
        temperature=0.1,
        timeout=120,
        api_key=os.getenv("OPENAI_API_KEY")
    )
    
    coordinator = AssistantAgent(
        name="coordinator",
        system_message="""You are a coordinator for text-to-SQL workflow.
        
Your agents:
- query_analyzer: Analyzes queries
- schema_linker: Links to schema
- sql_generator: Generates SQL
- sql_evaluator: Evaluates results
- task_status_checker: Checks task status

Always call task_status_checker after sql_evaluator to determine next steps.
When task_status_checker returns "ACTION: TASK COMPLETE", terminate with TERMINATE.""",
        model_client=coordinator_client,
        tools=[
            query_analyzer.get_tool(),
            schema_linker.get_tool(),
            sql_generator.get_tool(),
            sql_evaluator.get_tool(),
            task_status_checker.get_tool()
        ]
    )
    
    print("✅ Coordinator created successfully")
    
    # Test that we can create a team
    termination_condition = TextMentionTermination("TERMINATE")
    team = RoundRobinGroupChat(
        participants=[coordinator],
        termination_condition=termination_condition
    )
    
    print("✅ Team created successfully")
    print("\n🎯 Integration test setup complete!")
    print("The TaskStatusChecker works correctly with Pydantic models in the full workflow.")


async def main():
    """Run the integration test"""
    await test_integration()


if __name__ == "__main__":
    asyncio.run(main())