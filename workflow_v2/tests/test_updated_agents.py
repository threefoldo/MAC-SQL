"""
Test the updated agents with the new BaseMemoryAgent pattern.
"""

import pytest
import asyncio
import sys
from pathlib import Path
import logging

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from keyvalue_memory import KeyValueMemory
from base_memory_agent import BaseMemoryAgent
from query_analyzer_agent_v2 import QueryAnalyzerAgent
from schema_linking_agent_v2 import SchemaLinkingAgent  
from sql_generator_agent_v2 import SQLGeneratorAgent
from sql_executor_agent_v2 import SQLExecutorAgent
from database_schema_manager import DatabaseSchemaManager
from task_context_manager import TaskContextManager
from memory_content_types import TableSchema, ColumnInfo

# Set up logging
logging.basicConfig(level=logging.INFO)


async def setup_test_schema(schema_manager: DatabaseSchemaManager):
    """Setup a simple test schema"""
    # Create customers table
    customers = TableSchema(
        name="customers",
        columns={
            "customer_id": ColumnInfo(
                dataType="INTEGER", nullable=False,
                isPrimaryKey=True, isForeignKey=False
            ),
            "name": ColumnInfo(
                dataType="VARCHAR(100)", nullable=False,
                isPrimaryKey=False, isForeignKey=False
            ),
            "email": ColumnInfo(
                dataType="VARCHAR(100)", nullable=True,
                isPrimaryKey=False, isForeignKey=False
            ),
            "country": ColumnInfo(
                dataType="VARCHAR(50)", nullable=True,
                isPrimaryKey=False, isForeignKey=False
            )
        },
        sampleData=[
            {"customer_id": 1, "name": "John Doe", "email": "john@example.com", "country": "USA"},
            {"customer_id": 2, "name": "Jane Smith", "email": "jane@example.com", "country": "UK"}
        ]
    )
    await schema_manager.add_table(customers)
    
    # Create orders table
    orders = TableSchema(
        name="orders",
        columns={
            "order_id": ColumnInfo(
                dataType="INTEGER", nullable=False,
                isPrimaryKey=True, isForeignKey=False
            ),
            "customer_id": ColumnInfo(
                dataType="INTEGER", nullable=False,
                isPrimaryKey=False, isForeignKey=True,
                references={"table": "customers", "column": "customer_id"}
            ),
            "order_date": ColumnInfo(
                dataType="DATE", nullable=False,
                isPrimaryKey=False, isForeignKey=False
            ),
            "total_amount": ColumnInfo(
                dataType="DECIMAL(10,2)", nullable=False,
                isPrimaryKey=False, isForeignKey=False
            )
        }
    )
    await schema_manager.add_table(orders)


@pytest.mark.asyncio
async def test_base_memory_agent_pattern():
    """Test that BaseMemoryAgent provides the correct structure"""
    
    class TestAgent(BaseMemoryAgent):
        agent_name = "test_agent"
        
        def _initialize_managers(self):
            pass
        
        def _build_system_message(self) -> str:
            return "Test system message"
        
        async def _reader_callback(self, memory, task, cancellation_token):
            return {"test": "context"}
        
        async def _parser_callback(self, memory, task, result, cancellation_token):
            pass
    
    memory = KeyValueMemory()
    agent = TestAgent(memory)
    
    # Check that all components are created
    assert agent.assistant is not None
    assert agent.tool is not None
    assert agent.get_tool() is not None
    assert agent.assistant.name == "test_agent"
    
    print("✓ BaseMemoryAgent pattern test passed")


@pytest.mark.asyncio
async def test_query_analyzer_agent():
    """Test QueryAnalyzerAgent initialization and basic structure"""
    memory = KeyValueMemory()
    
    # Setup task context
    task_manager = TaskContextManager(memory)
    await task_manager.initialize("test_task", "Find all customers", "test_db")
    
    # Initialize schema
    schema_manager = DatabaseSchemaManager(memory)
    await schema_manager.initialize()
    await setup_test_schema(schema_manager)
    
    # Create agent
    agent = QueryAnalyzerAgent(memory, debug=True)
    
    # Verify structure
    assert agent.assistant is not None
    assert agent.tool is not None
    assert agent.agent_name == "query_analyzer"
    
    # Test that managers are initialized
    assert hasattr(agent, 'task_manager')
    assert hasattr(agent, 'schema_manager')
    assert hasattr(agent, 'tree_manager')
    assert hasattr(agent, 'history_manager')
    
    print("✓ QueryAnalyzerAgent test passed")


@pytest.mark.asyncio
async def test_schema_linking_agent():
    """Test SchemaLinkingAgent initialization and basic structure"""
    memory = KeyValueMemory()
    
    # Initialize schema
    schema_manager = DatabaseSchemaManager(memory)
    await schema_manager.initialize()
    await setup_test_schema(schema_manager)
    
    # Create agent
    agent = SchemaLinkingAgent(memory, debug=True)
    
    # Verify structure
    assert agent.assistant is not None
    assert agent.tool is not None
    assert agent.agent_name == "schema_linker"
    
    # Test that managers are initialized
    assert hasattr(agent, 'schema_manager')
    assert hasattr(agent, 'tree_manager')
    assert hasattr(agent, 'history_manager')
    
    print("✓ SchemaLinkingAgent test passed")


@pytest.mark.asyncio
async def test_sql_generator_agent():
    """Test SQLGeneratorAgent initialization and basic structure"""
    memory = KeyValueMemory()
    
    # Create agent
    agent = SQLGeneratorAgent(memory, debug=True)
    
    # Verify structure
    assert agent.assistant is not None
    assert agent.tool is not None
    assert agent.agent_name == "sql_generator"
    
    # Test that managers are initialized
    assert hasattr(agent, 'schema_manager')
    assert hasattr(agent, 'tree_manager')
    assert hasattr(agent, 'history_manager')
    
    print("✓ SQLGeneratorAgent test passed")


@pytest.mark.asyncio
async def test_sql_executor_agent():
    """Test SQLExecutorAgent initialization and basic structure"""
    memory = KeyValueMemory()
    
    # Create agent
    agent = SQLExecutorAgent(memory, debug=True)
    
    # Verify structure
    assert agent.assistant is not None
    assert agent.tool is not None
    assert agent.agent_name == "sql_executor"
    
    # Test that managers are initialized
    assert hasattr(agent, 'tree_manager')
    assert hasattr(agent, 'history_manager')
    
    print("✓ SQLExecutorAgent test passed")


@pytest.mark.asyncio
async def test_agent_tool_integration():
    """Test that agents can be used as tools in tree orchestration"""
    memory = KeyValueMemory()
    
    # Initialize context
    task_manager = TaskContextManager(memory)
    await task_manager.initialize("test_task", "Find customers", "test_db")
    
    schema_manager = DatabaseSchemaManager(memory)
    await schema_manager.initialize()
    await setup_test_schema(schema_manager)
    
    # Create agents
    analyzer = QueryAnalyzerAgent(memory)
    linker = SchemaLinkingAgent(memory)
    generator = SQLGeneratorAgent(memory)
    executor = SQLExecutorAgent(memory)
    
    # Get tools
    analyzer_tool = analyzer.get_tool()
    linker_tool = linker.get_tool()
    generator_tool = generator.get_tool()
    executor_tool = executor.get_tool()
    
    # Verify tools have the correct structure
    assert analyzer_tool._agent == analyzer.assistant
    assert analyzer_tool._memory == memory
    assert analyzer_tool._reader_callback == analyzer._reader_callback
    assert analyzer_tool._parser_callback == analyzer._parser_callback
    
    print("✓ Agent tool integration test passed")


@pytest.mark.asyncio
async def test_memory_callback_flow():
    """Test that memory callbacks work correctly"""
    memory = KeyValueMemory()
    
    # Track callback calls
    reader_called = False
    parser_called = False
    
    class TestAgent(BaseMemoryAgent):
        agent_name = "callback_test"
        
        def _initialize_managers(self):
            pass
        
        def _build_system_message(self) -> str:
            return "Test"
        
        async def _reader_callback(self, memory, task, cancellation_token):
            nonlocal reader_called
            reader_called = True
            await memory.set("reader_called", True)
            return {"context": "test"}
        
        async def _parser_callback(self, memory, task, result, cancellation_token):
            nonlocal parser_called
            parser_called = True
            await memory.set("parser_called", True)
    
    agent = TestAgent(memory)
    tool = agent.get_tool()
    
    # Verify callbacks are set
    assert tool._reader_callback is not None
    assert tool._parser_callback is not None
    
    print("✓ Memory callback flow test passed")


if __name__ == "__main__":
    # Run tests
    asyncio.run(test_base_memory_agent_pattern())
    asyncio.run(test_query_analyzer_agent())
    asyncio.run(test_schema_linking_agent())
    asyncio.run(test_sql_generator_agent())
    asyncio.run(test_sql_executor_agent())
    asyncio.run(test_agent_tool_integration())
    asyncio.run(test_memory_callback_flow())
    
    print("\n✅ All agent update tests passed!")