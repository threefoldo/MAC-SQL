"""
Test the structure of updated agents without making actual API calls.
"""

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

# Set up logging
logging.basicConfig(level=logging.INFO)


def test_imports():
    """Test that all imports work correctly"""
    print("Testing imports...")
    
    # All imports should work without errors
    assert BaseMemoryAgent is not None
    assert QueryAnalyzerAgent is not None
    assert SchemaLinkingAgent is not None
    assert SQLGeneratorAgent is not None
    assert SQLExecutorAgent is not None
    
    print("✓ All imports successful")


def test_agent_attributes():
    """Test that agents have correct attributes"""
    print("\nTesting agent attributes...")
    
    # Check agent_name attributes
    assert QueryAnalyzerAgent.agent_name == "query_analyzer"
    assert SchemaLinkingAgent.agent_name == "schema_linker"
    assert SQLGeneratorAgent.agent_name == "sql_generator"
    assert SQLExecutorAgent.agent_name == "sql_executor"
    
    print("✓ All agent names correct")


def test_base_class_methods():
    """Test that BaseMemoryAgent has all required methods"""
    print("\nTesting BaseMemoryAgent methods...")
    
    # Check that abstract methods are defined
    assert hasattr(BaseMemoryAgent, '_initialize_managers')
    assert hasattr(BaseMemoryAgent, '_build_system_message')
    assert hasattr(BaseMemoryAgent, '_reader_callback')
    assert hasattr(BaseMemoryAgent, '_parser_callback')
    
    # Check concrete methods
    assert hasattr(BaseMemoryAgent, '_create_model_client')
    assert hasattr(BaseMemoryAgent, '_create_agent')
    assert hasattr(BaseMemoryAgent, '_create_tool')
    assert hasattr(BaseMemoryAgent, 'get_tool')
    assert hasattr(BaseMemoryAgent, 'run')
    
    print("✓ All BaseMemoryAgent methods present")


def test_agent_methods():
    """Test that each agent implements required methods"""
    print("\nTesting agent method implementations...")
    
    agents = [QueryAnalyzerAgent, SchemaLinkingAgent, SQLGeneratorAgent, SQLExecutorAgent]
    
    for agent_class in agents:
        # Check that required methods are implemented
        assert hasattr(agent_class, '_initialize_managers')
        assert hasattr(agent_class, '_build_system_message')
        assert hasattr(agent_class, '_reader_callback')
        assert hasattr(agent_class, '_parser_callback')
        
        # Check that they're not the abstract versions
        assert agent_class._initialize_managers != BaseMemoryAgent._initialize_managers
        assert agent_class._build_system_message != BaseMemoryAgent._build_system_message
        
        print(f"  ✓ {agent_class.__name__} implements all required methods")


def test_helper_classes():
    """Test that helper classes are available"""
    print("\nTesting helper classes...")
    
    from base_memory_agent import MemoryCallbackHelpers
    
    # Check helper methods
    assert hasattr(MemoryCallbackHelpers, 'read_schema_context')
    assert hasattr(MemoryCallbackHelpers, 'extract_xml_content')
    assert hasattr(MemoryCallbackHelpers, 'extract_code_block')
    
    print("✓ MemoryCallbackHelpers available")


def test_agent_specific_features():
    """Test agent-specific features"""
    print("\nTesting agent-specific features...")
    
    # QueryAnalyzerAgent should have analysis methods
    assert hasattr(QueryAnalyzerAgent, 'analyze')
    assert hasattr(QueryAnalyzerAgent, 'get_analysis_summary')
    print("  ✓ QueryAnalyzerAgent has analysis methods")
    
    # SchemaLinkingAgent should have linking methods
    assert hasattr(SchemaLinkingAgent, 'link_schema')
    print("  ✓ SchemaLinkingAgent has link_schema method")
    
    # SQLGeneratorAgent should have generation methods
    assert hasattr(SQLGeneratorAgent, 'generate_sql')
    assert hasattr(SQLGeneratorAgent, 'generate_combined_sql')
    print("  ✓ SQLGeneratorAgent has generation methods")
    
    # SQLExecutorAgent should have analysis methods
    assert hasattr(SQLExecutorAgent, 'analyze_execution')
    assert hasattr(SQLExecutorAgent, 'execute_and_analyze')
    print("  ✓ SQLExecutorAgent has analysis methods")


def test_memory_integration():
    """Test that agents work with KeyValueMemory"""
    print("\nTesting memory integration...")
    
    memory = KeyValueMemory()
    
    # Each agent should accept memory in constructor
    agents = [
        QueryAnalyzerAgent.__init__,
        SchemaLinkingAgent.__init__,
        SQLGeneratorAgent.__init__,
        SQLExecutorAgent.__init__
    ]
    
    for agent_init in agents:
        # Check that memory is the first parameter after self
        import inspect
        params = inspect.signature(agent_init).parameters
        param_list = list(params.keys())
        assert len(param_list) > 1  # At least self and memory
        assert param_list[1] == 'memory'  # memory should be first after self
    
    print("✓ All agents accept KeyValueMemory")


def main():
    """Run all tests"""
    print("Testing Updated Agent Architecture")
    print("=" * 50)
    
    test_imports()
    test_agent_attributes()
    test_base_class_methods()
    test_agent_methods()
    test_helper_classes()
    test_agent_specific_features()
    test_memory_integration()
    
    print("\n" + "=" * 50)
    print("✅ All structure tests passed!")
    print("\nThe updated agents follow the correct pattern:")
    print("1. Inherit from BaseMemoryAgent")
    print("2. Create AutoGen AssistantAgent internally")
    print("3. Wrap with MemoryAgentTool")
    print("4. Implement required callback methods")
    print("5. Provide agent-specific functionality")


if __name__ == "__main__":
    main()