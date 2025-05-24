"""
Updated test cases for Schema Linking Agent using the correct architecture.
"""

import pytest
import pytest_asyncio
from typing import Dict, List, Optional, Any
import sys
from pathlib import Path
import logging

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from keyvalue_memory import KeyValueMemory
from database_schema_manager import DatabaseSchemaManager
from query_tree_manager import QueryTreeManager
from schema_linking_agent import SchemaLinkingAgent
from memory_content_types import TableSchema, ColumnInfo
from task_context_manager import TaskContextManager

# Mock the autogen components for testing
class MockAssistantAgent:
    """Mock AssistantAgent for testing without actual LLM calls"""
    def __init__(self, name, system_message, model_client, **kwargs):
        self.name = name
        self.system_message = system_message
        self.model_client = model_client
        self.description = f"Mock {name}"
    
    async def run(self, task, cancellation_token=None):
        """Mock run method that returns predefined responses"""
        from types import SimpleNamespace
        
        # Generate appropriate response based on the context in task
        if "female clients" in str(task):
            response = """
            <schema_linking>
              <selected_tables>
                <table name="clients" alias="c">
                  <purpose>Contains client information including gender</purpose>
                  <columns>
                    <column name="client_id" used_for="select">
                      <reason>Primary key to identify clients</reason>
                    </column>
                    <column name="gender" used_for="filter">
                      <reason>Filter for female clients</reason>
                    </column>
                    <column name="birth_date" used_for="select">
                      <reason>Additional client information</reason>
                    </column>
                    <column name="district_id" used_for="select">
                      <reason>Client location reference</reason>
                    </column>
                  </columns>
                </table>
              </selected_tables>
              <joins></joins>
              <sample_query_pattern>SELECT columns FROM clients WHERE gender = 'F'</sample_query_pattern>
            </schema_linking>
            """
        elif "loan amounts" in str(task) and "clients" in str(task):
            response = """
            <schema_linking>
              <selected_tables>
                <table name="clients" alias="c">
                  <purpose>Main entity - client information</purpose>
                  <columns>
                    <column name="client_id" used_for="join">
                      <reason>Join key to link with accounts</reason>
                    </column>
                    <column name="gender" used_for="select">
                      <reason>Client demographic info</reason>
                    </column>
                  </columns>
                </table>
                <table name="disp" alias="d">
                  <purpose>Links clients to accounts</purpose>
                  <columns>
                    <column name="client_id" used_for="join">
                      <reason>Foreign key to clients</reason>
                    </column>
                    <column name="account_id" used_for="join">
                      <reason>Foreign key to accounts</reason>
                    </column>
                  </columns>
                </table>
                <table name="loans" alias="l">
                  <purpose>Contains loan information</purpose>
                  <columns>
                    <column name="account_id" used_for="join">
                      <reason>Links loans to accounts</reason>
                    </column>
                    <column name="amount" used_for="select">
                      <reason>Loan amount to display</reason>
                    </column>
                  </columns>
                </table>
              </selected_tables>
              <joins>
                <join>
                  <from_table>clients</from_table>
                  <from_column>client_id</from_column>
                  <to_table>disp</to_table>
                  <to_column>client_id</to_column>
                  <join_type>INNER</join_type>
                </join>
                <join>
                  <from_table>disp</from_table>
                  <from_column>account_id</from_column>
                  <to_table>loans</to_table>
                  <to_column>account_id</to_column>
                  <join_type>INNER</join_type>
                </join>
              </joins>
              <sample_query_pattern>SELECT c.*, l.amount FROM clients c JOIN disp d ON c.client_id = d.client_id JOIN loans l ON d.account_id = l.account_id</sample_query_pattern>
            </schema_linking>
            """
        else:
            response = """
            <schema_linking>
              <selected_tables>
                <table name="accounts" alias="a">
                  <purpose>Default table selection</purpose>
                  <columns>
                    <column name="account_id" used_for="select">
                      <reason>Primary key</reason>
                    </column>
                  </columns>
                </table>
              </selected_tables>
              <joins></joins>
              <sample_query_pattern>SELECT * FROM accounts</sample_query_pattern>
            </schema_linking>
            """
        
        # Create mock message
        message = SimpleNamespace(content=response, source=self.name)
        
        # Create mock result
        result = SimpleNamespace(messages=[message])
        
        return result


class MockMemoryAgentTool:
    """Mock MemoryAgentTool that matches the expected interface"""
    def __init__(self, agent, memory, reader_callback=None, parser_callback=None):
        self.agent = agent
        self.memory = memory
        self.reader_callback = reader_callback
        self.parser_callback = parser_callback
        self.name = agent.name
        self.description = agent.description
    
    async def run(self, args, cancellation_token=None):
        """Mock run method"""
        from types import SimpleNamespace
        
        # Call reader callback if provided
        context = {}
        if self.reader_callback:
            task = args.task if hasattr(args, 'task') else str(args)
            context = await self.reader_callback(self.memory, task, cancellation_token)
        
        # Enhance task with context
        enhanced_task = ""
        if context:
            # Format context into the task
            for key, value in context.items():
                enhanced_task += f"\n{key}: {value}\n"
        
        # Add original task
        task_str = args.task if hasattr(args, 'task') else str(args)
        enhanced_task += f"\nTask: {task_str}"
        
        # Run the agent
        result = await self.agent.run(enhanced_task, cancellation_token)
        
        # Call parser callback if provided
        if self.parser_callback and result:
            await self.parser_callback(self.memory, task_str, result, cancellation_token)
        
        return result


# Patch imports for testing
def setup_mocks(monkeypatch):
    """Setup mocks for testing"""
    # We'll patch the SchemaLinkingAgent._setup_agent method instead
    pass


class TestSchemaLinkingAgentUpdated:
    """Test schema linking with updated architecture"""
    
    async def setup_financial_database(self, schema_manager: DatabaseSchemaManager):
        """Setup the financial database schema"""
        # clients table
        clients_schema = TableSchema(
            name="clients",
            columns={
                "client_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "gender": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "birth_date": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "district_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False, isForeignKey=False)
            },
            sampleData=[
                {"client_id": 1, "gender": "F", "birth_date": "1970-12-13", "district_id": 18},
                {"client_id": 2, "gender": "M", "birth_date": "1945-02-04", "district_id": 1}
            ]
        )
        await schema_manager.add_table(clients_schema)
        
        # disp table (links clients to accounts)
        disp_schema = TableSchema(
            name="disp",
            columns={
                "disp_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "client_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                      isForeignKey=True, references={"table": "clients", "column": "client_id"}),
                "account_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                       isForeignKey=True, references={"table": "accounts", "column": "account_id"}),
                "type": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(disp_schema)
        
        # loans table
        loans_schema = TableSchema(
            name="loans",
            columns={
                "loan_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "account_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                       isForeignKey=True, references={"table": "accounts", "column": "account_id"}),
                "amount": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "date": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(loans_schema)
        
        # accounts table
        accounts_schema = TableSchema(
            name="accounts",
            columns={
                "account_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "district_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False, isForeignKey=False),
                "frequency": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "date": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(accounts_schema)
    
    @pytest.mark.asyncio
    async def test_simple_table_selection(self, monkeypatch):
        """Test: Select all female clients - simple single table scenario"""
        # Override _setup_agent method
        def mock_setup_agent(self):
            # Create a mock assistant agent
            mock_assistant = MockAssistantAgent(
                name="schema_linker",
                system_message="Link schema elements",
                model_client=None
            )
            
            # Create mock MemoryAgentTool
            self.agent = MockMemoryAgentTool(
                agent=mock_assistant,
                memory=self.memory,
                reader_callback=None,  # We'll override pre/post callbacks
                parser_callback=None
            )
            
            # Set the callbacks directly
            self.agent.pre_callback = self._pre_callback
            self.agent.post_callback = self._post_callback
        
        monkeypatch.setattr(SchemaLinkingAgent, "_setup_agent", mock_setup_agent)
        
        # Setup environment
        memory = KeyValueMemory()
        
        # Initialize task context
        task_manager = TaskContextManager(memory)
        await task_manager.initialize(
            "test_task",
            "Show all female clients",
            "financial"
        )
        
        # Initialize schema manager
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_financial_database(schema_manager)
        
        # Initialize query tree
        tree_manager = QueryTreeManager(memory)
        node_id = await tree_manager.initialize("Show all female clients")
        
        # Create and test the agent
        agent = SchemaLinkingAgent(memory, debug=True)
        
        # Test that XML parsing works
        mock_output = """
        <schema_linking>
          <selected_tables>
            <table name="clients" alias="c">
              <purpose>Contains client information including gender</purpose>
              <columns>
                <column name="client_id" used_for="select">
                  <reason>Primary key to identify clients</reason>
                </column>
                <column name="gender" used_for="filter">
                  <reason>Filter for female clients</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          <joins></joins>
          <sample_query_pattern>SELECT * FROM clients WHERE gender = 'F'</sample_query_pattern>
        </schema_linking>
        """
        
        result = agent._parse_linking_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert len(result["tables"]) == 1
        assert result["tables"][0]["name"] == "clients"
        assert len(result["tables"][0]["columns"]) == 2
        assert any(col["name"] == "gender" and col["used_for"] == "filter" 
                  for col in result["tables"][0]["columns"])
        
        # Test mapping creation
        mapping = await agent._create_mapping_from_linking(result)
        assert len(mapping.tables) == 1
        assert mapping.tables[0].name == "clients"
        assert len(mapping.columns) == 2
        
        print("✓ Simple table selection test passed!")
    
    @pytest.mark.asyncio
    async def test_join_detection(self, monkeypatch):
        """Test: Show loan amounts for all clients - multi-table join scenario"""
        # Override _setup_agent method
        def mock_setup_agent(self):
            mock_assistant = MockAssistantAgent(
                name="schema_linker",
                system_message="Link schema elements",
                model_client=None
            )
            self.agent = MockMemoryAgentTool(
                agent=mock_assistant,
                memory=self.memory,
                reader_callback=None,
                parser_callback=None
            )
            self.agent.pre_callback = self._pre_callback
            self.agent.post_callback = self._post_callback
        
        monkeypatch.setattr(SchemaLinkingAgent, "_setup_agent", mock_setup_agent)
        
        # Setup environment
        memory = KeyValueMemory()
        
        # Initialize components
        task_manager = TaskContextManager(memory)
        await task_manager.initialize(
            "test_task",
            "Show loan amounts for all clients",
            "financial"
        )
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_financial_database(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        node_id = await tree_manager.initialize("Show loan amounts for all clients")
        
        # Create agent
        agent = SchemaLinkingAgent(memory, debug=True)
        
        # Test join detection XML
        mock_output = """
        <schema_linking>
          <selected_tables>
            <table name="clients" alias="c">
              <purpose>Main entity - client information</purpose>
              <columns>
                <column name="client_id" used_for="join">
                  <reason>Join key to link with accounts</reason>
                </column>
              </columns>
            </table>
            <table name="disp" alias="d">
              <purpose>Links clients to accounts</purpose>
              <columns>
                <column name="client_id" used_for="join">
                  <reason>Foreign key to clients</reason>
                </column>
                <column name="account_id" used_for="join">
                  <reason>Foreign key to accounts</reason>
                </column>
              </columns>
            </table>
            <table name="loans" alias="l">
              <purpose>Contains loan information</purpose>
              <columns>
                <column name="account_id" used_for="join">
                  <reason>Links loans to accounts</reason>
                </column>
                <column name="amount" used_for="select">
                  <reason>Loan amount to display</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          <joins>
            <join>
              <from_table>clients</from_table>
              <from_column>client_id</from_column>
              <to_table>disp</to_table>
              <to_column>client_id</to_column>
              <join_type>INNER</join_type>
            </join>
            <join>
              <from_table>disp</from_table>
              <from_column>account_id</from_column>
              <to_table>loans</to_table>
              <to_column>account_id</to_column>
              <join_type>INNER</join_type>
            </join>
          </joins>
          <sample_query_pattern>SELECT c.*, l.amount FROM clients c JOIN disp d ON c.client_id = d.client_id JOIN loans l ON d.account_id = l.account_id</sample_query_pattern>
        </schema_linking>
        """
        
        result = agent._parse_linking_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert len(result["tables"]) == 3
        assert set(t["name"] for t in result["tables"]) == {"clients", "disp", "loans"}
        assert len(result["joins"]) == 2
        assert result["joins"][0]["from_table"] == "clients"
        assert result["joins"][1]["to_table"] == "loans"
        
        # Test mapping creation
        mapping = await agent._create_mapping_from_linking(result)
        assert len(mapping.tables) == 3
        assert len(mapping.joins) == 2
        
        print("✓ Join detection test passed!")
    
    @pytest.mark.asyncio  
    async def test_invalid_xml_handling(self, monkeypatch):
        """Test handling of invalid XML responses"""
        # Override _setup_agent
        def mock_setup_agent(self):
            mock_assistant = MockAssistantAgent(
                name="schema_linker",
                system_message="Link schema elements",
                model_client=None
            )
            self.agent = MockMemoryAgentTool(
                agent=mock_assistant,
                memory=self.memory,
                reader_callback=None,
                parser_callback=None
            )
        
        monkeypatch.setattr(SchemaLinkingAgent, "_setup_agent", mock_setup_agent)
        
        memory = KeyValueMemory()
        agent = SchemaLinkingAgent(memory, debug=True)
        
        # Test invalid XML
        invalid_xml = """
        <schema_linking>
          <selected_tables>
            <table name="test_table">
              <!-- Missing closing tag -->
        """
        
        result = agent._parse_linking_xml(invalid_xml)
        
        # Should return None for invalid XML
        assert result is None
        
        print("✓ Invalid XML handling test passed!")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Run tests
    import asyncio
    asyncio.run(pytest.main([__file__, "-v"]))