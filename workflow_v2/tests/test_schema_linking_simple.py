"""
Simple test for Schema Linking Agent using BIRD dataset.
"""

import src.pytest as pytest
import src.pytest_asyncio as pytest_asyncio
from src.memory import KeyValueMemory
from src.database_schema_manager import DatabaseSchemaManager
from src.query_tree_manager import QueryTreeManager
from src.schema_linking_agent import SchemaLinkingAgent
from src.memory_types import TableSchema, ColumnInfo


class MockMemoryAgentTool:
    """Mock agent tool for testing."""
    def __init__(self, name=None, signature=None, instructions=None, model=None, 
                 memory=None, pre_callback=None, post_callback=None, debug=None):
        self.name = name or 'mock_agent'
        self.memory = memory
        self.pre_callback = pre_callback
        self.post_callback = post_callback
        self._mock_response = None
        
    async def run(self, inputs):
        """Return the mocked response and call callbacks."""
        # Call pre_callback if it exists
        if self.pre_callback:
            inputs = await self.pre_callback(inputs)
        
        # Return mocked response
        output = self._mock_response if self._mock_response else """
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
          <sample_query_pattern>SELECT columns FROM clients WHERE gender = 'F'</sample_query_pattern>
        </schema_linking>
        """
        
        # Skip post_callback to avoid NodeOperation issues
        # if self.post_callback:
        #     await self.post_callback(output, inputs)
            
        return output


class TestSchemaLinkingSimple:
    """Simple test for schema linking with BIRD data."""
    
    @pytest_asyncio.fixture
    async def setup(self):
        """Setup test environment."""
        memory = KeyValueMemory()
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        
        # Monkey patch MemoryAgentTool before creating the agent
        import schema_linking_agent
        original_memory_agent_tool = schema_linking_agent.MemoryAgentTool
        schema_linking_agent.MemoryAgentTool = MockMemoryAgentTool
        
        # Create schema linking agent with debug enabled
        agent = SchemaLinkingAgent(memory, model_name="gpt-4o", debug=True)
        
        # Restore original class
        schema_linking_agent.MemoryAgentTool = original_memory_agent_tool
        
        # Setup simple schema
        clients_schema = TableSchema(
            name="clients",
            columns={
                "client_id": ColumnInfo(
                    dataType="INTEGER", 
                    nullable=False, 
                    isPrimaryKey=True, 
                    isForeignKey=False
                ),
                "gender": ColumnInfo(
                    dataType="TEXT", 
                    nullable=True, 
                    isPrimaryKey=False, 
                    isForeignKey=False
                ),
                "birth_date": ColumnInfo(
                    dataType="DATE", 
                    nullable=True, 
                    isPrimaryKey=False, 
                    isForeignKey=False
                )
            },
            sampleData=[
                {"client_id": 1, "gender": "F", "birth_date": "1970-12-13"},
                {"client_id": 2, "gender": "M", "birth_date": "1945-02-04"},
                {"client_id": 3, "gender": "F", "birth_date": "1940-10-09"}
            ]
        )
        await schema_manager.add_table(clients_schema)
        
        yield {
            "memory": memory,
            "schema_manager": schema_manager,
            "tree_manager": tree_manager,
            "agent": agent
        }
    
    @pytest.mark.asyncio
    async def test_simple_table_selection(self, setup):
        """Test: Select all female clients."""
        env = setup
        
        # Create query node
        node_id = await env["tree_manager"].initialize("Show all female clients")
        
        # Set mock response
        env["agent"].agent._mock_response = """
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
              </columns>
            </table>
          </selected_tables>
          
          <joins>
          </joins>
          
          <sample_query_pattern>
            SELECT columns FROM clients WHERE gender = 'F'
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test the XML parsing directly
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
                <column name="birth_date" used_for="select">
                  <reason>Additional client information</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          
          <joins>
          </joins>
          
          <sample_query_pattern>
            SELECT columns FROM clients WHERE gender = 'F'
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_linking_xml(mock_output)
        
        # Verify
        assert result is not None
        assert len(result["tables"]) == 1
        assert result["tables"][0]["name"] == "clients"
        assert len(result["tables"][0]["columns"]) == 3
        assert any(col["name"] == "gender" and col["used_for"] == "filter" 
                  for col in result["tables"][0]["columns"])
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 1
        assert mapping.tables[0].name == "clients"
        assert len(mapping.columns) == 3
        
        print("✓ Schema linking XML parsing test passed!")


if __name__ == "__main__":
    import asyncio
    async def run_test():
        test = TestSchemaLinkingSimple()
        setup_gen = test.setup()
        setup_data = await setup_gen.__anext__()
        await test.test_simple_table_selection(setup_data)
    
    asyncio.run(run_test())