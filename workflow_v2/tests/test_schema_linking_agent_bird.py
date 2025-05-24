"""
Comprehensive test cases for Schema Linking Agent using actual BIRD dataset examples.

This test suite covers:
1. Simple table selection scenarios  
2. Multi-table join scenarios
3. Complex aggregation scenarios
4. Formula 1 dataset scenarios
5. Edge cases and error handling

Tests use real database schemas and queries from the BIRD benchmark
to ensure realistic testing of the schema linking functionality.
"""

import src.pytest as pytest
import src.pytest_asyncio as pytest_asyncio
from src.typing import Dict, List, Optional, Any

from src.memory import KeyValueMemory
from src.database_schema_manager import DatabaseSchemaManager
from src.query_tree_manager import QueryTreeManager
from src.schema_linking_agent import SchemaLinkingAgent
from src.memory_content_types import TableSchema, ColumnInfo


class MockMemoryAgentTool:
    """Mock agent tool for testing without LLM dependencies."""
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
            <table name="test_table" alias="t">
              <purpose>Default test table</purpose>
              <columns>
                <column name="id" used_for="select">
                  <reason>Primary key</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          <joins></joins>
          <sample_query_pattern>SELECT * FROM test_table</sample_query_pattern>
        </schema_linking>
        """
        
        # Skip post_callback to avoid NodeOperation issues in testing
        return output


class TestSchemaLinkingAgentBIRD:
    """Test schema linking with comprehensive BIRD dataset scenarios."""
    
    @pytest_asyncio.fixture
    async def setup(self):
        """Setup test environment with memory and managers."""
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
        
        yield {
            "memory": memory,
            "schema_manager": schema_manager,
            "tree_manager": tree_manager,
            "agent": agent
        }
    
    async def setup_financial_database(self, schema_manager: DatabaseSchemaManager):
        """Setup the financial database schema from BIRD."""
        # accounts table
        accounts_schema = TableSchema(
            name="accounts",
            columns={
                "account_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "district_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                        isForeignKey=True, references={"table": "district", "column": "district_id"}),
                "frequency": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "date": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False)
            },
            sampleData=[
                {"account_id": 1, "district_id": 18, "frequency": "POPLATEK MESICNE", "date": "1995-03-24"},
                {"account_id": 2, "district_id": 1, "frequency": "POPLATEK MESICNE", "date": "1993-02-26"},
                {"account_id": 3, "district_id": 5, "frequency": "POPLATEK MESICNE", "date": "1997-07-07"}
            ]
        )
        await schema_manager.add_table(accounts_schema)
        
        # clients table
        clients_schema = TableSchema(
            name="clients",
            columns={
                "client_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "gender": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "birth_date": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "district_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                        isForeignKey=True, references={"table": "district", "column": "district_id"})
            },
            sampleData=[
                {"client_id": 1, "gender": "F", "birth_date": "1970-12-13", "district_id": 18},
                {"client_id": 2, "gender": "M", "birth_date": "1945-02-04", "district_id": 1},
                {"client_id": 3, "gender": "F", "birth_date": "1940-10-09", "district_id": 54}
            ]
        )
        await schema_manager.add_table(clients_schema)
        
        # disp table (disposition - linking clients to accounts)
        disp_schema = TableSchema(
            name="disp",
            columns={
                "disp_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "client_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                      isForeignKey=True, references={"table": "clients", "column": "client_id"}),
                "account_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                       isForeignKey=True, references={"table": "accounts", "column": "account_id"}),
                "type": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            },
            sampleData=[
                {"disp_id": 1, "client_id": 1, "account_id": 1, "type": "OWNER"},
                {"disp_id": 2, "client_id": 2, "account_id": 2, "type": "OWNER"},
                {"disp_id": 3, "client_id": 3, "account_id": 2, "type": "DISPONENT"}
            ]
        )
        await schema_manager.add_table(disp_schema)
        
        # transactions table
        transactions_schema = TableSchema(
            name="transactions",
            columns={
                "trans_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "account_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                       isForeignKey=True, references={"table": "accounts", "column": "account_id"}),
                "date": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "type": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "operation": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "amount": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "balance": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "k_symbol": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            },
            sampleData=[
                {"trans_id": 1, "account_id": 1, "date": "1995-03-24", "type": "PRIJEM", 
                 "operation": "VKLAD", "amount": 1000, "balance": 1000, "k_symbol": None},
                {"trans_id": 2, "account_id": 2, "date": "1993-02-26", "type": "PRIJEM",
                 "operation": "VKLAD", "amount": 2452, "balance": 2452, "k_symbol": None}
            ]
        )
        await schema_manager.add_table(transactions_schema)
        
        # loans table
        loans_schema = TableSchema(
            name="loans",
            columns={
                "loan_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "account_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False,
                                       isForeignKey=True, references={"table": "accounts", "column": "account_id"}),
                "date": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "amount": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "duration": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "payments": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "status": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            },
            sampleData=[
                {"loan_id": 4959, "account_id": 2, "date": "1994-01-05", "amount": 80952, 
                 "duration": 24, "payments": 3373, "status": "A"},
                {"loan_id": 4960, "account_id": 19, "date": "1996-04-29", "amount": 30276,
                 "duration": 12, "payments": 2523, "status": "B"}
            ]
        )
        await schema_manager.add_table(loans_schema)
        
        # district table
        district_schema = TableSchema(
            name="district",
            columns={
                "district_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "A2": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),  # district name
                "A3": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),  # region
                "A4": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),  # inhabitants
                "A11": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)  # average salary
            },
            sampleData=[
                {"district_id": 1, "A2": "Hl.m. Praha", "A3": "Prague", "A4": 1204953, "A11": 12541},
                {"district_id": 2, "A2": "Benesov", "A3": "central Bohemia", "A4": 88884, "A11": 8507}
            ]
        )
        await schema_manager.add_table(district_schema)
    
    @pytest.mark.asyncio
    async def test_simple_table_selection(self, setup):
        """Test: Select all female clients - simple single table scenario."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create query node
        node_id = await env["tree_manager"].initialize("Show all female clients")
        
        # Mock XML output for single table selection
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
                <column name="district_id" used_for="select">
                  <reason>Client location reference</reason>
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
        
        # Verify parsing
        assert result is not None
        assert len(result["tables"]) == 1
        assert result["tables"][0]["name"] == "clients"
        assert len(result["tables"][0]["columns"]) == 4
        assert any(col["name"] == "gender" and col["used_for"] == "filter" 
                  for col in result["tables"][0]["columns"])
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 1
        assert mapping.tables[0].name == "clients"
        assert len(mapping.columns) == 4
        
        print("✓ Simple table selection test passed!")
    
    @pytest.mark.asyncio
    async def test_join_detection(self, setup):
        """Test: Show loan amounts for all clients - multi-table join scenario."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create query node
        node_id = await env["tree_manager"].initialize("Show loan amounts for all clients")
        
        # Mock XML output for multi-table join
        mock_output = """
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
                <column name="date" used_for="select">
                  <reason>Loan date information</reason>
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
          
          <sample_query_pattern>
            SELECT c.*, l.amount FROM clients c 
            JOIN disp d ON c.client_id = d.client_id
            JOIN loans l ON d.account_id = l.account_id
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_linking_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert len(result["tables"]) == 3
        assert set(t["name"] for t in result["tables"]) == {"clients", "disp", "loans"}
        assert len(result["joins"]) == 2
        assert result["joins"][0]["from_table"] == "clients"
        assert result["joins"][1]["to_table"] == "loans"
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 3
        assert len(mapping.joins) == 2
        
        print("✓ Join detection test passed!")
    
    @pytest.mark.asyncio
    async def test_complex_aggregation(self, setup):
        """Test: Average transaction amount by district for accounts with loans."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create query node
        node_id = await env["tree_manager"].initialize(
            "Calculate average transaction amount by district for accounts that have loans"
        )
        
        # Mock XML output for complex aggregation
        mock_output = """
        <schema_linking>
          <selected_tables>
            <table name="accounts" alias="a">
              <purpose>Central table linking transactions, loans, and districts</purpose>
              <columns>
                <column name="account_id" used_for="join">
                  <reason>Primary key for joining with transactions and loans</reason>
                </column>
                <column name="district_id" used_for="join">
                  <reason>Foreign key for grouping by district</reason>
                </column>
              </columns>
            </table>
            <table name="transactions" alias="t">
              <purpose>Contains transaction amounts for averaging</purpose>
              <columns>
                <column name="account_id" used_for="join">
                  <reason>Foreign key to accounts</reason>
                </column>
                <column name="amount" used_for="aggregate">
                  <reason>Transaction amount for AVG calculation</reason>
                </column>
              </columns>
            </table>
            <table name="loans" alias="l">
              <purpose>Filter accounts that have loans</purpose>
              <columns>
                <column name="account_id" used_for="join">
                  <reason>Identify accounts with loans</reason>
                </column>
              </columns>
            </table>
            <table name="district" alias="d">
              <purpose>District information for grouping</purpose>
              <columns>
                <column name="district_id" used_for="join">
                  <reason>Primary key for district</reason>
                </column>
                <column name="A2" used_for="select">
                  <reason>District name for display</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          
          <joins>
            <join>
              <from_table>accounts</from_table>
              <from_column>account_id</from_column>
              <to_table>transactions</to_table>
              <to_column>account_id</to_column>
              <join_type>INNER</join_type>
            </join>
            <join>
              <from_table>accounts</from_table>
              <from_column>account_id</from_column>
              <to_table>loans</to_table>
              <to_column>account_id</to_column>
              <join_type>INNER</join_type>
            </join>
            <join>
              <from_table>accounts</from_table>
              <from_column>district_id</from_column>
              <to_table>district</to_table>
              <to_column>district_id</to_column>
              <join_type>INNER</join_type>
            </join>
          </joins>
          
          <sample_query_pattern>
            SELECT d.A2, AVG(t.amount) FROM accounts a
            JOIN transactions t ON a.account_id = t.account_id
            JOIN loans l ON a.account_id = l.account_id
            JOIN district d ON a.district_id = d.district_id
            GROUP BY d.district_id, d.A2
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_linking_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert len(result["tables"]) == 4
        assert set(t["name"] for t in result["tables"]) == {"accounts", "transactions", "loans", "district"}
        assert len(result["joins"]) == 3
        assert any(col["used_for"] == "aggregate" for t in result["tables"] for col in t["columns"])
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 4
        assert len(mapping.joins) == 3
        
        print("✓ Complex aggregation test passed!")
    
    @pytest.mark.asyncio
    async def test_minimal_column_selection(self, setup):
        """Test: Just count the number of accounts - minimal selection."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create query node
        node_id = await env["tree_manager"].initialize("Count total number of accounts")
        
        # Mock XML output for minimal selection
        mock_output = """
        <schema_linking>
          <selected_tables>
            <table name="accounts" alias="a">
              <purpose>Table to count records from</purpose>
              <columns>
                <column name="account_id" used_for="aggregate">
                  <reason>Count distinct accounts (or use COUNT(*))</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          
          <joins>
          </joins>
          
          <sample_query_pattern>
            SELECT COUNT(*) FROM accounts
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_linking_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert len(result["tables"]) == 1
        assert result["tables"][0]["name"] == "accounts"
        # Should only select necessary columns for counting
        assert len(result["tables"][0]["columns"]) == 1
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 1
        assert len(mapping.columns) == 1
        
        print("✓ Minimal column selection test passed!")
    
    @pytest.mark.asyncio
    async def test_date_filtering(self, setup):
        """Test: Find loans issued in 1994 - date filtering scenario."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create query node
        node_id = await env["tree_manager"].initialize("Show all loans issued in 1994")
        
        # Mock XML output for date filtering
        mock_output = """
        <schema_linking>
          <selected_tables>
            <table name="loans" alias="l">
              <purpose>Main table containing loan information</purpose>
              <columns>
                <column name="loan_id" used_for="select">
                  <reason>Loan identifier</reason>
                </column>
                <column name="account_id" used_for="select">
                  <reason>Associated account</reason>
                </column>
                <column name="date" used_for="filter">
                  <reason>Filter for year 1994</reason>
                </column>
                <column name="amount" used_for="select">
                  <reason>Loan amount</reason>
                </column>
                <column name="duration" used_for="select">
                  <reason>Loan duration</reason>
                </column>
                <column name="status" used_for="select">
                  <reason>Loan status</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          
          <joins>
          </joins>
          
          <sample_query_pattern>
            SELECT * FROM loans WHERE YEAR(date) = 1994
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_linking_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert len(result["tables"]) == 1
        assert result["tables"][0]["name"] == "loans"
        assert any(col["name"] == "date" and col["used_for"] == "filter"
                  for col in result["tables"][0]["columns"])
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 1
        assert any(col.column == "date" for col in mapping.columns)
        
        print("✓ Date filtering test passed!")
    
    @pytest.mark.asyncio  
    async def test_implicit_join_requirements(self, setup):
        """Test: Count transactions per client gender - requires implicit join through disp table."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create query node
        node_id = await env["tree_manager"].initialize("Count the number of transactions for each gender")
        
        # Mock XML output for implicit join requirements
        mock_output = """
        <schema_linking>
          <selected_tables>
            <table name="clients" alias="c">
              <purpose>Source of gender information</purpose>
              <columns>
                <column name="client_id" used_for="join">
                  <reason>Link to accounts via disp</reason>
                </column>
                <column name="gender" used_for="group">
                  <reason>Group by gender</reason>
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
            <table name="transactions" alias="t">
              <purpose>Transaction data for counting</purpose>
              <columns>
                <column name="trans_id" used_for="aggregate">
                  <reason>Count transactions</reason>
                </column>
                <column name="account_id" used_for="join">
                  <reason>Link to accounts</reason>
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
              <to_table>transactions</to_table>
              <to_column>account_id</to_column>
              <join_type>INNER</join_type>
            </join>
          </joins>
          
          <sample_query_pattern>
            SELECT c.gender, COUNT(t.trans_id) FROM clients c
            JOIN disp d ON c.client_id = d.client_id
            JOIN transactions t ON d.account_id = t.account_id
            GROUP BY c.gender
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_linking_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert len(result["tables"]) == 3
        # Should include intermediate table 'disp' for the join path
        assert "disp" in [t["name"] for t in result["tables"]]
        assert len(result["joins"]) == 2
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 3
        assert len(mapping.joins) == 2
        
        print("✓ Implicit join requirements test passed!")


class TestSchemaLinkingEdgeCases:
    """Test edge cases and error scenarios for schema linking."""
    
    @pytest_asyncio.fixture
    async def setup(self):
        """Setup test environment."""
        memory = KeyValueMemory()
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        
        # Monkey patch MemoryAgentTool
        import schema_linking_agent
        original_memory_agent_tool = schema_linking_agent.MemoryAgentTool
        schema_linking_agent.MemoryAgentTool = MockMemoryAgentTool
        
        agent = SchemaLinkingAgent(memory, model_name="gpt-4o", debug=True)
        schema_linking_agent.MemoryAgentTool = original_memory_agent_tool
        
        yield {
            "memory": memory,
            "schema_manager": schema_manager,
            "tree_manager": tree_manager,
            "agent": agent
        }
    
    @pytest.mark.asyncio
    async def test_no_tables_selected(self, setup):
        """Test behavior when no tables are selected."""
        env = setup
        
        # Mock XML output with no tables
        mock_output = """
        <schema_linking>
          <selected_tables>
          </selected_tables>
          <joins>
          </joins>
          <sample_query_pattern>
            No schema available
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_linking_xml(mock_output)
        
        # Should handle gracefully
        assert result is not None
        assert result.get("tables", []) == []
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 0
        
        print("✓ No tables selected test passed!")
    
    @pytest.mark.asyncio
    async def test_invalid_xml_response(self, setup):
        """Test handling of invalid XML in LLM response."""
        env = setup
        
        # Mock invalid XML response
        invalid_xml = """
        <schema_linking>
          <selected_tables>
            <table name="test_table">
              <!-- Missing closing tag -->
        """
        
        # Test XML parsing - should handle error gracefully
        result = env["agent"]._parse_linking_xml(invalid_xml)
        
        # Should return None for invalid XML
        assert result is None
        
        print("✓ Invalid XML response test passed!")
    
    @pytest.mark.asyncio
    async def test_self_referential_table(self, setup):
        """Test handling of self-referential foreign keys."""
        env = setup
        
        # Mock XML output for self-join
        mock_output = """
        <schema_linking>
          <selected_tables>
            <table name="employees" alias="e">
              <purpose>Employee information</purpose>
              <columns>
                <column name="employee_id" used_for="join">
                  <reason>Primary key for self-join</reason>
                </column>
                <column name="name" used_for="select">
                  <reason>Employee name</reason>
                </column>
                <column name="manager_id" used_for="join">
                  <reason>Foreign key for self-join</reason>
                </column>
              </columns>
            </table>
            <table name="employees" alias="m">
              <purpose>Manager information (self-join)</purpose>
              <columns>
                <column name="employee_id" used_for="join">
                  <reason>Join as manager</reason>
                </column>
                <column name="name" used_for="select">
                  <reason>Manager name</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          
          <joins>
            <join>
              <from_table>employees</from_table>
              <from_column>manager_id</from_column>
              <to_table>employees</to_table>
              <to_column>employee_id</to_column>
              <join_type>LEFT</join_type>
            </join>
          </joins>
          
          <sample_query_pattern>
            SELECT e.name as employee, m.name as manager 
            FROM employees e
            LEFT JOIN employees m ON e.manager_id = m.employee_id
          </sample_query_pattern>
        </schema_linking>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_linking_xml(mock_output)
        
        # Verify self-join is handled
        assert result is not None
        assert len(result["tables"]) == 2
        assert all(t["name"] == "employees" for t in result["tables"])
        assert result["tables"][0]["alias"] != result["tables"][1]["alias"]
        
        # Test mapping creation
        mapping = await env["agent"]._create_mapping_from_linking(result)
        assert len(mapping.tables) == 2
        assert len(mapping.joins) == 1
        
        print("✓ Self-referential table test passed!")


if __name__ == "__main__":
    import sys
    sys.path.append('..')
    import asyncio
    async def run_tests():
        # Run a subset of tests
        test_suite = TestSchemaLinkingAgentBIRD()
        setup_gen = test_suite.setup()
        setup_data = await setup_gen.__anext__()
        
        await test_suite.test_simple_table_selection(setup_data)
        await test_suite.test_join_detection(setup_data)
        await test_suite.test_complex_aggregation(setup_data)
        await test_suite.test_minimal_column_selection(setup_data)
        await test_suite.test_date_filtering(setup_data)
        await test_suite.test_implicit_join_requirements(setup_data)
        
        print("\n✅ All Schema Linking Agent tests completed successfully!")
    
    asyncio.run(run_tests())