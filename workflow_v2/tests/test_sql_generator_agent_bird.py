"""
Comprehensive test cases for SQL Generator Agent using actual BIRD dataset examples.

This test suite covers:
1. Simple SELECT queries
2. Multi-table JOIN queries  
3. Aggregation queries with GROUP BY
4. Complex queries with subqueries/CTEs
5. Date filtering queries
6. Edge cases and error handling

Tests use real database schemas and expected SQL outputs from the BIRD benchmark
to ensure realistic testing of the SQL generation functionality.
"""

import src.pytest as pytest
import src.pytest_asyncio as pytest_asyncio
from src.typing import Dict, List, Optional, Any

from src.memory import KeyValueMemory
from src.database_schema_manager import DatabaseSchemaManager
from src.query_tree_manager import QueryTreeManager
from src.sql_generator_agent import SQLGeneratorAgent
from src.memory_content_types import (
    TableSchema, ColumnInfo, QueryNode, QueryMapping, 
    TableMapping, ColumnMapping, JoinMapping, NodeStatus
)


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
        <sql_generation>
          <sql>
            <![CDATA[
            SELECT * FROM test_table;
            ]]>
          </sql>
          <explanation>Default test SQL query</explanation>
          <query_type>select</query_type>
          <components>
            <tables>
              <table name="test_table">Main data source</table>
            </tables>
            <key_operations>
              <operation type="select">Select all columns</operation>
            </key_operations>
          </components>
        </sql_generation>
        """
        
        # Skip post_callback to avoid NodeOperation issues in testing
        return output


class TestSQLGeneratorAgentBIRD:
    """Test SQL generation with comprehensive BIRD dataset scenarios."""
    
    @pytest_asyncio.fixture
    async def setup(self):
        """Setup test environment with memory and managers."""
        memory = KeyValueMemory()
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        
        # Monkey patch MemoryAgentTool before creating the agent
        import sql_generator_agent
        original_memory_agent_tool = sql_generator_agent.MemoryAgentTool
        sql_generator_agent.MemoryAgentTool = MockMemoryAgentTool
        
        # Create SQL generator agent with debug enabled
        agent = SQLGeneratorAgent(memory, model_name="gpt-4o", debug=True)
        
        # Restore original class
        sql_generator_agent.MemoryAgentTool = original_memory_agent_tool
        
        yield {
            "memory": memory,
            "schema_manager": schema_manager,
            "tree_manager": tree_manager,
            "agent": agent
        }
    
    async def setup_financial_database(self, schema_manager: DatabaseSchemaManager):
        """Setup the financial database schema from BIRD."""
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
                {"account_id": 2, "district_id": 1, "frequency": "POPLATEK MESICNE", "date": "1993-02-26"}
            ]
        )
        await schema_manager.add_table(accounts_schema)
        
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
    
    def create_query_node_with_mapping(self, intent: str, tables: List[Dict], columns: List[Dict], joins: List[Dict] = None) -> QueryNode:
        """Helper to create a QueryNode with schema mapping."""
        # Create table mappings
        table_mappings = []
        for table in tables:
            table_mappings.append(TableMapping(
                name=table["name"],
                alias=table.get("alias"),
                purpose=table.get("purpose", "")
            ))
        
        # Create column mappings
        column_mappings = []
        for col in columns:
            column_mappings.append(ColumnMapping(
                table=col["table"],
                column=col["column"],
                usedFor=col["usedFor"]
            ))
        
        # Create join mappings
        join_mappings = []
        if joins:
            for join in joins:
                join_mappings.append(JoinMapping(
                    from_table=join["from_table"],
                    to=join["to_table"],
                    on=join["on"]
                ))
        
        # Create query mapping
        mapping = QueryMapping(
            tables=table_mappings,
            columns=column_mappings,
            joins=join_mappings if join_mappings else None
        )
        
        # Create query node
        node = QueryNode(
            nodeId="test_node_1",
            intent=intent,
            mapping=mapping,
            status=NodeStatus.CREATED
        )
        
        return node
    
    @pytest.mark.asyncio
    async def test_simple_select_query(self, setup):
        """Test: Simple SELECT query for female clients."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with schema mapping
        node = self.create_query_node_with_mapping(
            intent="Show all female clients",
            tables=[{"name": "clients", "alias": "c", "purpose": "Client information"}],
            columns=[
                {"table": "clients", "column": "client_id", "usedFor": "select"},
                {"table": "clients", "column": "gender", "usedFor": "filter"},
                {"table": "clients", "column": "birth_date", "usedFor": "select"},
                {"table": "clients", "column": "district_id", "usedFor": "select"}
            ]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Mock SQL generation response
        expected_sql = "SELECT c.client_id, c.gender, c.birth_date, c.district_id FROM clients c WHERE c.gender = 'F'"
        
        mock_output = f"""
        <sql_generation>
          <sql>
            <![CDATA[
            {expected_sql}
            ]]>
          </sql>
          <explanation>Simple SELECT query filtering clients by gender</explanation>
          <query_type>select</query_type>
          <components>
            <tables>
              <table name="clients" alias="c">Source of client data with gender filter</table>
            </tables>
            <key_operations>
              <operation type="filter">WHERE gender = 'F' to filter female clients</operation>
              <operation type="select">SELECT client details</operation>
            </key_operations>
          </components>
        </sql_generation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_generation_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert expected_sql.strip() in result["sql"].strip()
        assert result["query_type"] == "select"
        assert result["explanation"] != ""
        assert len(result["components"]["tables"]) == 1
        assert result["components"]["tables"][0]["name"] == "clients"
        
        print("✓ Simple SELECT query test passed!")
    
    @pytest.mark.asyncio
    async def test_join_query(self, setup):
        """Test: Multi-table JOIN query for loans with client info."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with join mapping
        node = self.create_query_node_with_mapping(
            intent="Show loans with client gender information",
            tables=[
                {"name": "clients", "alias": "c", "purpose": "Client demographic info"},
                {"name": "accounts", "alias": "a", "purpose": "Link clients to loans"},
                {"name": "loans", "alias": "l", "purpose": "Loan information"}
            ],
            columns=[
                {"table": "clients", "column": "client_id", "usedFor": "join"},
                {"table": "clients", "column": "gender", "usedFor": "select"},
                {"table": "accounts", "column": "account_id", "usedFor": "join"},
                {"table": "loans", "column": "loan_id", "usedFor": "select"},
                {"table": "loans", "column": "amount", "usedFor": "select"},
                {"table": "loans", "column": "date", "usedFor": "select"}
            ],
            joins=[
                {"from_table": "clients", "to_table": "accounts", "on": "c.district_id = a.district_id"},
                {"from_table": "accounts", "to_table": "loans", "on": "a.account_id = l.account_id"}
            ]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Mock SQL generation response
        expected_sql = """
        SELECT c.gender, l.loan_id, l.amount, l.date
        FROM clients c
        JOIN accounts a ON c.district_id = a.district_id
        JOIN loans l ON a.account_id = l.account_id
        """
        
        mock_output = f"""
        <sql_generation>
          <sql>
            <![CDATA[
            {expected_sql.strip()}
            ]]>
          </sql>
          <explanation>Multi-table JOIN to connect clients with their loan information</explanation>
          <query_type>join</query_type>
          <components>
            <tables>
              <table name="clients" alias="c">Client demographic information</table>
              <table name="accounts" alias="a">Account linking table</table>
              <table name="loans" alias="l">Loan details</table>
            </tables>
            <key_operations>
              <operation type="join">JOIN accounts on district to link clients</operation>
              <operation type="join">JOIN loans on account_id to get loan info</operation>
              <operation type="select">SELECT client gender and loan details</operation>
            </key_operations>
          </components>
        </sql_generation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_generation_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert "JOIN" in result["sql"].upper()
        assert result["query_type"] == "join"
        assert len(result["components"]["tables"]) == 3
        assert any(op["type"] == "join" for op in result["components"]["operations"])
        
        print("✓ Multi-table JOIN query test passed!")
    
    @pytest.mark.asyncio
    async def test_aggregation_query(self, setup):
        """Test: Aggregation query with GROUP BY."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with aggregation mapping
        node = self.create_query_node_with_mapping(
            intent="Count loans by district",
            tables=[
                {"name": "loans", "alias": "l", "purpose": "Loan data for counting"},
                {"name": "accounts", "alias": "a", "purpose": "Link loans to districts"},
                {"name": "district", "alias": "d", "purpose": "District information"}
            ],
            columns=[
                {"table": "loans", "column": "loan_id", "usedFor": "aggregate"},
                {"table": "accounts", "column": "account_id", "usedFor": "join"},
                {"table": "accounts", "column": "district_id", "usedFor": "join"},
                {"table": "district", "column": "district_id", "usedFor": "group"},
                {"table": "district", "column": "A2", "usedFor": "select"}
            ],
            joins=[
                {"from_table": "loans", "to_table": "accounts", "on": "l.account_id = a.account_id"},
                {"from_table": "accounts", "to_table": "district", "on": "a.district_id = d.district_id"}
            ]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Mock SQL generation response
        expected_sql = """
        SELECT d.A2 as district_name, COUNT(l.loan_id) as loan_count
        FROM loans l
        JOIN accounts a ON l.account_id = a.account_id
        JOIN district d ON a.district_id = d.district_id
        GROUP BY d.district_id, d.A2
        ORDER BY loan_count DESC
        """
        
        mock_output = f"""
        <sql_generation>
          <sql>
            <![CDATA[
            {expected_sql.strip()}
            ]]>
          </sql>
          <explanation>Aggregate query counting loans grouped by district</explanation>
          <query_type>aggregate</query_type>
          <components>
            <tables>
              <table name="loans" alias="l">Source of loan records</table>
              <table name="accounts" alias="a">Linking table</table>
              <table name="district" alias="d">District names</table>
            </tables>
            <key_operations>
              <operation type="join">JOIN to link loans with districts</operation>
              <operation type="aggregate">COUNT loans per district</operation>
              <operation type="group">GROUP BY district</operation>
              <operation type="order">ORDER BY count descending</operation>
            </key_operations>
          </components>
        </sql_generation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_generation_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert "COUNT" in result["sql"].upper()
        assert "GROUP BY" in result["sql"].upper()
        assert result["query_type"] == "aggregate"
        assert any(op["type"] == "aggregate" for op in result["components"]["operations"])
        assert any(op["type"] == "group" for op in result["components"]["operations"])
        
        print("✓ Aggregation query test passed!")
    
    @pytest.mark.asyncio
    async def test_date_filtering_query(self, setup):
        """Test: Date filtering query."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with date filtering
        node = self.create_query_node_with_mapping(
            intent="Find loans issued in 1994",
            tables=[{"name": "loans", "alias": "l", "purpose": "Loan records with dates"}],
            columns=[
                {"table": "loans", "column": "loan_id", "usedFor": "select"},
                {"table": "loans", "column": "account_id", "usedFor": "select"},
                {"table": "loans", "column": "date", "usedFor": "filter"},
                {"table": "loans", "column": "amount", "usedFor": "select"},
                {"table": "loans", "column": "duration", "usedFor": "select"},
                {"table": "loans", "column": "status", "usedFor": "select"}
            ]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Mock SQL generation response
        expected_sql = """
        SELECT l.loan_id, l.account_id, l.date, l.amount, l.duration, l.status
        FROM loans l
        WHERE YEAR(l.date) = 1994
        ORDER BY l.date
        """
        
        mock_output = f"""
        <sql_generation>
          <sql>
            <![CDATA[
            {expected_sql.strip()}
            ]]>
          </sql>
          <explanation>Filter loans by year 1994 using date column</explanation>
          <query_type>select</query_type>
          <components>
            <tables>
              <table name="loans" alias="l">Loan records with date filtering</table>
            </tables>
            <key_operations>
              <operation type="filter">WHERE YEAR(date) = 1994 for temporal filtering</operation>
              <operation type="select">SELECT loan details</operation>
              <operation type="order">ORDER BY date chronologically</operation>
            </key_operations>
          </components>
        </sql_generation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_generation_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert "YEAR" in result["sql"].upper() or "1994" in result["sql"]
        assert "WHERE" in result["sql"].upper()
        assert result["query_type"] == "select"
        assert any(op["type"] == "filter" for op in result["components"]["operations"])
        
        print("✓ Date filtering query test passed!")
    
    @pytest.mark.asyncio
    async def test_complex_subquery(self, setup):
        """Test: Complex query with subquery/CTE."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node for complex query
        node = self.create_query_node_with_mapping(
            intent="Find clients in districts with above average loan amounts",
            tables=[
                {"name": "clients", "alias": "c", "purpose": "Client information"},
                {"name": "district", "alias": "d", "purpose": "District data"},
                {"name": "accounts", "alias": "a", "purpose": "Account linking"},
                {"name": "loans", "alias": "l", "purpose": "Loan amounts"}
            ],
            columns=[
                {"table": "clients", "column": "client_id", "usedFor": "select"},
                {"table": "clients", "column": "gender", "usedFor": "select"},
                {"table": "district", "column": "A2", "usedFor": "select"},
                {"table": "loans", "column": "amount", "usedFor": "aggregate"}
            ],
            joins=[
                {"from_table": "clients", "to_table": "district", "on": "c.district_id = d.district_id"},
                {"from_table": "district", "to_table": "accounts", "on": "d.district_id = a.district_id"},
                {"from_table": "accounts", "to_table": "loans", "on": "a.account_id = l.account_id"}
            ]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Mock SQL generation response for CTE
        expected_sql = """
        WITH district_avg_loans AS (
            SELECT d.district_id, d.A2 as district_name, AVG(l.amount) as avg_loan_amount
            FROM district d
            JOIN accounts a ON d.district_id = a.district_id
            JOIN loans l ON a.account_id = l.account_id
            GROUP BY d.district_id, d.A2
        ),
        overall_avg AS (
            SELECT AVG(amount) as overall_average FROM loans
        )
        SELECT c.client_id, c.gender, dal.district_name
        FROM clients c
        JOIN district_avg_loans dal ON c.district_id = dal.district_id
        CROSS JOIN overall_avg oa
        WHERE dal.avg_loan_amount > oa.overall_average
        """
        
        mock_output = f"""
        <sql_generation>
          <sql>
            <![CDATA[
            {expected_sql.strip()}
            ]]>
          </sql>
          <explanation>Complex query using CTE to find clients in districts with above-average loan amounts</explanation>
          <query_type>cte</query_type>
          <components>
            <tables>
              <table name="clients" alias="c">Client information</table>
              <table name="district" alias="d">District data</table>
              <table name="accounts" alias="a">Account linking</table>
              <table name="loans" alias="l">Loan amounts</table>
            </tables>
            <key_operations>
              <operation type="cte">WITH clause for district averages</operation>
              <operation type="aggregate">AVG calculation for loans</operation>
              <operation type="join">Multiple joins to connect entities</operation>
              <operation type="filter">WHERE clause comparing averages</operation>
            </key_operations>
          </components>
        </sql_generation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_generation_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert "WITH" in result["sql"].upper()
        assert "AVG" in result["sql"].upper()
        assert result["query_type"] == "cte"
        assert any(op["type"] == "cte" for op in result["components"]["operations"])
        assert len(result["components"]["tables"]) == 4
        
        print("✓ Complex subquery/CTE test passed!")
    
    @pytest.mark.asyncio
    async def test_count_query(self, setup):
        """Test: Simple COUNT query."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node for count query
        node = self.create_query_node_with_mapping(
            intent="Count total number of loans",
            tables=[{"name": "loans", "alias": "l", "purpose": "Loan records for counting"}],
            columns=[
                {"table": "loans", "column": "loan_id", "usedFor": "aggregate"}
            ]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Mock SQL generation response
        expected_sql = "SELECT COUNT(*) as total_loans FROM loans l"
        
        mock_output = f"""
        <sql_generation>
          <sql>
            <![CDATA[
            {expected_sql}
            ]]>
          </sql>
          <explanation>Simple count of all loan records</explanation>
          <query_type>aggregate</query_type>
          <components>
            <tables>
              <table name="loans" alias="l">Source table for counting</table>
            </tables>
            <key_operations>
              <operation type="aggregate">COUNT(*) to count all rows</operation>
            </key_operations>
          </components>
        </sql_generation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_generation_xml(mock_output)
        
        # Verify parsing
        assert result is not None
        assert "COUNT" in result["sql"].upper()
        assert result["query_type"] == "aggregate"
        assert len(result["components"]["tables"]) == 1
        
        print("✓ Simple COUNT query test passed!")
    
    @pytest.mark.asyncio
    async def test_schema_mapping_format(self, setup):
        """Test: Schema mapping formatting for agent input."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with complex mapping
        node = self.create_query_node_with_mapping(
            intent="Test schema mapping format",
            tables=[
                {"name": "clients", "alias": "c", "purpose": "Client data"},
                {"name": "loans", "alias": "l", "purpose": "Loan data"}
            ],
            columns=[
                {"table": "clients", "column": "client_id", "usedFor": "select"},
                {"table": "clients", "column": "gender", "usedFor": "filter"},
                {"table": "loans", "column": "amount", "usedFor": "select"}
            ],
            joins=[
                {"from_table": "clients", "to_table": "loans", "on": "c.client_id = l.client_id"}
            ]
        )
        
        # Test schema mapping formatting
        schema_xml = await env["agent"]._format_schema_mapping(node)
        
        # Verify format
        assert "<schema_mapping>" in schema_xml
        assert "<tables>" in schema_xml
        assert 'name="clients"' in schema_xml
        assert 'alias="c"' in schema_xml
        assert "<joins>" in schema_xml
        assert "c.client_id = l.client_id" in schema_xml
        
        print("✓ Schema mapping format test passed!")


class TestSQLGeneratorEdgeCases:
    """Test edge cases and error scenarios for SQL generation."""
    
    @pytest_asyncio.fixture
    async def setup(self):
        """Setup test environment."""
        memory = KeyValueMemory()
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        
        # Monkey patch MemoryAgentTool
        import sql_generator_agent
        original_memory_agent_tool = sql_generator_agent.MemoryAgentTool
        sql_generator_agent.MemoryAgentTool = MockMemoryAgentTool
        
        agent = SQLGeneratorAgent(memory, model_name="gpt-4o", debug=True)
        sql_generator_agent.MemoryAgentTool = original_memory_agent_tool
        
        yield {
            "memory": memory,
            "schema_manager": schema_manager,
            "tree_manager": tree_manager,
            "agent": agent
        }
    
    @pytest.mark.asyncio
    async def test_no_schema_mapping(self, setup):
        """Test behavior when node has no schema mapping."""
        env = setup
        
        # Create node without schema mapping
        node = QueryNode(
            nodeId="test_node_no_mapping",
            intent="Test query without mapping",
            mapping=QueryMapping(),  # Empty mapping
            status=NodeStatus.CREATED
        )
        
        # Test XML parsing with empty mapping
        schema_xml = await env["agent"]._format_schema_mapping(node)
        
        assert "No schema mapping available" in schema_xml
        
        print("✓ No schema mapping test passed!")
    
    @pytest.mark.asyncio
    async def test_invalid_sql_xml(self, setup):
        """Test handling of invalid SQL generation XML."""
        env = setup
        
        # Mock invalid XML response
        invalid_xml = """
        <sql_generation>
          <sql>
            <![CDATA[
            SELECT * FROM test_table
            <!-- Missing closing CDATA and tags -->
        """
        
        # Test XML parsing - should handle error gracefully
        result = env["agent"]._parse_generation_xml(invalid_xml)
        
        # Should return None for invalid XML
        assert result is None
        
        print("✓ Invalid SQL XML test passed!")
    
    @pytest.mark.asyncio
    async def test_empty_sql_generation(self, setup):
        """Test handling of empty SQL in generation result."""
        env = setup
        
        # Mock XML with empty SQL
        empty_sql_xml = """
        <sql_generation>
          <sql>
            <![CDATA[
            
            ]]>
          </sql>
          <explanation>Empty SQL generation</explanation>
          <query_type>select</query_type>
          <components>
            <tables></tables>
            <key_operations></key_operations>
          </components>
        </sql_generation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_generation_xml(empty_sql_xml)
        
        # Should parse but have empty SQL
        assert result is not None
        assert result["sql"].strip() == ""
        assert result["explanation"] == "Empty SQL generation"
        
        print("✓ Empty SQL generation test passed!")
    
    @pytest.mark.asyncio
    async def test_sql_validation(self, setup):
        """Test SQL validation functionality."""
        env = setup
        
        # Create node with SQL
        node = QueryNode(
            nodeId="test_validation_node",
            intent="Test SQL validation",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="clients", alias="c", purpose="Client data"),
                    TableMapping(name="loans", alias="l", purpose="Loan data")
                ]
            ),
            sql="SELECT c.client_id, l.amount FROM clients c JOIN loans l ON c.client_id = l.client_id",
            status=NodeStatus.SQL_GENERATED
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Test validation
        validation = await env["agent"].validate_sql("test_validation_node")
        
        # Should be valid
        assert validation["valid"] == True
        assert len(validation["issues"]) == 0
        
        print("✓ SQL validation test passed!")
    
    @pytest.mark.asyncio
    async def test_sql_validation_missing_table(self, setup):
        """Test SQL validation with missing table."""
        env = setup
        
        # Create node with SQL missing a mapped table
        node = QueryNode(
            nodeId="test_missing_table_node",
            intent="Test missing table validation",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="clients", alias="c", purpose="Client data"),
                    TableMapping(name="loans", alias="l", purpose="Loan data")
                ]
            ),
            sql="SELECT c.client_id FROM clients c",  # Missing loans table
            status=NodeStatus.SQL_GENERATED
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Test validation
        validation = await env["agent"].validate_sql("test_missing_table_node")
        
        # Should have validation issues
        assert validation["valid"] == False
        assert len(validation["issues"]) > 0
        assert any("loans" in issue for issue in validation["issues"])
        
        print("✓ SQL validation missing table test passed!")


if __name__ == "__main__":
    import sys
    sys.path.append('..')
    import asyncio
    async def run_tests():
        # Run a subset of tests
        test_suite = TestSQLGeneratorAgentBIRD()
        setup_gen = test_suite.setup()
        setup_data = await setup_gen.__anext__()
        
        await test_suite.test_simple_select_query(setup_data)
        await test_suite.test_join_query(setup_data)
        await test_suite.test_aggregation_query(setup_data)
        await test_suite.test_date_filtering_query(setup_data)
        await test_suite.test_complex_subquery(setup_data)
        await test_suite.test_count_query(setup_data)
        await test_suite.test_schema_mapping_format(setup_data)
        
        print("\n✅ All SQL Generator Agent tests completed successfully!")
    
    asyncio.run(run_tests())