"""
Comprehensive test cases for SQL Evaluator Agent using actual BIRD dataset examples.

This test suite covers:
1. Successful SQL execution scenarios
2. SQL execution error handling
3. Result evaluation and analysis
4. Performance analysis
5. Improvement suggestions
6. Edge cases and error handling

Tests use real database schemas and SQL queries to ensure realistic testing
of the SQL execution and evaluation functionality.
"""

import src.pytest as pytest
import src.pytest_asyncio as pytest_asyncio
from src.typing import Dict, List, Optional, Any
from unittest.mock import Mock, AsyncMock

from src.memory import KeyValueMemory
from src.database_schema_manager import DatabaseSchemaManager
from src.query_tree_manager import QueryTreeManager
from src.sql_evaluator_agent import SQLEvaluatorAgent
from src.sql_executor import SQLExecutor
from src.memory_content_types import (
    TableSchema, ColumnInfo, QueryNode, QueryMapping, 
    TableMapping, ColumnMapping, NodeStatus, ExecutionResult
)


class MockSQLExecutor:
    """Mock SQL executor for testing without database dependencies."""
    def __init__(self, data_path: str = "", dataset_name: str = "bird"):
        self.data_path = data_path
        self.dataset_name = dataset_name
        self._mock_results = {}
        self._execution_count = 0
        
    def set_mock_result(self, sql: str, result: Dict[str, Any]):
        """Set a mock result for a specific SQL query."""
        self._mock_results[sql.strip()] = result
        
    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Mock execute method that returns predefined results."""
        self._execution_count += 1
        sql_key = sql.strip()
        
        # Return mock result if available
        if sql_key in self._mock_results:
            result = self._mock_results[sql_key]
            if result.get("success", True):
                return result.get("data", [])
            else:
                raise Exception(result.get("error", "SQL execution failed"))
        
        # Default successful result
        return [{"count": 10}]


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
        <execution_evaluation>
          <status>success</status>
          <result_analysis>
            <matches_intent>true</matches_intent>
            <explanation>Results match the query intent</explanation>
            <data_quality>good</data_quality>
            <anomalies>None detected</anomalies>
          </result_analysis>
          <performance_analysis>
            <execution_time_assessment>fast</execution_time_assessment>
            <bottlenecks>None identified</bottlenecks>
          </performance_analysis>
          <improvements>
          </improvements>
          <final_verdict>
            <usable>true</usable>
            <confidence>high</confidence>
            <summary>Query executed successfully with good results</summary>
          </final_verdict>
        </execution_evaluation>
        """
        
        # Call post_callback if it exists  
        if self.post_callback:
            await self.post_callback(output, inputs)
            
        return output


class TestSQLEvaluatorAgentBIRD:
    """Test SQL execution with comprehensive BIRD dataset scenarios."""
    
    @pytest_asyncio.fixture
    async def setup(self):
        """Setup test environment with memory and managers."""
        memory = KeyValueMemory()
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        
        # Create mock SQL executor
        mock_sql_executor = MockSQLExecutor()
        
        # Monkey patch MemoryAgentTool before creating the agent
        import sql_evaluator_agent
        original_memory_agent_tool = sql_evaluator_agent.MemoryAgentTool
        sql_evaluator_agent.MemoryAgentTool = MockMemoryAgentTool
        
        # Create SQL evaluator agent with debug enabled
        agent = SQLEvaluatorAgent(memory, mock_sql_executor, model_name="gpt-4o", debug=True)
        
        # Restore original class
        sql_evaluator_agent.MemoryAgentTool = original_memory_agent_tool
        
        yield {
            "memory": memory,
            "schema_manager": schema_manager,
            "tree_manager": tree_manager,
            "agent": agent,
            "sql_executor": mock_sql_executor
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
            ],
            metadata={"rowCount": 1000, "indexes": ["client_id", "district_id"]}
        )
        await schema_manager.add_table(clients_schema)
        
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
                "status": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            },
            sampleData=[
                {"loan_id": 4959, "account_id": 2, "date": "1994-01-05", "amount": 80952, 
                 "duration": 24, "status": "A"},
                {"loan_id": 4960, "account_id": 19, "date": "1996-04-29", "amount": 30276,
                 "duration": 12, "status": "B"}
            ],
            metadata={"rowCount": 500, "indexes": ["loan_id", "account_id"]}
        )
        await schema_manager.add_table(loans_schema)
    
    def create_query_node_with_sql(self, intent: str, sql: str, tables: List[str] = None) -> QueryNode:
        """Helper to create a QueryNode with SQL."""
        # Create basic mapping
        table_mappings = []
        if tables:
            for table in tables:
                table_mappings.append(TableMapping(name=table, purpose=f"Used in {intent}"))
        
        mapping = QueryMapping(tables=table_mappings)
        
        # Create query node
        node = QueryNode(
            nodeId="test_node_exec",
            intent=intent,
            mapping=mapping,
            sql=sql,
            status=NodeStatus.SQL_GENERATED
        )
        
        return node
    
    @pytest.mark.asyncio
    async def test_successful_sql_execution(self, setup):
        """Test: Successful SQL execution with good results."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with SQL
        sql = "SELECT gender, COUNT(*) as count FROM clients GROUP BY gender"
        node = self.create_query_node_with_sql(
            intent="Count clients by gender",
            sql=sql,
            tables=["clients"]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Set mock SQL execution result
        env["sql_executor"].set_mock_result(sql, {
            "success": True,
            "data": [
                {"gender": "F", "count": 450},
                {"gender": "M", "count": 550}
            ],
            "row_count": 2
        })
        
        # Mock evaluation response
        env["agent"].agent._mock_response = """
        <execution_evaluation>
          <status>success</status>
          <result_analysis>
            <matches_intent>true</matches_intent>
            <explanation>Query successfully counts clients by gender, returning appropriate counts for both genders</explanation>
            <data_quality>good</data_quality>
            <anomalies>None detected - data distribution seems reasonable</anomalies>
          </result_analysis>
          <performance_analysis>
            <execution_time_assessment>fast</execution_time_assessment>
            <bottlenecks>None identified for this simple aggregation</bottlenecks>
          </performance_analysis>
          <improvements>
            <suggestion priority="low">
              <type>optimization</type>
              <description>Consider adding index on gender column for better performance</description>
              <example>CREATE INDEX idx_clients_gender ON clients(gender)</example>
            </suggestion>
          </improvements>
          <final_verdict>
            <usable>true</usable>
            <confidence>high</confidence>
            <summary>Query executed successfully with accurate gender-based counts</summary>
          </final_verdict>
        </execution_evaluation>
        """
        
        # Execute and evaluate
        result = await env["agent"].execute_and_evaluate("test_node_exec")
        
        # Verify execution
        assert result["execution"]["success"] == True
        assert result["execution"]["row_count"] == 2
        assert result["execution"]["error"] is None
        
        # Verify evaluation
        evaluation = result["evaluation"]
        assert evaluation["status"] == "success"
        assert evaluation["result_analysis"]["matches_intent"] == True
        assert evaluation["result_analysis"]["data_quality"] == "good"
        assert evaluation["performance_analysis"]["execution_time_assessment"] == "fast"
        assert evaluation["final_verdict"]["usable"] == True
        assert len(evaluation["improvements"]) == 1
        
        print("✓ Successful SQL execution test passed!")
    
    @pytest.mark.asyncio
    async def test_sql_execution_error(self, setup):
        """Test: SQL execution with syntax error."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with invalid SQL
        sql = "SELECT * FROM nonexistent_table WHERE invalid syntax"
        node = self.create_query_node_with_sql(
            intent="Invalid query test",
            sql=sql,
            tables=["clients"]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Set mock SQL execution error
        env["sql_executor"].set_mock_result(sql, {
            "success": False,
            "error": "SQLite error: no such table: nonexistent_table",
            "data": [],
            "row_count": 0
        })
        
        # Mock evaluation response for error
        env["agent"].agent._mock_response = """
        <execution_evaluation>
          <status>failure</status>
          <result_analysis>
            <matches_intent>false</matches_intent>
            <explanation>Query failed to execute due to table not existing</explanation>
            <data_quality>poor</data_quality>
            <anomalies>SQL syntax error preventing execution</anomalies>
          </result_analysis>
          <performance_analysis>
            <execution_time_assessment>fast</execution_time_assessment>
            <bottlenecks>Query failed before performance could be assessed</bottlenecks>
          </performance_analysis>
          <improvements>
            <suggestion priority="high">
              <type>rewrite</type>
              <description>Fix table name and SQL syntax</description>
              <example>SELECT * FROM clients WHERE condition</example>
            </suggestion>
          </improvements>
          <final_verdict>
            <usable>false</usable>
            <confidence>high</confidence>
            <summary>Query contains errors and cannot be used</summary>
          </final_verdict>
        </execution_evaluation>
        """
        
        # Execute and evaluate
        result = await env["agent"].execute_and_evaluate("test_node_exec")
        
        # Verify execution failure
        assert result["execution"]["success"] == False
        assert result["execution"]["row_count"] == 0
        assert result["execution"]["error"] is not None
        
        # Verify evaluation
        evaluation = result["evaluation"]
        assert evaluation["status"] == "failure"
        assert evaluation["result_analysis"]["matches_intent"] == False
        assert evaluation["final_verdict"]["usable"] == False
        assert any(s["priority"] == "high" for s in evaluation["improvements"])
        
        print("✓ SQL execution error test passed!")
    
    @pytest.mark.asyncio
    async def test_performance_analysis(self, setup):
        """Test: Performance analysis for slow query."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with potentially slow SQL
        sql = "SELECT l.*, c.* FROM loans l JOIN clients c ON l.account_id = c.client_id ORDER BY l.amount DESC"
        node = self.create_query_node_with_sql(
            intent="Get all loan details with client info sorted by amount",
            sql=sql,
            tables=["loans", "clients"]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Set mock SQL execution result (slow)
        env["sql_executor"].set_mock_result(sql, {
            "success": True,
            "data": [
                {"loan_id": 1, "amount": 100000, "client_id": 1, "gender": "F"},
                {"loan_id": 2, "amount": 95000, "client_id": 2, "gender": "M"}
            ],
            "row_count": 500
        })
        
        # Mock evaluation response for performance issues
        env["agent"].agent._mock_response = """
        <execution_evaluation>
          <status>success</status>
          <result_analysis>
            <matches_intent>true</matches_intent>
            <explanation>Query returns loan details with client info as requested</explanation>
            <data_quality>good</data_quality>
            <anomalies>Large result set may impact performance</anomalies>
          </result_analysis>
          <performance_analysis>
            <execution_time_assessment>slow</execution_time_assessment>
            <bottlenecks>JOIN without proper indexing and ORDER BY on large dataset</bottlenecks>
          </performance_analysis>
          <improvements>
            <suggestion priority="high">
              <type>index</type>
              <description>Add index on join columns for better JOIN performance</description>
              <example>CREATE INDEX idx_loans_account_id ON loans(account_id)</example>
            </suggestion>
            <suggestion priority="medium">
              <type>optimization</type>
              <description>Add index on amount column for ORDER BY performance</description>
              <example>CREATE INDEX idx_loans_amount ON loans(amount DESC)</example>
            </suggestion>
            <suggestion priority="low">
              <type>rewrite</type>
              <description>Consider adding LIMIT clause if full result set not needed</description>
              <example>Add LIMIT 100 to reduce result size</example>
            </suggestion>
          </improvements>
          <final_verdict>
            <usable>true</usable>
            <confidence>medium</confidence>
            <summary>Query works but has performance issues that should be addressed</summary>
          </final_verdict>
        </execution_evaluation>
        """
        
        # Execute and evaluate
        result = await env["agent"].execute_and_evaluate("test_node_exec")
        
        # Verify execution
        assert result["execution"]["success"] == True
        assert result["execution"]["row_count"] == 2  # Based on mock data
        
        # Verify performance analysis
        evaluation = result["evaluation"]
        assert evaluation["status"] == "success"
        assert evaluation["performance_analysis"]["execution_time_assessment"] == "slow"
        assert "JOIN" in evaluation["performance_analysis"]["bottlenecks"]
        assert len(evaluation["improvements"]) >= 3
        
        # Check improvement priorities
        priorities = [s["priority"] for s in evaluation["improvements"]]
        assert "high" in priorities
        assert "medium" in priorities
        
        print("✓ Performance analysis test passed!")
    
    @pytest.mark.asyncio
    async def test_data_quality_analysis(self, setup):
        """Test: Data quality analysis with anomalies."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with SQL that might return unexpected results
        sql = "SELECT AVG(amount) as avg_amount, COUNT(*) as count FROM loans WHERE amount > 1000000"
        node = self.create_query_node_with_sql(
            intent="Average amount for very large loans",
            sql=sql,
            tables=["loans"]
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Set mock SQL execution result with suspicious data
        env["sql_executor"].set_mock_result(sql, {
            "success": True,
            "data": [
                {"avg_amount": None, "count": 0}  # No results found
            ],
            "row_count": 1
        })
        
        # Mock evaluation response for data quality issues
        env["agent"].agent._mock_response = """
        <execution_evaluation>
          <status>partial</status>
          <result_analysis>
            <matches_intent>false</matches_intent>
            <explanation>Query executed but returned no results for very large loans, which may indicate threshold is too high</explanation>
            <data_quality>poor</data_quality>
            <anomalies>Zero results for loan query suggests either no large loans exist or threshold is inappropriate</anomalies>
          </result_analysis>
          <performance_analysis>
            <execution_time_assessment>fast</execution_time_assessment>
            <bottlenecks>None - query completed quickly due to no matching records</bottlenecks>
          </performance_analysis>
          <improvements>
            <suggestion priority="high">
              <type>validation</type>
              <description>Review loan amount threshold - 1,000,000 may be too high for this dataset</description>
              <example>Try SELECT MAX(amount) FROM loans to understand data range</example>
            </suggestion>
            <suggestion priority="medium">
              <type>rewrite</type>
              <description>Use a more appropriate threshold based on data distribution</description>
              <example>SELECT AVG(amount) FROM loans WHERE amount > 50000</example>
            </suggestion>
          </improvements>
          <final_verdict>
            <usable>false</usable>
            <confidence>medium</confidence>
            <summary>Query needs adjustment as current threshold yields no results</summary>
          </final_verdict>
        </execution_evaluation>
        """
        
        # Execute and evaluate
        result = await env["agent"].execute_and_evaluate("test_node_exec")
        
        # Verify execution
        assert result["execution"]["success"] == True
        assert result["execution"]["row_count"] == 1
        
        # Verify data quality analysis
        evaluation = result["evaluation"]
        assert evaluation["status"] == "partial"
        assert evaluation["result_analysis"]["matches_intent"] == False
        assert evaluation["result_analysis"]["data_quality"] == "poor"
        assert "threshold" in evaluation["result_analysis"]["anomalies"].lower()
        assert evaluation["final_verdict"]["usable"] == False
        
        print("✓ Data quality analysis test passed!")
    
    @pytest.mark.asyncio
    async def test_xml_parsing(self, setup):
        """Test: XML parsing of evaluation results."""
        env = setup
        
        # Test XML parsing directly
        mock_xml = """
        <execution_evaluation>
          <status>success</status>
          <result_analysis>
            <matches_intent>true</matches_intent>
            <explanation>Test explanation</explanation>
            <data_quality>good</data_quality>
            <anomalies>No issues</anomalies>
          </result_analysis>
          <performance_analysis>
            <execution_time_assessment>acceptable</execution_time_assessment>
            <bottlenecks>Minor JOIN optimization needed</bottlenecks>
          </performance_analysis>
          <improvements>
            <suggestion priority="high">
              <type>index</type>
              <description>Add missing index</description>
              <example>CREATE INDEX test_idx ON table(col)</example>
            </suggestion>
            <suggestion priority="low">
              <type>optimization</type>
              <description>Minor optimization</description>
              <example>Use LIMIT clause</example>
            </suggestion>
          </improvements>
          <final_verdict>
            <usable>true</usable>
            <confidence>high</confidence>
            <summary>Overall good query</summary>
          </final_verdict>
        </execution_evaluation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_evaluation_xml(mock_xml)
        
        # Verify parsing
        assert result is not None
        assert result["status"] == "success"
        assert result["result_analysis"]["matches_intent"] == True
        assert result["result_analysis"]["explanation"] == "Test explanation"
        assert result["performance_analysis"]["execution_time_assessment"] == "acceptable"
        assert len(result["improvements"]) == 2
        assert result["improvements"][0]["priority"] == "high"
        assert result["improvements"][1]["priority"] == "low"
        assert result["final_verdict"]["usable"] == True
        assert result["final_verdict"]["confidence"] == "high"
        
        print("✓ XML parsing test passed!")
    
    @pytest.mark.asyncio
    async def test_execution_summary(self, setup):
        """Test: Execution summary generation."""
        env = setup
        await self.setup_financial_database(env["schema_manager"])
        
        # Create node with execution result
        node = QueryNode(
            nodeId="test_summary_node",
            intent="Test summary generation",
            mapping=QueryMapping(),
            sql="SELECT COUNT(*) FROM clients",
            status=NodeStatus.EXECUTED_SUCCESS,
            executionResult=ExecutionResult(
                data=[{"count": 1000}],
                rowCount=1,
                error=None
            )
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Store mock evaluation
        evaluation = {
            "status": "success",
            "result_analysis": {"matches_intent": True},
            "final_verdict": {"usable": True},
            "improvements": [
                {"priority": "high", "type": "index"},
                {"priority": "medium", "type": "optimization"}
            ]
        }
        await env["memory"].set("sql_evaluation_test_summary_node", evaluation)
        
        # Get execution summary
        summary = await env["agent"].get_execution_summary("test_summary_node")
        
        # Verify summary
        assert summary["node_id"] == "test_summary_node"
        assert summary["intent"] == "Test summary generation"
        assert summary["has_sql"] == True
        assert summary["status"] == "executed_success"
        assert summary["execution"]["row_count"] == 1
        assert summary["execution"]["has_error"] == False
        assert summary["evaluation"]["status"] == "success"
        assert summary["evaluation"]["matches_intent"] == True
        assert summary["evaluation"]["usable"] == True
        assert summary["evaluation"]["improvement_count"] == 2
        assert summary["evaluation"]["high_priority_improvements"] == 1
        
        print("✓ Execution summary test passed!")


class TestSQLExecutorEdgeCases:
    """Test edge cases and error scenarios for SQL execution."""
    
    @pytest_asyncio.fixture
    async def setup(self):
        """Setup test environment."""
        memory = KeyValueMemory()
        schema_manager = DatabaseSchemaManager(memory)
        tree_manager = QueryTreeManager(memory)
        
        mock_sql_executor = MockSQLExecutor()
        
        # Monkey patch MemoryAgentTool
        import sql_evaluator_agent
        original_memory_agent_tool = sql_evaluator_agent.MemoryAgentTool
        sql_evaluator_agent.MemoryAgentTool = MockMemoryAgentTool
        
        agent = SQLEvaluatorAgent(memory, mock_sql_executor, model_name="gpt-4o", debug=True)
        sql_evaluator_agent.MemoryAgentTool = original_memory_agent_tool
        
        yield {
            "memory": memory,
            "schema_manager": schema_manager,
            "tree_manager": tree_manager,
            "agent": agent,
            "sql_executor": mock_sql_executor
        }
    
    @pytest.mark.asyncio
    async def test_node_without_sql(self, setup):
        """Test behavior when node has no SQL to execute."""
        env = setup
        
        # Create node without SQL
        node = QueryNode(
            nodeId="test_no_sql_node",
            intent="Test node without SQL",
            mapping=QueryMapping(),
            status=NodeStatus.CREATED
        )
        
        # Initialize tree and add node
        await env["tree_manager"].initialize("test query")
        await env["tree_manager"].add_node(node)
        
        # Try to execute
        result = await env["agent"].execute_and_evaluate("test_no_sql_node")
        
        # Should return error
        assert "error" in result
        assert "no SQL to execute" in result["error"]
        
        print("✓ Node without SQL test passed!")
    
    @pytest.mark.asyncio
    async def test_invalid_evaluation_xml(self, setup):
        """Test handling of invalid evaluation XML."""
        env = setup
        
        # Test invalid XML
        invalid_xml = """
        <execution_evaluation>
          <status>success
          <!-- Missing closing tags -->
        """
        
        # Test XML parsing - should handle error gracefully
        result = env["agent"]._parse_evaluation_xml(invalid_xml)
        
        # Should return None for invalid XML
        assert result is None
        
        print("✓ Invalid evaluation XML test passed!")
    
    @pytest.mark.asyncio
    async def test_empty_improvements(self, setup):
        """Test evaluation with no improvement suggestions."""
        env = setup
        
        # Test XML with empty improvements
        xml_no_improvements = """
        <execution_evaluation>
          <status>success</status>
          <result_analysis>
            <matches_intent>true</matches_intent>
            <explanation>Perfect query</explanation>
            <data_quality>good</data_quality>
            <anomalies>None</anomalies>
          </result_analysis>
          <performance_analysis>
            <execution_time_assessment>fast</execution_time_assessment>
            <bottlenecks>None</bottlenecks>
          </performance_analysis>
          <improvements>
          </improvements>
          <final_verdict>
            <usable>true</usable>
            <confidence>high</confidence>
            <summary>Optimal query</summary>
          </final_verdict>
        </execution_evaluation>
        """
        
        # Test XML parsing
        result = env["agent"]._parse_evaluation_xml(xml_no_improvements)
        
        # Should parse successfully with empty improvements
        assert result is not None
        assert result["status"] == "success"
        assert len(result["improvements"]) == 0
        assert result["final_verdict"]["usable"] == True
        
        print("✓ Empty improvements test passed!")
    
    @pytest.mark.asyncio
    async def test_improvement_suggestions_retrieval(self, setup):
        """Test retrieval of improvement suggestions."""
        env = setup
        
        # Store mock evaluation with improvements
        evaluation = {
            "improvements": [
                {
                    "priority": "high",
                    "type": "index",
                    "description": "Add index on frequently queried column",
                    "example": "CREATE INDEX idx_test ON table(col)"
                },
                {
                    "priority": "medium", 
                    "type": "optimization",
                    "description": "Use more efficient JOIN approach",
                    "example": "Use EXISTS instead of JOIN"
                }
            ]
        }
        await env["memory"].set("sql_evaluation_test_node", evaluation)
        
        # Get improvement suggestions
        suggestions = await env["agent"].get_improvement_suggestions("test_node")
        
        # Verify suggestions
        assert len(suggestions) == 2
        assert suggestions[0]["priority"] == "high"
        assert suggestions[0]["type"] == "index"
        assert suggestions[1]["priority"] == "medium"
        assert suggestions[1]["type"] == "optimization"
        
        print("✓ Improvement suggestions retrieval test passed!")


if __name__ == "__main__":
    import sys
    sys.path.append('..')
    import asyncio
    async def run_tests():
        # Run a subset of tests
        test_suite = TestSQLEvaluatorAgentBIRD()
        setup_gen = test_suite.setup()
        setup_data = await setup_gen.__anext__()
        
        await test_suite.test_successful_sql_execution(setup_data)
        await test_suite.test_sql_execution_error(setup_data)
        await test_suite.test_performance_analysis(setup_data)
        await test_suite.test_data_quality_analysis(setup_data)
        await test_suite.test_xml_parsing(setup_data)
        await test_suite.test_execution_summary(setup_data)
        
        print("\n✅ All SQL Executor Agent tests completed successfully!")
    
    asyncio.run(run_tests())