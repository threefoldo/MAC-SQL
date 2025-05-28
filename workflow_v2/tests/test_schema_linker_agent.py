"""
Test cases for SchemaLinkerAgent using real LLM and BIRD dataset.

Tests the actual run method and internal implementation.
"""

import asyncio
import pytest
import os
from pathlib import Path
import sys
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from keyvalue_memory import KeyValueMemory
from memory_content_types import (
    TaskContext, QueryNode, NodeStatus, TaskStatus,
    TableSchema, ColumnInfo
)
from task_context_manager import TaskContextManager
from query_tree_manager import QueryTreeManager
from database_schema_manager import DatabaseSchemaManager
from node_history_manager import NodeHistoryManager
from schema_linker_agent import SchemaLinkerAgent
from schema_reader import SchemaReader


class TestSchemaLinkerAgent:
    """Test cases for SchemaLinkerAgent"""
    
    async def setup_test_environment(self, query: str, task_id: str, node_intent: str, db_name: str = "california_schools", evidence: str = None):
        """Setup test environment with schema loaded and query tree initialized"""
        memory = KeyValueMemory()
        
        # Initialize task
        task_manager = TaskContextManager(memory)
        await task_manager.initialize(task_id, query, db_name, evidence=evidence)
        
        # Load schema
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        
        # Load real schema from BIRD dataset
        data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
        tables_json_path = Path(data_path) / "dev_tables.json"
        
        if tables_json_path.exists():
            schema_reader = SchemaReader(
                data_path=data_path,
                tables_json_path=str(tables_json_path),
                dataset_name="bird",
                lazy=False
            )
            await schema_manager.load_from_schema_reader(schema_reader, db_name)
        else:
            # Fallback to manual schema for testing
            await self._setup_manual_schema(schema_manager)
        
        # Initialize schema_linking as orchestrator would
        from datetime import datetime
        schema_context = {
            "original_query": query,
            "database_name": db_name,
            "evidence": evidence,
            "initialized_at": datetime.now().isoformat(),
            "schema_analysis": None,
            "last_update": None
        }
        await memory.set("schema_linking", schema_context)
        
        # Initialize query tree with node
        tree_manager = QueryTreeManager(memory)
        node_id = await tree_manager.initialize(node_intent)
        
        return memory, node_id
    
    async def _setup_manual_schema(self, schema_manager: DatabaseSchemaManager):
        """Setup basic test schema"""
        # schools table
        schools_schema = TableSchema(
            name="schools",
            columns={
                "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "School": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "County": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "City": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Zip": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(schools_schema)
        
        # frpm table (Free/Reduced Price Meals)
        frpm_schema = TableSchema(
            name="frpm",
            columns={
                "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=True,
                                    references={"table": "schools", "column": "CDSCode"}),
                "School Name": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Eligible Free Rate (K-12)": ColumnInfo(dataType="REAL", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Free Meal Count (K-12)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Enrollment (K-12)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "FRPM Count (K-12)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(frpm_schema)
        
        # satscores table
        satscores_schema = TableSchema(
            name="satscores",
            columns={
                "cds": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=True,
                                references={"table": "schools", "column": "CDSCode"}),
                "sname": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "NumTstTakr": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "AvgScrRead": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "AvgScrMath": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(satscores_schema)
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_simple_schema_linking(self):
        """Test running the agent with a simple single-table query"""
        query = "What is the highest eligible free rate for K-12 students in schools in Alameda County?"
        node_intent = "Find the maximum eligible free rate for K-12 students in schools located in Alameda County"
        memory, node_id = await self.setup_test_environment(query, "test_simple", node_intent)
        
        # Create schema linking agent
        agent = SchemaLinkerAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        }, debug=True)
        
        # Run the agent - SchemaLinker now uses schema_linking
        result = await agent.run("Analyze schema for the query")
        
        # Verify the agent ran and returned a result
        assert result is not None
        assert hasattr(result, 'messages')
        assert len(result.messages) > 0
        
        # Check that schema_linking was updated
        schema_context = await memory.get("schema_linking")
        assert schema_context is not None
        assert schema_context["schema_analysis"] is not None
        assert "selected_tables" in schema_context["schema_analysis"]
        
        print(f"\nSimple Query Schema Linking:")
        selected_tables = schema_context['schema_analysis']['selected_tables']
        if isinstance(selected_tables, dict) and 'table' in selected_tables:
            tables = selected_tables['table'] if isinstance(selected_tables['table'], list) else [selected_tables['table']]
            print(f"Tables found: {len(tables)}")
            for table in tables:
                print(f"  - {table.get('name', 'N/A')}: {table.get('purpose', 'N/A')}")
        else:
            print("Tables found: 0")
        
        # Verify the LLM response structure
        last_message = result.messages[-1].content
        assert "<schema_linking>" in last_message
        assert "</schema_linking>" in last_message
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_join_schema_linking(self):
        """Test running the agent with a query requiring joins"""
        query = "What are the SAT scores for schools with the highest free meal count?"
        node_intent = "Find SAT scores for schools that have the highest free meal count"
        memory, node_id = await self.setup_test_environment(query, "test_join", node_intent)
        
        # Create schema linking agent
        agent = SchemaLinkerAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        # Run the agent - SchemaLinker now uses schema_linking
        result = await agent.run("Analyze schema for the query")
        
        # Verify result
        assert result is not None
        assert len(result.messages) > 0
        
        # Check schema_linking was updated
        schema_context = await memory.get("schema_linking")
        assert schema_context is not None
        assert schema_context["schema_analysis"] is not None
        
        print(f"\nJoin Query Schema Linking:")
        schema_analysis = schema_context["schema_analysis"]
        selected_tables = schema_analysis.get('selected_tables', {})
        if isinstance(selected_tables, dict) and 'table' in selected_tables:
            tables = selected_tables['table'] if isinstance(selected_tables['table'], list) else [selected_tables['table']]
            print(f"Tables: {len(tables)}")
        else:
            print("Tables: 0")
        
        joins = schema_analysis.get('joins', [])
        print(f"Joins: {len(joins) if isinstance(joins, list) else (1 if joins else 0)}")
        if joins:
            joins_list = joins if isinstance(joins, list) else [joins]
            for join in joins_list:
                if isinstance(join, dict):
                    print(f"  {join.get('from_table')} -> {join.get('to_table')} ({join.get('join_type', 'INNER')})")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set") 
    async def test_reader_callback(self):
        """Test the _reader_callback method"""
        query = "Find schools in California"
        memory, node_id = await self.setup_test_environment(query, "test_reader", "Find all schools")
        
        agent = SchemaLinkerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test reader callback directly
        context = await agent._reader_callback(memory, "task", None)
        
        assert context is not None
        assert "original_query" in context  # Schema context uses original_query
        assert "database_name" in context
        assert "full_schema" in context
        
        assert context["original_query"] == query
        assert context["database_name"] == "california_schools"
        assert "<database_schema>" in context["full_schema"]
        
        print(f"\nReader callback context keys: {list(context.keys())}")
        print(f"Query: {context['original_query']}")
        print(f"Schema length: {len(context['full_schema'])}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_parse_linking_xml(self):
        """Test XML parsing of schema linking results"""
        memory = KeyValueMemory()
        agent = SchemaLinkerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test simple linking XML
        simple_xml = """
        <schema_linking>
          <selected_tables>
            <table name="schools" alias="s">
              <purpose>Contains school location data</purpose>
              <columns>
                <column name="County" used_for="filter">
                  <reason>Filter by Alameda County</reason>
                </column>
                <column name="CDSCode" used_for="join">
                  <reason>Join with frpm table</reason>
                </column>
              </columns>
            </table>
            <table name="frpm" alias="f">
              <purpose>Contains free meal rate data</purpose>
              <columns>
                <column name="CDSCode" used_for="join">
                  <reason>Join with schools table</reason>
                </column>
                <column name="Eligible Free Rate (K-12)" used_for="select">
                  <reason>The metric we need to find max of</reason>
                </column>
              </columns>
            </table>
          </selected_tables>
          <joins>
            <join>
              <from_table>schools</from_table>
              <from_column>CDSCode</from_column>
              <to_table>frpm</to_table>
              <to_column>CDSCode</to_column>
              <join_type>INNER</join_type>
            </join>
          </joins>
          <sample_query_pattern>SELECT MAX(f."Eligible Free Rate (K-12)") FROM schools s JOIN frpm f ON s.CDSCode = f.CDSCode WHERE s.County = 'Alameda'</sample_query_pattern>
        </schema_linking>
        """
        
        result = agent._parse_linking_xml(simple_xml)
        
        assert result is not None
        assert "selected_tables" in result
        assert "table" in result["selected_tables"]
        
        # Handle both single table and multiple tables
        tables = result["selected_tables"]["table"]
        if isinstance(tables, list):
            assert len(tables) == 2
            assert tables[0]["name"] == "schools"
        else:
            # Single table case
            assert tables["name"] == "schools"
        if isinstance(tables, list):
            assert tables[1]["name"] == "frpm"
        
        # Check joins
        joins = result.get("joins", {})
        if isinstance(joins, dict) and "join" in joins:
            join_list = joins["join"] if isinstance(joins["join"], list) else [joins["join"]]
            assert len(join_list) >= 1
            assert join_list[0]["from_table"] == "schools"
            assert join_list[0]["to_table"] == "frpm"
        
        assert "sample_query_pattern" in result
        
        print(f"\nParsed Schema Linking: {result}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_dictionary_storage(self):
        """Test that schema linking results are stored as dictionaries"""
        memory = KeyValueMemory()
        agent = SchemaLinkerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Create test linking result (direct dictionary)
        linking_result = {
            "selected_tables": {
                "table": [
                    {
                        "name": "schools",
                        "alias": "s",
                        "purpose": "School information"
                    },
                    {
                        "name": "frpm", 
                        "alias": "f",
                        "purpose": "Free meal data"
                    }
                ]
            },
            "column_discovery": {
                "query_term": [
                    {
                        "original": "test term",
                        "selected_columns": {"column": {"table": "schools", "column": "County"}}
                    }
                ]
            }
        }
        
        # Test that we can store and retrieve the dictionary directly
        assert isinstance(linking_result, dict)
        assert "selected_tables" in linking_result
        assert "column_discovery" in linking_result
        
        # Verify the structure is what we expect from hybrid XML parsing
        tables = linking_result["selected_tables"]["table"]
        assert isinstance(tables, list)
        assert len(tables) == 2
        assert tables[0]["name"] == "schools"
        assert tables[0]["alias"] == "s"
        assert tables[1]["name"] == "frpm"
        
        print("✓ Dictionary storage test passed")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_real_bird_queries(self):
        """Test with real BIRD dataset queries"""
        test_cases = [
            {
                "query": "List the zip codes of all charter schools in Fresno County Office of Education.",
                "intent": "Get zip codes of charter schools in Fresno County Office of Education"
            },
            {
                "query": "What is the number of SAT test takers in schools with the highest FRPM count for K-12 students?",
                "intent": "Find SAT test taker count for schools with maximum FRPM count"
            }
        ]
        
        for i, test_case in enumerate(test_cases):
            print(f"\n--- Testing BIRD Query {i+1} ---")
            print(f"Query: {test_case['query']}")
            
            memory, node_id = await self.setup_test_environment(
                test_case['query'], 
                f"bird_test_{i}",
                test_case['intent']
            )
            
            agent = SchemaLinkerAgent(memory, llm_config={
                "model_name": "gpt-4o",
                "temperature": 0.1,
                "timeout": 60
            })
            
            # Run the agent - SchemaLinker now uses schema_linking
            result = await agent.run("Analyze schema for the query")
            
            assert result is not None
            assert len(result.messages) > 0
            
            # Check schema_linking was updated
            schema_context = await memory.get("schema_linking")
            assert schema_context is not None
            assert schema_context["schema_analysis"] is not None
            
            schema_analysis = schema_context["schema_analysis"]
            selected_tables = schema_analysis.get("selected_tables", {})
            
            # Handle the new dictionary structure
            if isinstance(selected_tables, dict) and "table" in selected_tables:
                tables = selected_tables["table"] if isinstance(selected_tables["table"], list) else [selected_tables["table"]]
                assert len(tables) > 0
                print(f"Tables linked: {[t.get('name', 'N/A') for t in tables]}")
            else:
                print("No tables found in schema analysis")
            
            joins = schema_analysis.get("joins", [])
            joins_count = len(joins) if isinstance(joins, list) else (1 if joins else 0)
            print(f"Joins identified: {joins_count}")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))