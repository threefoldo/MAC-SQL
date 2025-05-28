"""
Test cases for SQLGeneratorAgent using real LLM and BIRD dataset.

Tests the actual run method and internal implementation.
"""

import asyncio
import pytest
import os
from pathlib import Path
import sys
import logging
from typing import Dict, List, Optional, Any, Tuple
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
from sql_generator_agent import SQLGeneratorAgent
from schema_reader import SchemaReader


class TestSQLGeneratorAgent:
    """Test cases for SQLGeneratorAgent"""
    
    async def setup_test_environment(self, query: str, task_id: str, db_name: str = "california_schools"):
        """Setup test environment with schema loaded"""
        memory = KeyValueMemory()
        
        # Initialize task
        task_manager = TaskContextManager(memory)
        await task_manager.initialize(task_id, query, db_name)
        
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
        
        return memory
    
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
                "Enrollment (K-12)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
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
    
    async def create_test_node_with_schema_linking(self, memory: KeyValueMemory, intent: str, 
                                           tables: List[str], columns: List[Tuple[str, str]], 
                                           joins: Optional[List[Dict]] = None) -> str:
        """Create a test node with pre-defined schema linking results"""
        tree_manager = QueryTreeManager(memory)
        
        # Create node
        node_id = await tree_manager.initialize(intent)
        
        # Create schema linking result as dictionary (as SchemaLinkerAgent would)
        schema_linking = {
            "selected_tables": [],
            "selected_columns": [],
            "joins": []
        }
        
        # Add tables
        for table_name in tables:
            schema_linking["selected_tables"].append({
                "name": table_name,
                "purpose": f"Table {table_name} for the query"
            })
        
        # Add columns
        for table, column in columns:
            schema_linking["selected_columns"].append({
                "table": table,
                "column": column,
                "used_for": "select"
            })
        
        # Add joins if provided
        if joins:
            for join_info in joins:
                schema_linking["joins"].append({
                    "from_table": join_info["from_table"],
                    "to_table": join_info["to_table"],
                    "on": join_info["on"]
                })
        
        # Update node with schema linking results in schema_linking
        await tree_manager.update_node(node_id, {"schema_linking": schema_linking})
        
        # Also update schema_linking as SchemaLinkerAgent would
        schema_context = await memory.get("schema_linking") or {}
        schema_context["schema_analysis"] = schema_linking
        schema_context["last_update"] = "2024-01-01T00:00:00"
        await memory.set("schema_linking", schema_context)
        
        # Set as current node for SQL generation
        await tree_manager.set_current_node_id(node_id)
        
        return node_id
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_simple_sql_generation(self):
        """Test running the agent with a simple query"""
        query = "What is the highest eligible free rate for K-12 students in schools in Alameda County?"
        memory = await self.setup_test_environment(query, "test_simple")
        
        # Create a node with mapping
        node_id = await self.create_test_node_with_schema_linking(
            memory,
            intent="Find the maximum eligible free rate for K-12 students in schools located in Alameda County",
            tables=["schools", "frpm"],
            columns=[("schools", "County"), ("frpm", "Eligible Free Rate (K-12)")],
            joins=[{
                "from_table": "schools",
                "to_table": "frpm",
                "on": "schools.CDSCode = frpm.CDSCode"
            }]
        )
        
        # Create SQL generator
        agent = SQLGeneratorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        }, debug=True)
        
        # Run the agent - SQLGenerator uses current_node_id from tree manager
        result = await agent.run("Generate SQL for current node")
        
        # Verify the agent ran and returned a result
        assert result is not None
        assert hasattr(result, 'messages')
        assert len(result.messages) > 0
        
        # Check that node was updated with SQL
        tree_manager = QueryTreeManager(memory)
        node = await tree_manager.get_node(node_id)
        assert node is not None
        assert node.generation is not None
        assert "sql" in node.generation
        assert node.generation["sql"] is not None
        assert "SELECT" in node.generation["sql"].upper()
        
        print(f"\nGenerated SQL:")
        print(node.generation["sql"])
        
        # Verify the LLM response structure
        last_message = result.messages[-1].content
        assert "<sql_generation>" in last_message or "```sql" in last_message
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_aggregation_sql_generation(self):
        """Test running the agent with an aggregation query"""
        query = "What is the average SAT math score for schools with more than 500 students?"
        memory = await self.setup_test_environment(query, "test_aggregation")
        
        # Create a node with mapping
        node_id = await self.create_test_node_with_schema_linking(
            memory,
            intent="Calculate average SAT math score for schools with enrollment over 500",
            tables=["satscores", "frpm"],
            columns=[("satscores", "AvgScrMath"), ("frpm", "Enrollment (K-12)")],
            joins=[{
                "from_table": "satscores",
                "to_table": "frpm",
                "on": "satscores.cds = frpm.CDSCode"
            }]
        )
        
        # Create SQL generator
        agent = SQLGeneratorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        # Run the agent - SQLGenerator uses current_node_id from tree manager
        result = await agent.run("Generate SQL for current node")
        
        # Verify result
        assert result is not None
        assert len(result.messages) > 0
        
        # Check generated SQL
        tree_manager = QueryTreeManager(memory)
        node = await tree_manager.get_node(node_id)
        assert node.generation is not None
        assert "sql" in node.generation
        assert node.generation["sql"] is not None
        assert "AVG" in node.generation["sql"].upper()
        assert "WHERE" in node.generation["sql"].upper()
        
        print(f"\nAggregation SQL:")
        print(node.generation["sql"])
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set") 
    async def test_reader_callback(self):
        """Test the _reader_callback method"""
        query = "Find schools in California"
        memory = await self.setup_test_environment(query, "test_reader")
        
        # Create a test node
        node_id = await self.create_test_node_with_schema_linking(
            memory,
            intent="Find all schools",
            tables=["schools"],
            columns=[("schools", "School")]
        )
        
        agent = SQLGeneratorAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test reader callback directly
        context = await agent._reader_callback(memory, "task", None)
        
        assert context is not None
        assert "current_node" in context
        
        # Parse the current_node JSON to check content
        import json
        current_node = json.loads(context["current_node"])
        assert current_node["intent"] == "Find all schools"
        
        # The schema linking info should be in schema_linking
        assert "schema_linking" in current_node
        assert "selected_tables" in current_node["schema_linking"]
        
        print(f"\nReader callback context keys: {list(context.keys())}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_parse_generation_xml(self):
        """Test XML parsing of SQL generation results"""
        memory = KeyValueMemory()
        agent = SQLGeneratorAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test SQL XML
        sql_xml = """
        <sql_generation>
          <query_type>aggregation</query_type>
          <sql>
            SELECT MAX(f."Eligible Free Rate (K-12)")
            FROM schools s
            JOIN frpm f ON s.CDSCode = f.CDSCode
            WHERE s.County = 'Alameda'
          </sql>
          <explanation>
            This query finds the maximum eligible free rate for K-12 students
            in schools located in Alameda County by joining the schools and frpm tables.
          </explanation>
          <considerations>
            Using MAX aggregation function to find the highest rate.
          </considerations>
        </sql_generation>
        """
        
        result = agent._parse_generation_xml(sql_xml)
        
        assert result is not None
        assert result["sql"] is not None
        assert "SELECT MAX" in result["sql"]
        assert "JOIN frpm" in result["sql"]
        assert result["explanation"] is not None
        assert "maximum eligible free rate" in result["explanation"]
        assert result["query_type"] == "aggregation"
        
        print(f"\nParsed result: {result}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_sql_for_child_nodes(self):
        """Test SQL generation for nodes with child queries"""
        memory = await self.setup_test_environment("Complex query", "test_children")
        
        tree_manager = QueryTreeManager(memory)
        
        # Create parent node
        parent_id = await tree_manager.initialize("Find average scores for top schools")
        
        # Create child nodes
        child1 = QueryNode(
            nodeId="child1",
            intent="Find top 10 schools by enrollment",
            parentId=parent_id
        )
        # Add SQL generation result
        child1.generation = {
            "sql": "SELECT CDSCode FROM frpm ORDER BY \"Enrollment (K-12)\" DESC LIMIT 10",
            "explanation": "Get top 10 schools by enrollment"
        }
        await tree_manager.add_node(child1, parent_id)
        
        # Create parent mapping that references child results
        parent_node_id = await self.create_test_node_with_schema_linking(
            memory,
            intent="Calculate average SAT scores for top schools",
            tables=["satscores"],
            columns=[("satscores", "AvgScrMath"), ("satscores", "AvgScrRead")]
        )
        
        # Update parent node to have the child
        parent_node = await tree_manager.get_node(parent_node_id)
        parent_node.childIds = ["child1"]
        await tree_manager.update_node(parent_node_id, parent_node.to_dict())
        
        # Generate SQL for parent
        agent = SQLGeneratorAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        # Set current node and run
        await tree_manager.set_current_node_id(parent_node_id)
        result = await agent.run("Generate SQL for current node")
        
        # Check result
        assert result is not None
        
        parent_node = await tree_manager.get_node(parent_node_id)
        assert parent_node.generation is not None
        assert "sql" in parent_node.generation
        assert parent_node.generation["sql"] is not None
        
        print(f"\nParent SQL with child reference:")
        print(parent_node.generation["sql"])
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_real_bird_queries(self):
        """Test with real BIRD dataset queries"""
        test_cases = [
            {
                "query": "What is the total number of schools in Los Angeles?",
                "intent": "Count total schools in Los Angeles",
                "tables": ["schools"],
                "columns": [("schools", "City")]
            },
            {
                "query": "Find the school with the highest SAT math score",
                "intent": "Find school with maximum SAT math score",
                "tables": ["schools", "satscores"],
                "columns": [("schools", "School"), ("satscores", "AvgScrMath")],
                "joins": [{
                    "from_table": "schools",
                    "to_table": "satscores",
                    "on": "schools.CDSCode = satscores.cds"
                }]
            }
        ]
        
        for i, test_case in enumerate(test_cases):
            print(f"\n--- Testing BIRD Query {i+1} ---")
            print(f"Query: {test_case['query']}")
            
            memory = await self.setup_test_environment(test_case['query'], f"bird_test_{i}")
            
            # Create node with mapping
            node_id = await self.create_test_node_with_schema_linking(
                memory,
                intent=test_case['intent'],
                tables=test_case['tables'],
                columns=test_case['columns'],
                joins=test_case.get('joins')
            )
            
            agent = SQLGeneratorAgent(memory, llm_config={
                "model_name": "gpt-4o",
                "temperature": 0.1,
                "timeout": 60
            })
            
            # Run the agent - SQLGenerator uses current_node_id from tree manager
            result = await agent.run("Generate SQL for current node")
            
            assert result is not None
            assert len(result.messages) > 0
            
            # Check generated SQL
            tree_manager = QueryTreeManager(memory)
            node = await tree_manager.get_node(node_id)
            assert node.generation is not None
            assert "sql" in node.generation
            assert node.generation["sql"] is not None
            
            print(f"Generated SQL: {node.generation['sql']}")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))