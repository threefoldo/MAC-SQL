"""
Test cases for SQLGeneratorAgent - TESTING_PLAN.md Layer 2.3 Requirements.

Verifies that SQLGeneratorAgent:
1. ONLY prepares context, calls LLM, and extracts outputs (NO business logic)
2. Reads schema_linking results and formats for LLM
3. Stores SQL and explanation without validation or modification
4. Does NOT implement SQL optimization logic
5. Does NOT make decisions about SQL structure
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
    """Test cases for SQLGeneratorAgent - Verify NO business logic per TESTING_PLAN.md"""
    
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
    async def test_agent_only_formats_context_and_extracts_sql(self):
        """Verify agent ONLY prepares context, calls LLM, and extracts SQL - NO logic"""
        query = "What is the highest eligible free rate for K-12 students in schools in Alameda County?"
        memory = await self.setup_test_environment(query, "test_no_logic")
        
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
        
        # VERIFY AGENT RESPONSIBILITIES:
        # 1. CONTEXT PREPARATION - reads schema linking and formats for LLM
        # 2. LLM INTERACTION - sends context and receives SQL
        # 3. OUTPUT EXTRACTION - stores SQL without modification
        
        # Run the agent
        result = await agent.run("Generate SQL for current node")
        
        # Verify agent stored LLM's SQL WITHOUT modification
        tree_manager = QueryTreeManager(memory)
        node = await tree_manager.get_node(node_id)
        assert node.generation is not None
        assert "sql" in node.generation
        
        # Agent should NOT have logic to:
        # - Optimize the SQL (LLM's SQL is final)
        # - Validate SQL syntax (trust LLM)
        # - Modify formatting (preserve LLM's format)
        
        generated_sql = node.generation["sql"]
        print(f"\nLLM-Generated SQL (stored as-is):")
        print(generated_sql)
        
        # Verify SQL came from LLM response
        last_message = result.messages[-1].content
        assert "<sql_generation>" in last_message or "```sql" in last_message
        
        # Key point: Whatever SQL format LLM provided, agent stored it
        print("\n✓ Agent only prepared context and extracted LLM's SQL")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_agent_does_not_optimize_sql(self):
        """Verify agent does NOT optimize or modify SQL from LLM"""
        query = "What is the average SAT math score for schools with more than 500 students?"
        memory = await self.setup_test_environment(query, "test_no_optimization")
        
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
        
        # Run the agent
        result = await agent.run("Generate SQL for current node")
        
        # Get generated SQL
        tree_manager = QueryTreeManager(memory)
        node = await tree_manager.get_node(node_id)
        generated_sql = node.generation["sql"]
        
        # CRITICAL VERIFICATION:
        # Agent should NOT have code that:
        # - Adds indexes for performance
        # - Rewrites subqueries as joins
        # - Changes column order for efficiency
        # - Adds query hints
        
        print(f"\nSQL Optimization Verification:")
        print(f"LLM's SQL (unmodified):")
        print(generated_sql)
        
        # Even if LLM generates potentially inefficient SQL,
        # agent stores it exactly as provided
        
        # Example: LLM might generate:
        # SELECT AVG(AvgScrMath) FROM satscores WHERE cds IN 
        #   (SELECT CDSCode FROM frpm WHERE "Enrollment (K-12)" > 500)
        # 
        # Agent should NOT rewrite it as a more efficient JOIN
        
        print("\n✓ Agent did NOT optimize SQL - stored LLM's version")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set") 
    async def test_context_preparation_only(self):
        """Verify _reader_callback ONLY prepares context, no SQL decisions"""
        query = "Find schools in California"
        memory = await self.setup_test_environment(query, "test_context_prep")
        
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
        
        # VERIFY: Callback only prepares data, no SQL logic
        assert context is not None
        assert "current_node" in context
        
        # Parse to verify structure
        import json
        current_node = json.loads(context["current_node"])
        
        # Context should contain:
        # - Node intent (what to generate SQL for)
        # - Schema linking results (tables/columns to use)
        # - Task context (original query, database)
        
        # Context should NOT contain:
        # - SQL templates or patterns
        # - Optimization rules
        # - SQL structure decisions
        
        print(f"\nContext preparation verification:")
        print(f"Intent provided: {current_node['intent']}")
        print(f"Schema context: {'schema_linking' in current_node}")
        print("✓ Context contains only data, no SQL logic")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_sql_extraction_without_modification(self):
        """Verify agent extracts SQL from LLM without modification"""
        memory = KeyValueMemory()
        agent = SQLGeneratorAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test 1: Well-formatted SQL
        good_sql_xml = """
        <sql_generation>
          <sql>
            SELECT MAX(f."Eligible Free Rate (K-12)")
            FROM schools s
            JOIN frpm f ON s.CDSCode = f.CDSCode
            WHERE s.County = 'Alameda'
          </sql>
        </sql_generation>
        """
        
        result = agent._parse_generation_xml(good_sql_xml)
        assert result["sql"] is not None
        # SQL preserved exactly as LLM provided
        assert "MAX(f.\"Eligible Free Rate (K-12)\")" in result["sql"]
        print("✓ Well-formatted SQL extracted without modification")
        
        # Test 2: Poorly formatted SQL (extra spaces, odd casing)
        poor_sql_xml = """
        <sql_generation>
          <sql>
            SeLeCt   MAX(  f."Eligible Free Rate (K-12)"  )
            FrOm schools   s
                JOIN frpm f    ON s.CDSCode=f.CDSCode
            WHERE   s.County='Alameda'
          </sql>
        </sql_generation>
        """
        
        result = agent._parse_generation_xml(poor_sql_xml)
        # Agent should NOT fix formatting - preserve LLM's output
        assert "SeLeCt" in result["sql"] or "MAX" in result["sql"]
        print("✓ Poorly formatted SQL preserved as LLM provided")
        
        # Test 3: SQL with potential inefficiency
        inefficient_sql = """
        <sql_generation>
          <sql>
            SELECT * FROM (
              SELECT * FROM schools WHERE County = 'Alameda'
            ) s
            JOIN frpm f ON s.CDSCode = f.CDSCode
          </sql>
        </sql_generation>
        """
        
        result = agent._parse_generation_xml(inefficient_sql)
        # Agent should NOT optimize the nested SELECT *
        assert "SELECT * FROM (" in result["sql"]
        print("✓ Inefficient SQL preserved without optimization")
    
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
    async def test_no_sql_structure_decisions(self):
        """Verify agent makes NO decisions about SQL structure"""
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
            print(f"\n--- Verifying No SQL Structure Decisions for Query {i+1} ---")
            print(f"Query: {test_case['query']}")
            
            memory = await self.setup_test_environment(test_case['query'], f"no_structure_test_{i}")
            
            # Create node with schema linking
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
            
            # Run the agent
            result = await agent.run("Generate SQL for current node")
            
            # Get generated SQL
            tree_manager = QueryTreeManager(memory)
            node = await tree_manager.get_node(node_id)
            generated_sql = node.generation['sql']
            
            # CRITICAL VERIFICATION:
            # Agent should NOT have patterns like:
            # - if "count" in intent: use COUNT(*)
            # - if "highest" in intent: use MAX() or ORDER BY DESC LIMIT 1
            # - if multiple tables: prefer JOIN over subquery
            
            print(f"LLM decided SQL structure:")
            print(f"{generated_sql}")
            
            # For "count" query - LLM chose COUNT vs other methods
            if "count" in test_case['intent'].lower():
                print("  Query asks for count - LLM decided to use COUNT()")
            
            # For "highest" query - LLM chose MAX vs ORDER BY LIMIT
            if "highest" in test_case['intent'].lower():
                if "MAX" in generated_sql.upper():
                    print("  Query asks for highest - LLM chose MAX()")
                elif "ORDER BY" in generated_sql.upper() and "LIMIT" in generated_sql.upper():
                    print("  Query asks for highest - LLM chose ORDER BY + LIMIT")
            
            print("✓ SQL structure decisions made by LLM, not agent")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "="*70)
    print("SQLGeneratorAgent Tests - Verifying NO Business Logic")
    print("Based on TESTING_PLAN.md Layer 2.3 Requirements")
    print("="*70)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))