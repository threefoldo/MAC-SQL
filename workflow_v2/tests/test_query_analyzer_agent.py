"""
Test cases for QueryAnalyzerAgent using real LLM and BIRD dataset.

Tests the actual run method and internal implementation.
"""

import asyncio
import pytest
import os
from pathlib import Path
import sys
import logging

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from keyvalue_memory import KeyValueMemory
from memory_content_types import (
    TaskContext, QueryNode, NodeStatus, TaskStatus,
    CombineStrategy, CombineStrategyType, QueryMapping,
    TableMapping, ColumnMapping, JoinMapping,
    TableSchema, ColumnInfo
)
from task_context_manager import TaskContextManager
from query_tree_manager import QueryTreeManager
from database_schema_manager import DatabaseSchemaManager
from node_history_manager import NodeHistoryManager
from query_analyzer_agent import QueryAnalyzerAgent
from schema_reader import SchemaReader


class TestQueryAnalyzerAgent:
    """Test cases for QueryAnalyzerAgent"""
    
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
                "NumTstTakr": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "AvgScrRead": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "AvgScrMath": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(satscores_schema)
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_simple_query(self):
        """Test running the agent with a simple query"""
        query = "What is the total number of schools in Alameda County?"
        memory = await self.setup_test_environment(query, "test_simple")
        
        # Create analyzer
        analyzer = QueryAnalyzerAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        }, debug=True)
        
        # Run the agent with the query
        result = await analyzer.run(query)
        
        # Verify the agent ran and returned a result
        assert result is not None
        assert hasattr(result, 'messages')
        assert len(result.messages) > 0
        
        # Check that analysis was stored in memory
        analysis = await memory.get("query_analysis")
        assert analysis is not None
        assert "intent" in analysis
        assert "complexity" in analysis
        assert "tables" in analysis
        
        print(f"\nSimple Query Analysis:")
        print(f"Intent: {analysis['intent']}")
        print(f"Complexity: {analysis['complexity']}")
        print(f"Tables: {[t['name'] for t in analysis['tables']]}")
        
        # Check that tree was created
        tree_manager = QueryTreeManager(memory)
        tree_stats = await tree_manager.get_tree_stats()
        assert tree_stats["total_nodes"] >= 1
        
        # Verify the LLM response structure
        last_message = result.messages[-1].content
        assert "<analysis>" in last_message
        assert "</analysis>" in last_message
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_run_complex_query(self):
        """Test running the agent with a complex query that should be decomposed"""
        query = "What is the average SAT score of students in schools with the highest free meal count in each county?"
        memory = await self.setup_test_environment(query, "test_complex")
        
        # Create analyzer
        analyzer = QueryAnalyzerAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        # Run the agent
        result = await analyzer.run(query)
        
        # Verify result
        assert result is not None
        assert len(result.messages) > 0
        
        # Check stored analysis
        analysis = await memory.get("query_analysis")
        assert analysis is not None
        
        print(f"\nComplex Query Analysis:")
        print(f"Intent: {analysis['intent']}")
        print(f"Complexity: {analysis['complexity']}")
        print(f"Tables: {[t['name'] for t in analysis['tables']]}")
        
        # Complex query might have decomposition
        if analysis["complexity"] == "complex" and "decomposition" in analysis:
            decomposition = analysis["decomposition"]
            print(f"Subqueries: {len(decomposition['subqueries'])}")
            for i, sq in enumerate(decomposition["subqueries"]):
                print(f"  Subquery {i+1}: {sq['intent']}")
            
            if "combination" in decomposition:
                print(f"Combination Strategy: {decomposition['combination']['strategy']}")
            
            # Verify tree structure for complex queries
            tree_manager = QueryTreeManager(memory)
            tree_stats = await tree_manager.get_tree_stats()
            print(f"Tree nodes created: {tree_stats['total_nodes']}")
            assert tree_stats["total_nodes"] > 1
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set") 
    async def test_reader_callback(self):
        """Test the _reader_callback method"""
        query = "Find schools in California"
        memory = await self.setup_test_environment(query, "test_reader")
        
        analyzer = QueryAnalyzerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test reader callback directly
        context = await analyzer._reader_callback(memory, query, None)
        
        assert context is not None
        assert "query" in context
        assert context["query"] == query
        assert "schema" in context
        assert "<database_schema>" in context["schema"]
        
        print(f"\nReader callback context keys: {list(context.keys())}")
        print(f"Schema length: {len(context['schema'])}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_get_schema_xml(self):
        """Test the schema XML generation method"""
        memory = await self.setup_test_environment("test query", "test_schema_xml")
        
        analyzer = QueryAnalyzerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test schema XML generation
        schema_xml = await analyzer._get_schema_xml()
        
        print(f"\nGenerated Schema XML:")
        print(schema_xml[:500] + "..." if len(schema_xml) > 500 else schema_xml)
        
        # Verify XML structure
        assert "<database_schema>" in schema_xml
        assert "</database_schema>" in schema_xml
        assert "<table" in schema_xml
        assert "<column" in schema_xml
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_parse_analysis_xml(self):
        """Test XML parsing of analysis results"""
        memory = KeyValueMemory()
        analyzer = QueryAnalyzerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test simple analysis XML
        simple_xml = """
        <analysis>
          <intent>Find all schools in Alameda County</intent>
          <complexity>simple</complexity>
          <tables>
            <table name="schools" purpose="Contains school location data"/>
          </tables>
        </analysis>
        """
        
        result = analyzer._parse_analysis_xml(simple_xml)
        
        assert result is not None
        assert result["intent"] == "Find all schools in Alameda County"
        assert result["complexity"] == "simple"
        assert len(result["tables"]) == 1
        assert result["tables"][0]["name"] == "schools"
        
        print(f"\nParsed Simple Analysis: {result}")
        
        # Test complex analysis XML with decomposition
        complex_xml = """
        <analysis>
          <intent>Find average SAT scores for top schools</intent>
          <complexity>complex</complexity>
          <tables>
            <table name="schools" purpose="School information"/>
            <table name="satscores" purpose="SAT score data"/>
          </tables>
          <decomposition>
            <subquery id="1">
              <intent>Find top performing schools</intent>
              <description>Identify schools with highest rankings</description>
              <tables>schools, rankings</tables>
            </subquery>
            <subquery id="2">
              <intent>Get SAT scores for identified schools</intent>
              <description>Retrieve SAT data for the top schools</description>
              <tables>satscores</tables>
            </subquery>
            <combination>
              <strategy>join</strategy>
              <description>Join the results on school ID</description>
            </combination>
          </decomposition>
        </analysis>
        """
        
        complex_result = analyzer._parse_analysis_xml(complex_xml)
        
        assert complex_result is not None
        assert complex_result["complexity"] == "complex"
        assert "decomposition" in complex_result
        assert len(complex_result["decomposition"]["subqueries"]) == 2
        assert complex_result["decomposition"]["combination"]["strategy"] == "join"
        
        print(f"\nParsed Complex Analysis: {complex_result}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_parse_strategy_type(self):
        """Test strategy type parsing"""
        memory = KeyValueMemory()
        analyzer = QueryAnalyzerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test different strategy types
        assert analyzer._parse_strategy_type("union") == CombineStrategyType.UNION
        assert analyzer._parse_strategy_type("join") == CombineStrategyType.JOIN
        assert analyzer._parse_strategy_type("aggregate") == CombineStrategyType.AGGREGATE
        assert analyzer._parse_strategy_type("filter") == CombineStrategyType.FILTER
        assert analyzer._parse_strategy_type("custom") == CombineStrategyType.CUSTOM
        assert analyzer._parse_strategy_type("unknown") == CombineStrategyType.CUSTOM
        
        print("✓ Strategy type parsing tests passed")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_real_bird_queries(self):
        """Test with real BIRD dataset queries"""
        test_queries = [
            "What is the highest eligible free rate for K-12 students in schools in Alameda County?",
            "List the zip codes of all charter schools in Fresno County Office of Education.",
            "What is the number of SAT test takers in schools with the highest FRPM count for K-12 students?"
        ]
        
        for i, query in enumerate(test_queries):
            print(f"\n--- Testing BIRD Query {i+1} ---")
            print(f"Query: {query}")
            
            memory = await self.setup_test_environment(query, f"bird_test_{i}")
            
            analyzer = QueryAnalyzerAgent(memory, llm_config={
                "model_name": "gpt-4o",
                "temperature": 0.1,
                "timeout": 60
            })
            
            # Run the agent
            result = await analyzer.run(query)
            
            assert result is not None
            assert len(result.messages) > 0
            
            # Check stored analysis
            analysis = await memory.get("query_analysis")
            assert analysis is not None
            
            print(f"Intent: {analysis['intent']}")
            print(f"Complexity: {analysis['complexity']}")
            print(f"Tables: {[t['name'] for t in analysis['tables']]}")
            
            # Check tree creation
            tree_manager = QueryTreeManager(memory)
            tree_stats = await tree_manager.get_tree_stats()
            print(f"Nodes created: {tree_stats['total_nodes']}")
            
            # Verify at least one node was created
            assert tree_stats['total_nodes'] >= 1


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))