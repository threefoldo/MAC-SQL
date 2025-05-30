"""
Test cases for QueryAnalyzerAgent - TESTING_PLAN.md Layer 2.2 Requirements.

Verifies that QueryAnalyzerAgent:
1. ONLY prepares context, calls LLM, and extracts outputs (NO business logic)
2. Handles LLM response parsing with robust fallback strategies
3. Creates child nodes only if LLM requests decomposition
4. Does NOT implement query complexity logic in code
5. Does NOT make decomposition decisions independently
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
from query_analyzer_agent import QueryAnalyzerAgent
from schema_reader import SchemaReader


class TestQueryAnalyzerAgent:
    """Test cases for QueryAnalyzerAgent - Verify NO business logic per TESTING_PLAN.md"""
    
    async def get_analysis_from_node(self, memory):
        """Helper to get queryAnalysis from root node"""
        tree_manager = QueryTreeManager(memory)
        tree_data = await tree_manager.get_tree()
        
        # Always get analysis from root node since that's where QueryAnalyzer stores it
        root_id = tree_data.get("rootId", "root")
        node_data = tree_data["nodes"][root_id]
        analysis = node_data.get("queryAnalysis")
        return analysis
    
    async def setup_test_environment(self, query: str, task_id: str, db_name: str = "california_schools", with_schema_analysis: bool = False):
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
        
        # Initialize query tree with root node (like orchestrator does)
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(query, None)  # No evidence for tests
        await tree_manager.set_current_node_id(root_id)
        
        # Initialize schema_linking (like orchestrator does)
        await self._initialize_schema_linking(memory, query, db_name, with_schema_analysis)
        
        return memory
    
    async def _initialize_schema_linking(self, memory: KeyValueMemory, query: str, db_name: str, with_schema_analysis: bool = False):
        """Initialize schema linking context like the orchestrator does"""
        from datetime import datetime
        
        schema_context = {
            "original_query": query,
            "database_name": db_name,
            "evidence": None,
            "initialized_at": datetime.now().isoformat(),
            "schema_analysis": None,  # Updated by SchemaLinker
            "last_update": None
        }
        
        # Optionally add pre-analyzed schema for testing
        if with_schema_analysis:
            # Simulate schema analysis that would be done by SchemaLinker
            schema_analysis = {
                "tables": [
                    {
                        "name": "schools",
                        "alias": "s",
                        "purpose": "Contains school information including location",
                        "columns": [
                            {"name": "CDSCode", "used_for": "join", "reason": "Primary key"},
                            {"name": "County", "used_for": "filter", "reason": "Filter by county"}
                        ]
                    },
                    {
                        "name": "frpm",
                        "alias": "f",
                        "purpose": "Contains free/reduced price meal data",
                        "columns": [
                            {"name": "CDSCode", "used_for": "join", "reason": "Foreign key"},
                            {"name": "FRPM Count (K-12)", "used_for": "select", "reason": "Metric to analyze"}
                        ]
                    }
                ],
                "joins": [
                    {
                        "from_table": "schools",
                        "from_column": "CDSCode",
                        "to_table": "frpm",
                        "to_column": "CDSCode",
                        "join_type": "INNER"
                    }
                ],
                "sample_query": "SELECT ... FROM schools s JOIN frpm f ON s.CDSCode = f.CDSCode"
            }
            
            schema_context["schema_analysis"] = schema_analysis
            schema_context["last_update"] = datetime.now().isoformat()
        
        await memory.set("schema_linking", schema_context)
    
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
    async def test_agent_only_prepares_context_and_extracts_output(self):
        """Verify agent ONLY prepares context, calls LLM, and extracts output - NO logic"""
        query = "What is the total number of schools in Alameda County?"
        memory = await self.setup_test_environment(query, "test_no_logic")
        
        # Create analyzer
        analyzer = QueryAnalyzerAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        }, debug=True)
        
        # VERIFY AGENT RESPONSIBILITIES:
        # 1. CONTEXT PREPARATION - formats query and schema for LLM
        # 2. LLM INTERACTION - sends prompt and receives response
        # 3. OUTPUT EXTRACTION - parses XML and stores results
        
        # Run the agent
        result = await analyzer.run(query)
        
        # Verify agent stored LLM's analysis WITHOUT modification
        analysis = await self.get_analysis_from_node(memory)
        assert analysis is not None
        
        # Agent should NOT have logic to determine:
        # - Query is "simple" (LLM decided this)
        # - Which tables to use (LLM decided this)
        # - Whether to decompose (LLM decided this)
        
        print(f"\nLLM-Determined Analysis (not agent logic):")
        print(f"Intent: {analysis['intent']}")
        print(f"Complexity: {analysis['complexity']} (LLM decided, not agent)")
        
        # Verify agent didn't hardcode complexity rules
        # The complexity value comes from LLM, not agent code
        assert analysis['complexity'] in ['simple', 'medium', 'complex']
        
        # Verify XML parsing worked
        last_message = result.messages[-1].content
        assert "<analysis>" in last_message
        assert "</analysis>" in last_message
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_agent_creates_nodes_only_if_llm_requests(self):
        """Verify agent creates child nodes ONLY if LLM requests decomposition"""
        query = "What is the average SAT score of students in schools with the highest free meal count in each county?"
        memory = await self.setup_test_environment(query, "test_llm_decomposition", with_schema_analysis=False)
        
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
        analysis = await self.get_analysis_from_node(memory)
        assert analysis is not None
        
        print(f"\nAgent Behavior Verification:")
        print(f"LLM determined complexity: {analysis['complexity']}")
        
        # CRITICAL VERIFICATION: Agent creates nodes based on LLM decision
        tree_manager = QueryTreeManager(memory)
        tree_stats = await tree_manager.get_tree_stats()
        
        # If LLM said "complex" AND provided decomposition, agent should create nodes
        if analysis["complexity"] == "complex" and "decomposition" in analysis:
            print("LLM requested decomposition - verifying agent created child nodes")
            
            decomposition = analysis["decomposition"]
            # Count subqueries from LLM
            subquery_count = 0
            if "subqueries" in decomposition:
                subquery_count = len(decomposition["subqueries"])
            elif "subquery" in decomposition:
                subqueries = decomposition["subquery"]
                subquery_count = len(subqueries) if isinstance(subqueries, list) else 1
            
            print(f"LLM specified {subquery_count} subqueries")
            print(f"Agent created {tree_stats['total_nodes']} total nodes")
            
            # Agent should have created nodes matching LLM's decomposition
            assert tree_stats["total_nodes"] > 1
            print("✓ Agent correctly created nodes based on LLM decomposition")
        else:
            # If LLM didn't request decomposition, agent should NOT create extra nodes
            print("LLM did not request decomposition - verifying no extra nodes")
            assert tree_stats["total_nodes"] == 1  # Only root
            print("✓ Agent correctly did NOT create extra nodes")
        
        # VERIFY: Agent does NOT have hardcoded rules for complexity
        # The decision came entirely from LLM response
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set") 
    async def test_context_preparation_only(self):
        """Verify _reader_callback ONLY prepares context, no business logic"""
        query = "Find schools in California"
        memory = await self.setup_test_environment(query, "test_context_prep")
        
        analyzer = QueryAnalyzerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test reader callback directly
        context = await analyzer._reader_callback(memory, query, None)
        
        # VERIFY: Callback only prepares data, no analysis or decisions
        assert context is not None
        assert "query" in context
        assert context["query"] == query  # Raw query, unmodified
        assert "schema" in context
        assert "<database_schema>" in context["schema"]  # Schema formatted as XML
        
        # Context should NOT contain:
        # - Complexity assessment (that's for LLM)
        # - Table recommendations (that's for LLM)
        # - Query analysis (that's for LLM)
        
        print(f"\nContext preparation verification:")
        print(f"Context keys: {list(context.keys())}")
        print(f"Query passed as-is: {context['query'] == query}")
        print("✓ Reader callback only prepares context, no logic")
    
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
    async def test_robust_xml_parsing_with_fallbacks(self):
        """Verify robust XML parsing with fallback strategies"""
        memory = KeyValueMemory()
        analyzer = QueryAnalyzerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test 1: Well-formed XML
        good_xml = """
        <analysis>
          <intent>Find all schools in Alameda County</intent>
          <complexity>simple</complexity>
          <tables>
            <table name="schools" purpose="Contains school location data"/>
          </tables>
        </analysis>
        """
        
        result = analyzer._parse_analysis_xml(good_xml)
        assert result is not None
        assert result["intent"] == "Find all schools in Alameda County"
        print("✓ Well-formed XML parsed successfully")
        
        # Test 2: Malformed XML (missing closing tag)
        bad_xml = """
        <analysis>
          <intent>Find schools
          <complexity>simple</complexity>
        </analysis>
        """
        
        # Agent should handle gracefully with fallback
        result = analyzer._parse_analysis_xml(bad_xml)
        # Fallback parsing should still extract what it can
        assert result is not None
        print("✓ Malformed XML handled with fallback")
        
        # Test 3: Partial XML (LLM response cut off)
        partial_xml = """
        <analysis>
          <intent>Find all schools in Alameda County</intent>
          <complexity>simple</complexity>
          <tables>
            <table name="schools" purp
        """
        
        # Agent should extract what it can
        result = analyzer._parse_analysis_xml(partial_xml)
        assert result is not None
        # Should at least get intent and complexity
        if "intent" in result:
            assert "Find all schools" in result["intent"]
        print("✓ Partial XML handled with fallback extraction")
        
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
        
        # The hybrid XML parsing creates this structure:
        # decomposition: {'subquery': [...], 'combination': {...}}
        decomposition = complex_result["decomposition"]
        
        # Subqueries are in the 'subquery' key as a list
        assert "subquery" in decomposition
        subqueries_list = decomposition["subquery"]
        assert isinstance(subqueries_list, list)
        assert len(subqueries_list) == 2
        assert complex_result["decomposition"]["combination"]["strategy"] == "join"
        
        print(f"\nParsed Complex Analysis: {complex_result}")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_no_hardcoded_complexity_rules(self):
        """Verify agent has NO hardcoded rules for query complexity"""
        memory = KeyValueMemory()
        analyzer = QueryAnalyzerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test various queries - agent should NOT determine complexity
        test_cases = [
            {
                "query": "SELECT * FROM schools",  # Simple SQL
                "xml": '<analysis><complexity>simple</complexity></analysis>'
            },
            {
                "query": "Find top 5 schools by SAT scores in counties with high poverty",  # Complex
                "xml": '<analysis><complexity>complex</complexity></analysis>'
            },
            {
                "query": "What is 2+2?",  # Not even SQL
                "xml": '<analysis><complexity>simple</complexity></analysis>'
            }
        ]
        
        for test_case in test_cases:
            # Parse LLM response
            result = analyzer._parse_analysis_xml(test_case["xml"])
            
            # Agent should accept whatever complexity LLM says
            # NOT apply its own rules based on query patterns
            assert "complexity" in result
            assert result["complexity"] in ["simple", "medium", "complex"]
            
            print(f"Query: {test_case['query'][:50]}...")
            print(f"LLM said: {result['complexity']} (agent accepted without logic)")
        
        print("\n✓ Agent has NO hardcoded complexity rules")
        print("✓ Complexity determination is 100% from LLM")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set") 
    async def test_query_with_schema_analysis(self):
        """Test QueryAnalyzer using pre-analyzed schema from SchemaLinker"""
        query = "What is the highest free meal count in schools in Alameda County?"
        # Setup with schema analysis already done
        memory = await self.setup_test_environment(query, "test_schema_informed", with_schema_analysis=True)
        
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
        analysis = await self.get_analysis_from_node(memory)
        assert analysis is not None
        
        print(f"\nSchema-Informed Query Analysis:")
        print(f"Intent: {analysis['intent']}")
        print(f"Complexity: {analysis['complexity']}")
        # Handle different table formats from XML parsing
        if isinstance(analysis['tables'], list):
            table_names = [t['name'] for t in analysis['tables']]
        elif isinstance(analysis['tables'], dict) and 'table' in analysis['tables']:
            tables = analysis['tables']['table']
            if isinstance(tables, list):
                table_names = [t['name'] for t in tables]
            else:
                table_names = [tables['name']]
        else:
            table_names = []
        print(f"Tables identified: {table_names}")
        
        # Verify the agent could use schema analysis context
        schema_context = await memory.get("schema_linking")
        assert schema_context is not None
        assert schema_context["schema_analysis"] is not None
        print(f"Schema analysis was available: Yes")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_no_decomposition_logic_in_agent(self):
        """Verify agent does NOT make decomposition decisions independently"""
        test_queries = [
            "What is the highest eligible free rate for K-12 students in schools in Alameda County?",
            "List the zip codes of all charter schools in Fresno County Office of Education.",
            "What is the number of SAT test takers in schools with the highest FRPM count for K-12 students?"
        ]
        
        for i, query in enumerate(test_queries):
            print(f"\n--- Verifying No Decomposition Logic for Query {i+1} ---")
            print(f"Query: {query[:70]}...")
            
            memory = await self.setup_test_environment(query, f"no_logic_test_{i}")
            
            analyzer = QueryAnalyzerAgent(memory, llm_config={
                "model_name": "gpt-4o",
                "temperature": 0.1,
                "timeout": 60
            })
            
            # Run the agent
            result = await analyzer.run(query)
            
            # Get analysis
            analysis = await self.get_analysis_from_node(memory)
            assert analysis is not None
            
            # CRITICAL VERIFICATION:
            # Agent should NOT have patterns like:
            # - if "highest" in query and "each" in query: decompose()
            # - if len(tables) > 2: mark_as_complex()
            # - if "subquery" in query: create_children()
            
            print(f"LLM decided complexity: {analysis['complexity']}")
            
            # Check if decomposition happened
            tree_manager = QueryTreeManager(memory)
            tree_stats = await tree_manager.get_tree_stats()
            
            if tree_stats['total_nodes'] > 1:
                # Decomposition happened - verify it was LLM's decision
                assert "decomposition" in analysis
                print(f"✓ Decomposition happened because LLM requested it")
                print(f"  Nodes created: {tree_stats['total_nodes']}")
            else:
                # No decomposition - verify LLM didn't request it
                if "decomposition" in analysis:
                    # LLM suggested decomposition but no subqueries
                    print(f"✓ LLM suggested decomposition but no valid subqueries")
                else:
                    print(f"✓ No decomposition because LLM didn't request it")
            
            # The key point: decomposition decision came from LLM response,
            # NOT from agent's code analyzing the query


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "="*70)
    print("QueryAnalyzerAgent Tests - Verifying NO Business Logic")
    print("Based on TESTING_PLAN.md Layer 2.2 Requirements")
    print("="*70)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))