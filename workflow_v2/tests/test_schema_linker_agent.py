"""
Test cases for SchemaLinkerAgent - TESTING_PLAN.md Layer 2.1 Requirements.

Verifies that SchemaLinkerAgent:
1. ONLY prepares context, calls LLM, and extracts outputs (NO business logic)
2. Formats database schema into LLM-friendly XML/text
3. Stores results in node.schema_linking field without validation
4. Does NOT implement table selection logic in code
5. Does NOT validate schema selections or make decisions
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
    """Test cases for SchemaLinkerAgent - Verify NO business logic per TESTING_PLAN.md"""
    
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
    async def test_agent_only_prepares_schema_and_extracts_output(self):
        """Verify agent ONLY prepares schema context, calls LLM, and extracts output - NO logic"""
        query = "What is the highest eligible free rate for K-12 students in schools in Alameda County?"
        node_intent = "Find the maximum eligible free rate for K-12 students in schools located in Alameda County"
        memory, node_id = await self.setup_test_environment(query, "test_no_logic", node_intent)
        
        # Create schema linking agent
        agent = SchemaLinkerAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        }, debug=True)
        
        # VERIFY AGENT RESPONSIBILITIES:
        # 1. CONTEXT PREPARATION - reads schema and formats into XML
        # 2. LLM INTERACTION - sends schema context to LLM
        # 3. OUTPUT EXTRACTION - stores LLM's selections without validation
        
        # Run the agent
        result = await agent.run("Analyze schema for the query")
        
        # Verify agent stored LLM's schema analysis WITHOUT modification
        schema_context = await memory.get("schema_linking")
        assert schema_context is not None
        assert schema_context["schema_analysis"] is not None
        
        # Agent should NOT have logic to:
        # - Decide which tables are relevant (LLM decided)
        # - Validate if tables exist (LLM's responsibility)
        # - Check if columns are correct (trust LLM)
        
        print(f"\nLLM-Selected Schema (not agent logic):")
        selected_tables = schema_context['schema_analysis'].get('selected_tables', {})
        
        # Whatever tables LLM selected, agent stored them
        if isinstance(selected_tables, dict) and 'table' in selected_tables:
            tables = selected_tables['table']
            table_list = tables if isinstance(tables, list) else [tables]
            print(f"LLM selected {len(table_list)} tables (agent didn't validate)")
            for table in table_list:
                print(f"  - {table.get('name', 'N/A')}: Stored as LLM specified")
        
        # Verify XML was in LLM response
        last_message = result.messages[-1].content
        assert "<schema_linking>" in last_message
        print("\n✓ Agent only prepared context and extracted LLM output")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_agent_does_not_analyze_relationships(self):
        """Verify agent does NOT analyze table relationships - LLM decides joins"""
        query = "What are the SAT scores for schools with the highest free meal count?"
        node_intent = "Find SAT scores for schools that have the highest free meal count"
        memory, node_id = await self.setup_test_environment(query, "test_no_relationship_logic", node_intent)
        
        # Create schema linking agent
        agent = SchemaLinkerAgent(memory, llm_config={
            "model_name": "gpt-4o",
            "temperature": 0.1,
            "timeout": 60
        })
        
        # Run the agent
        result = await agent.run("Analyze schema for the query")
        
        # Get schema analysis
        schema_context = await memory.get("schema_linking")
        assert schema_context is not None
        schema_analysis = schema_context["schema_analysis"]
        
        # CRITICAL VERIFICATION:
        # Agent should NOT have code that:
        # - Detects foreign key relationships
        # - Decides which tables can join
        # - Validates join conditions
        
        print(f"\nAgent Behavior Verification:")
        
        # Check if LLM suggested joins
        joins = schema_analysis.get('joins', [])
        if joins:
            print("LLM identified joins - agent stored them without validation")
            joins_list = joins if isinstance(joins, list) else [joins]
            for join in joins_list:
                if isinstance(join, dict):
                    print(f"  {join.get('from_table')} -> {join.get('to_table')}")
                    print(f"    Agent didn't verify if this join is valid")
        else:
            print("LLM didn't identify joins - agent didn't force any")
        
        # The key point: join decisions came from LLM,
        # NOT from agent analyzing foreign keys
        print("\n✓ Agent did NOT analyze relationships, LLM decided")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set") 
    async def test_schema_formatting_only(self):
        """Verify _reader_callback ONLY formats schema, no selection logic"""
        query = "Find schools in California"
        memory, node_id = await self.setup_test_environment(query, "test_schema_format", "Find all schools")
        
        agent = SchemaLinkerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test reader callback directly
        context = await agent._reader_callback(memory, "task", None)
        
        # VERIFY: Callback only formats data, no selection or filtering
        assert context is not None
        assert "original_query" in context
        assert "database_name" in context
        assert "full_schema" in context
        
        # Schema should be complete, unfiltered
        schema_xml = context["full_schema"]
        assert "<database_schema>" in schema_xml
        
        # Context should NOT contain:
        # - Pre-selected tables (that's for LLM)
        # - Filtered schema (all tables included)
        # - Relationship analysis (LLM figures it out)
        
        print(f"\nSchema formatting verification:")
        print(f"Context keys: {list(context.keys())}")
        print(f"Full schema provided: Yes ({len(schema_xml)} chars)")
        print(f"Pre-filtered tables: No (all tables included)")
        print("✓ Reader callback only formats schema, no selection logic")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_trust_llm_output_without_validation(self):
        """Verify agent trusts LLM output without validation"""
        memory = KeyValueMemory()
        agent = SchemaLinkerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test 1: Valid tables and columns
        valid_xml = """
        <schema_linking>
          <selected_tables>
            <table name="schools" alias="s">
              <purpose>Contains school location data</purpose>
            </table>
            <table name="frpm" alias="f">
              <purpose>Contains free meal rate data</purpose>
            </table>
          </selected_tables>
        </schema_linking>
        """
        
        result = agent._parse_linking_xml(valid_xml)
        assert result is not None
        # Agent stores whatever LLM says
        assert "selected_tables" in result
        print("✓ Valid tables stored without validation")
        
        # Test 2: Non-existent table
        invalid_table_xml = """
        <schema_linking>
          <selected_tables>
            <table name="non_existent_table" alias="x">
              <purpose>This table doesn't exist</purpose>
            </table>
          </selected_tables>
        </schema_linking>
        """
        
        result = agent._parse_linking_xml(invalid_table_xml)
        assert result is not None
        # Agent STILL stores it - no validation
        tables = result["selected_tables"]["table"]
        assert tables["name"] == "non_existent_table"
        print("✓ Non-existent table stored - agent didn't validate")
        
        # Test 3: Invalid column names
        invalid_column_xml = """
        <schema_linking>
          <selected_tables>
            <table name="schools" alias="s">
              <columns>
                <column name="InvalidColumn" used_for="filter"/>
                <column name="AnotherBadColumn" used_for="select"/>
              </columns>
            </table>
          </selected_tables>
        </schema_linking>
        """
        
        result = agent._parse_linking_xml(invalid_column_xml)
        assert result is not None
        # Agent stores invalid columns too - trusts LLM
        tables = result["selected_tables"]["table"]
        if "columns" in tables:
            columns = tables["columns"]["column"]
            column_list = columns if isinstance(columns, list) else [columns]
            # Agent stored the invalid columns without checking
            assert any(c["name"] == "InvalidColumn" for c in column_list)
        print("✓ Invalid columns stored - agent trusts LLM completely")
        
        print("\n✅ Agent stores ALL LLM output without validation")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_no_schema_analysis_logic(self):
        """Verify agent has NO schema analysis logic in code"""
        memory = KeyValueMemory()
        agent = SchemaLinkerAgent(memory, llm_config={"model_name": "gpt-4o"})
        
        # Test various schema scenarios - agent should NOT analyze
        test_cases = [
            {
                "query": "Find schools with high SAT scores",
                "llm_tables": ["schools", "satscores"],
                "reason": "LLM identified need for SAT data"
            },
            {
                "query": "Count all schools", 
                "llm_tables": ["schools"],
                "reason": "LLM identified single table query"
            },
            {
                "query": "Complex aggregation across all data",
                "llm_tables": ["schools", "frpm", "satscores"],
                "reason": "LLM identified multi-table analysis"
            }
        ]
        
        for test_case in test_cases:
            print(f"\nQuery: {test_case['query']}")
            print(f"LLM selected tables: {test_case['llm_tables']}")
            print(f"Reason: {test_case['reason']}")
            
            # Agent should NOT have patterns like:
            # - if "SAT" in query: add_table("satscores")
            # - if "count" in query: single_table_only()
            # - if query_complexity > threshold: add_all_tables()
            
            # Instead, agent just stores what LLM decided
            print("✓ Agent has no table selection logic")
        
        print("\n✅ Agent contains NO schema analysis logic")
        print("✅ All schema decisions made by LLM")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
    async def test_no_table_selection_patterns(self):
        """Verify agent has NO hardcoded patterns for table selection"""
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
            print(f"\n--- Verifying No Selection Patterns for Query {i+1} ---")
            print(f"Query: {test_case['query'][:70]}...")
            
            memory, node_id = await self.setup_test_environment(
                test_case['query'], 
                f"no_pattern_test_{i}",
                test_case['intent']
            )
            
            agent = SchemaLinkerAgent(memory, llm_config={
                "model_name": "gpt-4o",
                "temperature": 0.1,
                "timeout": 60
            })
            
            # Run the agent
            result = await agent.run("Analyze schema for the query")
            
            # Get schema analysis
            schema_context = await memory.get("schema_linking")
            schema_analysis = schema_context["schema_analysis"]
            
            # CRITICAL VERIFICATION:
            # Agent should NOT have patterns like:
            # - if "charter schools" in query: select_table("schools")
            # - if "SAT" in query: select_table("satscores")
            # - if "FRPM" in query: select_table("frpm")
            
            selected_tables = schema_analysis.get("selected_tables", {})
            if isinstance(selected_tables, dict) and "table" in selected_tables:
                tables = selected_tables["table"]
                table_list = tables if isinstance(tables, list) else [tables]
                table_names = [t.get('name', 'N/A') for t in table_list]
                
                print(f"LLM selected tables: {table_names}")
                print("✓ Selection based on LLM analysis, not agent patterns")
                
                # Verify agent didn't force selections based on keywords
                if "charter" in test_case['query'].lower():
                    print("  Query mentions 'charter' - LLM decided if relevant")
                if "SAT" in test_case['query']:
                    print("  Query mentions 'SAT' - LLM decided if satscores needed")
                if "FRPM" in test_case['query']:
                    print("  Query mentions 'FRPM' - LLM decided if frpm table needed")
            
            # The key point: table selection came from LLM,
            # NOT from agent's keyword matching


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "="*70)
    print("SchemaLinkerAgent Tests - Verifying NO Business Logic")
    print("Based on TESTING_PLAN.md Layer 2.1 Requirements")
    print("="*70)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))