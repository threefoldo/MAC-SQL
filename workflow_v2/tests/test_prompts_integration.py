"""
Test the integration of prompts from core/const.py into workflow_v2 agents.
"""

import pytest
import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from keyvalue_memory import KeyValueMemory
from sql_generator_agent import SQLGeneratorAgent
from query_analyzer_agent import QueryAnalyzerAgent
from schema_linker_agent import SchemaLinkerAgent
from prompts import (
    SQL_CONSTRAINTS, MAX_ROUND, format_decompose_template,
    format_refiner_template, format_zeroshot_template
)


class TestPromptsIntegration:
    """Test that prompts are properly integrated into agents"""
    
    def test_sql_constraints_in_prompts(self):
        """Test that SQL constraints are included in agent prompts"""
        # Test SQL Generator Agent
        memory = KeyValueMemory()
        sql_agent = SQLGeneratorAgent(memory, None)
        system_msg = sql_agent._build_system_message()
        
        # Check that constraints are included
        assert SQL_CONSTRAINTS in system_msg
        assert "SELECT only needed columns" in system_msg
        assert "JOIN <table> FIRST, THEN use" in system_msg
        assert "GROUP BY before ORDER BY" in system_msg
        
        # Test Query Analyzer Agent
        query_agent = QueryAnalyzerAgent(memory, None)
        system_msg = query_agent._build_system_message()
        assert SQL_CONSTRAINTS in system_msg
        
        # Test Schema Linker Agent
        schema_agent = SchemaLinkerAgent(memory, None)
        system_msg = schema_agent._build_system_message()
        assert SQL_CONSTRAINTS in system_msg
    
    def test_refiner_template_formatting(self):
        """Test the refiner template formatting"""
        result = format_refiner_template(
            query="What is the gender of the youngest client?",
            evidence="Later birthdate refers to younger age",
            desc_str="# Table: client\n[(client_id, INT), (gender, VARCHAR)]",
            fk_str="client.district_id = district.district_id",
            sql="SELECT gender FROM client ORDER BY birth_date LIMIT 1",
            sqlite_error="no such column: birth_date",
            exception_class="SQLException"
        )
        
        # Check key components are included
        assert "【Instruction】" in result
        assert "What is the gender of the youngest client?" in result
        assert "Later birthdate refers to younger age" in result
        assert "no such column: birth_date" in result
        assert SQL_CONSTRAINTS in result
        assert "【correct SQL】" in result
    
    def test_decompose_template_formatting(self):
        """Test decomposition template formatting"""
        result = format_decompose_template(
            desc_str="# Table: frpm\n[(CDSCode, VARCHAR)]",
            fk_str="frpm.CDSCode = satscores.cds",
            query="List school names of charter schools",
            evidence="Charter schools refers to Charter School (Y/N) = 1",
            dataset="bird"
        )
        
        # Check example is included
        assert "Sub question 1:" in result
        assert "Sub question 2:" in result
        assert SQL_CONSTRAINTS in result
        assert "Charter schools refers to" in result
    
    def test_max_round_constant(self):
        """Test that MAX_ROUND is properly used"""
        assert MAX_ROUND == 3
        
        # Test it's used in SQL Generator
        memory = KeyValueMemory()
        sql_agent = SQLGeneratorAgent(memory, None)
        
        # The generate_sql method should accept retry_count parameter
        import inspect
        sig = inspect.signature(sql_agent.generate_sql)
        assert 'retry_count' in sig.parameters
    
    @pytest.mark.asyncio
    async def test_refine_sql_method(self):
        """Test that refine_sql method exists and works"""
        memory = KeyValueMemory()
        sql_agent = SQLGeneratorAgent(memory, None)
        
        # Check method exists
        assert hasattr(sql_agent, 'refine_sql')
        
        # The method should handle refinement
        # (Would need more setup for full integration test)
    
    def test_prompts_module_exports(self):
        """Test that all expected exports are available from prompts module"""
        from prompts import (
            MAX_ROUND,
            SQL_CONSTRAINTS,
            SUBQ_PATTERN,
            DECOMPOSE_TEMPLATE_BIRD,
            DECOMPOSE_TEMPLATE_SPIDER,
            REFINER_TEMPLATE,
            ONESHOT_TEMPLATE_1,
            ZEROSHOT_TEMPLATE,
            format_decompose_template,
            format_refiner_template,
            format_zeroshot_template
        )
        
        # Check constants
        assert isinstance(MAX_ROUND, int)
        assert isinstance(SQL_CONSTRAINTS, str)
        assert isinstance(SUBQ_PATTERN, str)
        
        # Check templates
        assert isinstance(DECOMPOSE_TEMPLATE_BIRD, str)
        assert isinstance(DECOMPOSE_TEMPLATE_SPIDER, str)
        assert isinstance(REFINER_TEMPLATE, str)
        
        # Check functions
        assert callable(format_decompose_template)
        assert callable(format_refiner_template)
        assert callable(format_zeroshot_template)


if __name__ == "__main__":
    # Run basic tests
    test = TestPromptsIntegration()
    test.test_sql_constraints_in_prompts()
    test.test_refiner_template_formatting()
    test.test_decompose_template_formatting()
    test.test_max_round_constant()
    test.test_prompts_module_exports()
    
    print("All basic tests passed!")
    
    # Run async test
    asyncio.run(test.test_refine_sql_method())
    print("Async test passed!")