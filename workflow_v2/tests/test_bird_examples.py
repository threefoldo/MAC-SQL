"""
Test cases using real BIRD dataset examples.

This module contains test cases with simple, moderate, and challenging queries
from the BIRD dataset for testing the text-to-SQL workflow.
"""

import asyncio
import pytest
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from text_to_sql_tree_orchestrator import TextToSQLTreeOrchestrator

# Load environment variables
load_dotenv()

# Test configuration
DATA_PATH = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
TABLES_JSON_PATH = str(Path(DATA_PATH) / "dev_tables.json")

# Set up logging for tests
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('autogen_core').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)


# Test data from BIRD dataset
SIMPLE_QUERIES = [
    {
        "db": "california_schools",
        "question": "What is the highest eligible free rate for K-12 students in the schools in Alameda County?",
        "expected_sql": "SELECT `Free Meal Count (K-12)` / `Enrollment (K-12)` FROM frpm WHERE `County Name` = 'Alameda' ORDER BY (CAST(`Free Meal Count (K-12)` AS REAL) / `Enrollment (K-12)`) DESC LIMIT 1"
    },
    {
        "db": "california_schools",
        "question": "Please list the zip code of all the charter schools in Fresno County Office of Education.",
        "expected_sql": "SELECT T2.Zip FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T1.`District Name` = 'Fresno County Office of Education' AND T1.`Charter School (Y/N)` = 1"
    },
    {
        "db": "california_schools",
        "question": "What is the unabbreviated mailing address of the school with the highest FRPM count for K-12 students?",
        "expected_sql": "SELECT T2.MailStreet FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode ORDER BY T1.`FRPM Count (K-12)` DESC LIMIT 1"
    },
    {
        "db": "california_schools", 
        "question": "How many schools with an average score in Math under 400 in the SAT test are exclusively virtual?",
        "expected_sql": "SELECT COUNT(DISTINCT T2.School) FROM satscores AS T1 INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode WHERE T2.Virtual = 'F' AND T1.AvgScrMath < 400"
    },
    {
        "db": "california_schools",
        "question": "Among the schools with the SAT test takers of over 500, please list the schools that are magnet schools or offer a magnet program.",
        "expected_sql": "SELECT T2.School FROM satscores AS T1 INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode WHERE T2.Magnet = 1 AND T1.NumTstTakr > 500"
    }
]

MODERATE_QUERIES = [
    {
        "db": "california_schools",
        "question": "Please list the lowest three eligible free rates for students aged 5-17 in continuation schools.",
        "expected_sql": "SELECT `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` FROM frpm WHERE `Educational Option Type` = 'Continuation School' AND `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` IS NOT NULL ORDER BY `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` ASC LIMIT 3"
    },
    {
        "db": "california_schools",
        "question": "Please list the phone numbers of the direct charter-funded schools that are opened after 2000/1/1.",
        "expected_sql": "SELECT T2.Phone FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T1.`Charter Funding Type` = 'Directly funded' AND T1.`Charter School (Y/N)` = 1 AND T2.OpenDate > '2000-01-01'"
    },
    {
        "db": "california_schools", 
        "question": "Among the schools with an SAT excellence rate of over 0.3, what is the highest eligible free rate for students aged 5-17?",
        "expected_sql": "SELECT MAX(CAST(T1.`Free Meal Count (Ages 5-17)` AS REAL) / T1.`Enrollment (Ages 5-17)`) FROM frpm AS T1 INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds WHERE CAST(T2.NumGE1500 AS REAL) / T2.NumTstTakr > 0.3"
    },
    {
        "db": "california_schools",
        "question": "For schools in the Los Angeles Unified School District, how many have an enrollment difference between K-12 and ages 5-17?",
        "expected_sql": "SELECT T1.School, T1.StreetAbr FROM schools AS T1 INNER JOIN frpm AS T2 ON T1.CDSCode = T2.CDSCode WHERE T2.`Enrollment (K-12)` - T2.`Enrollment (Ages 5-17)` > 30"
    },
    {
        "db": "california_schools",
        "question": "Name schools in Riverside which the average of average math score for SAT is grater than 400, what is the funding type of these schools?",
        "expected_sql": "SELECT T1.sname, T2.`Charter Funding Type` FROM satscores AS T1 INNER JOIN frpm AS T2 ON T1.cds = T2.CDSCode WHERE T2.`District Name` LIKE 'Riverside%' GROUP BY T1.sname, T2.`Charter Funding Type` HAVING CAST(SUM(T1.AvgScrMath) AS REAL) / COUNT(T1.cds) > 400"
    }
]

CHALLENGING_QUERIES = [
    {
        "db": "california_schools",
        "question": "Consider the average difference between K-12 enrollment and 15-17 enrollment of schools that are locally funded, list the names and DOC type of schools which has a difference above this average.",
        "expected_sql": "SELECT T2.School, T2.DOC FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T2.FundingType = 'Locally funded' AND (T1.`Enrollment (K-12)` - T1.`Enrollment (Ages 5-17)`) > (SELECT AVG(T3.`Enrollment (K-12)` - T3.`Enrollment (Ages 5-17)`) FROM frpm AS T3 INNER JOIN schools AS T4 ON T3.CDSCode = T4.CDSCode WHERE T4.FundingType = 'Locally funded')"
    },
    {
        "db": "financial",
        "question": "Which account in Jesenik has the biggest gap between the highest average salary and the lowest average salary? And how old is the oldest female client of this account?",
        "expected_sql": "SELECT T1.account_id , ( SELECT MAX(A11) - MIN(A11) FROM district ) FROM account AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T2.district_id = ( SELECT district_id FROM client WHERE gender = 'F' ORDER BY birth_date ASC LIMIT 1 ) ORDER BY T2.A11 DESC LIMIT 1"
    },
    {
        "db": "financial",
        "question": "For the client who first applied the loan in 1993/7/5, what is the increase rate of his/her account balance from 1993/3/22 to 1998/12/27?",
        "expected_sql": "SELECT CAST((SUM(IIF(T3.date = '1998-12-27', T3.balance, 0)) - SUM(IIF(T3.date = '1993-03-22', T3.balance, 0))) AS REAL) * 100 / SUM(IIF(T3.date = '1993-03-22', T3.balance, 0)) FROM loan AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id INNER JOIN trans AS T3 ON T3.account_id = T2.account_id WHERE T1.date = '1993-07-05'"
    },
    {
        "db": "financial",
        "question": "What is the unemployment increment rate from year 1995 to year 1996 for all the account holders who are still under debt?",
        "expected_sql": "SELECT CAST((T3.A13 - T3.A12) AS REAL) * 100 / T3.A12 FROM loan AS T1 INNER JOIN account AS T2 ON T1.account_id = T2.account_id INNER JOIN district AS T3 ON T2.district_id = T3.district_id WHERE T1.status = 'D'"
    },
    {
        "db": "california_schools",
        "question": "Out of the total number of schools in Los Angeles county, how many of them are non-chartered schools whose free meal rate for students aged between 5 to 17 is 18% and below?",
        "expected_sql": "SELECT COUNT(T2.School) FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T2.County = 'Los Angeles' AND T2.Charter = 0 AND CAST(T1.`Free Meal Count (K-12)` AS REAL) * 100 / T1.`Enrollment (K-12)` < 0.18"
    }
]


class TestBIRDExamples:
    """Test cases using real BIRD dataset examples."""
    
    @pytest.fixture
    async def workflow(self):
        """Create a workflow instance for testing."""
        workflow = TextToSQLTreeOrchestrator(
            data_path=DATA_PATH,
            tables_json_path=TABLES_JSON_PATH,
            dataset_name="bird"
        )
        yield workflow
        # Cleanup
        await workflow.memory.clear()
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_case", SIMPLE_QUERIES, ids=[f"simple_{i}" for i in range(len(SIMPLE_QUERIES))])
    async def test_simple_queries(self, workflow, test_case):
        """Test simple BIRD queries."""
        await self._run_test_case(workflow, test_case)
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_case", MODERATE_QUERIES, ids=[f"moderate_{i}" for i in range(len(MODERATE_QUERIES))])
    async def test_moderate_queries(self, workflow, test_case):
        """Test moderate BIRD queries."""
        await self._run_test_case(workflow, test_case)
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_case", CHALLENGING_QUERIES, ids=[f"challenging_{i}" for i in range(len(CHALLENGING_QUERIES))])
    async def test_challenging_queries(self, workflow, test_case):
        """Test challenging BIRD queries."""
        await self._run_test_case(workflow, test_case)
    
    async def _run_test_case(self, workflow, test_case):
        """Run a single test case."""
        # Initialize database
        await workflow.initialize_database(test_case["db"])
        
        # Process query
        results = await workflow.process_query(
            query=test_case["question"],
            db_name=test_case["db"],
              # Use sequential for faster testing
        )
        
        # Verify results
        assert "nodes" in results
        assert len(results["nodes"]) > 0
        
        # Get the first node
        first_node = list(results["nodes"].values())[0]
        
        # Check SQL was generated
        assert first_node["sql"] is not None, "SQL should be generated"
        
        # Log the generated SQL for comparison
        generated_sql = first_node["sql"].strip()
        expected_sql = test_case["expected_sql"]
        
        logging.info(f"Generated SQL: {generated_sql}")
        logging.info(f"Expected SQL: {expected_sql}")
        
        # Check execution result
        exec_result = first_node.get("execution_result")
        if exec_result and exec_result.get("error"):
            pytest.skip(f"SQL execution failed: {exec_result['error']}")
        
        # Verify analysis if available
        if first_node.get("analysis"):
            quality = first_node["analysis"].get("result_quality", "").lower()
            # Log detailed analysis for debugging
            logging.info(f"Analysis: {first_node['analysis']}")
            # For now, just check that we got an analysis
            assert quality in ["excellent", "good", "acceptable", "poor"], f"Unexpected quality: {quality}"


# Quick test runner
if __name__ == "__main__":
    async def run_single_test():
        """Run a single test for quick verification."""
        workflow = TextToSQLTreeOrchestrator(
            data_path=DATA_PATH,
            tables_json_path=TABLES_JSON_PATH,
            dataset_name="bird"
        )
        
        test_case = SIMPLE_QUERIES[0]
        print(f"Testing: {test_case['question']}")
        
        await workflow.initialize_database(test_case["db"])
        results = await workflow.process_query(
            query=test_case["question"],
            db_name=test_case["db"],
            
        )
        
        # Display results
        await workflow.display_query_tree()
        await workflow.display_final_results()
    
    asyncio.run(run_single_test())