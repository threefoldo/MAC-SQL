"""
Test cases for the text-to-SQL workflow.

This module contains sample test cases for evaluating the text-to-SQL workflow.
"""

# Sample test cases for the BIRD dataset
BIRD_TEST_CASES = [
    # Test case 1: Excellence rate calculation (basic join with aggregation)
    {
        "db_id": "california_schools",
        "query": "List school names of charter schools with an SAT excellence rate over the average.",
        "evidence": "Charter schools refers to `Charter School (Y/N)` = 1 in the table frpm; Excellence rate = NumGE1500 / NumTstTakr"
    },
    
    # Test case 2: Multi-table query with numeric conditions (multiple joins)
    {
        "db_id": "game_injury",
        "query": "Show the names of players who have been injured for more than 3 matches in the 2010 season.",
        "evidence": "Season info is in the game table with year 2010; injury severity is measured by the number of matches a player misses."
    },
    
    # Test case 3: Complex aggregation with grouping
    {
        "db_id": "formula_1",
        "query": "What is the name of the driver who has won the most races in rainy conditions?",
        "evidence": "Weather conditions are recorded in the races table; winner information is in the results table."
    },
    
    # Test case 4: Temporal query with date handling
    {
        "db_id": "loan_data",
        "query": "Find the customer with the highest total payment amount for loans taken in the first quarter of 2011.",
        "evidence": "First quarter means January to March (months 1-3); loan dates are stored in ISO format (YYYY-MM-DD)."
    }
]

# Sample test cases for the Spider dataset
SPIDER_TEST_CASES = [
    # Test case 1: Simple aggregation
    {
        "db_id": "concert_singer",
        "query": "Show the stadium name and the number of concerts in each stadium.",
        "evidence": ""
    },
    
    # Test case 2: Nested query
    {
        "db_id": "concert_singer",
        "query": "Show the name and the release year of the song by the youngest singer.",
        "evidence": ""
    },
    
    # Test case 3: Multi-table join with filtering
    {
        "db_id": "concert_singer",
        "query": "List all singers who performed at least one concert in the USA.",
        "evidence": ""
    },
    
    # Test case 4: Grouping with having clause
    {
        "db_id": "concert_singer",
        "query": "Find countries with more than 5 singers.",
        "evidence": ""
    }
]

# Function to get test cases by dataset
def get_test_cases(dataset_name: str):
    """
    Get test cases for a specific dataset.
    
    Args:
        dataset_name: Name of the dataset ('bird' or 'spider')
        
    Returns:
        List of test case dictionaries
    """
    if dataset_name.lower() == 'bird':
        return BIRD_TEST_CASES
    elif dataset_name.lower() == 'spider':
        return SPIDER_TEST_CASES
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")