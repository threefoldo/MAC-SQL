"""
Shared prompt templates and SQL generation constraints.

This module contains proven prompt templates and SQL generation best practices
imported from core/const.py and adapted for the workflow_v2 agents.
"""

# Maximum retry attempts for agent operations
MAX_ROUND = 3

# SQL Generation Constraints - Used across all SQL-generating agents
SQL_CONSTRAINTS = """
【Constraints】
- In `SELECT <column>`, just select needed columns in the 【Question】 without any unnecessary column or value
- In `FROM <table>` or `JOIN <table>`, do not include unnecessary table
- If use max or min func, `JOIN <table>` FIRST, THEN use `SELECT MAX(<column>)` or `SELECT MIN(<column>)`
- If [Value examples] of <column> has 'None' or None, use `JOIN <table>` or `WHERE <column> is NOT NULL` is better
- If use `ORDER BY <column> ASC|DESC`, add `GROUP BY <column>` before to select distinct values
- Ensure all columns are qualified with table aliases to avoid ambiguity
- Use CAST for type conversions in SQLite
- Include only necessary columns (avoid SELECT *)
"""

# Pattern for parsing sub-questions in decomposition
SUBQ_PATTERN = r"Sub question\s*\d+\s*:"

# Decomposition template for BIRD dataset
DECOMPOSE_TEMPLATE_BIRD = """
Given a 【Database schema】 description, a knowledge 【Evidence】 and the 【Question】, you need to use valid SQLite and understand the database and knowledge, and then decompose the question into subquestions for text-to-SQL generation.
When generating SQL, we should always consider constraints:
{constraints}

==========

【Database schema】
# Table: frpm
[
  (CDSCode, CDSCode. Value examples: ['01100170109835', '01100170112607'].),
  (Charter School (Y/N), Charter School (Y/N). Value examples: [1, 0, None]. And 0: N;. 1: Y),
  (Enrollment (Ages 5-17), Enrollment (Ages 5-17). Value examples: [5271.0, 4734.0].),
  (Free Meal Count (Ages 5-17), Free Meal Count (Ages 5-17). Value examples: [3864.0, 2637.0]. And eligible free rate = Free Meal Count / Enrollment)
]
# Table: satscores
[
  (cds, California Department Schools. Value examples: ['10101080000000', '10101080109991'].),
  (sname, school name. Value examples: ['None', 'Middle College High', 'John F. Kennedy High', 'Independence High', 'Foothill High'].),
  (NumTstTakr, Number of Test Takers in this school. Value examples: [24305, 4942, 1, 0, 280]. And number of test takers in each school),
  (AvgScrMath, average scores in Math. Value examples: [699, 698, 289, None, 492]. And average scores in Math),
  (NumGE1500, Number of Test Takers Whose Total SAT Scores Are Greater or Equal to 1500. Value examples: [5837, 2125, 0, None, 191]. And Number of Test Takers Whose Total SAT Scores Are Greater or Equal to 1500. . commonsense evidence:. . Excellence Rate = NumGE1500 / NumTstTakr)
]
【Foreign keys】
frpm.`CDSCode` = satscores.`cds`
【Question】
List school names of charter schools with an SAT excellence rate over the average.
【Evidence】
Charter schools refers to `Charter School (Y/N)` = 1 in the table frpm; Excellence rate = NumGE1500 / NumTstTakr


Decompose the question into sub questions, considering 【Constraints】, and generate the SQL after thinking step by step:
Sub question 1: Get the average value of SAT excellence rate of charter schools.
SQL
```sql
SELECT AVG(CAST(T2.`NumGE1500` AS REAL) / T2.`NumTstTakr`)
    FROM frpm AS T1
    INNER JOIN satscores AS T2
    ON T1.`CDSCode` = T2.`cds`
    WHERE T1.`Charter School (Y/N)` = 1
```

Sub question 2: List out school names of charter schools with an SAT excellence rate over the average.
SQL
```sql
SELECT T2.`sname`
  FROM frpm AS T1
  INNER JOIN satscores AS T2
  ON T1.`CDSCode` = T2.`cds`
  WHERE T2.`sname` IS NOT NULL
  AND T1.`Charter School (Y/N)` = 1
  AND CAST(T2.`NumGE1500` AS REAL) / T2.`NumTstTakr` > (
    SELECT AVG(CAST(T4.`NumGE1500` AS REAL) / T4.`NumTstTakr`)
    FROM frpm AS T3
    INNER JOIN satscores AS T4
    ON T3.`CDSCode` = T4.`cds`
    WHERE T3.`Charter School (Y/N)` = 1
  )
```

Question Solved.

==========

【Database schema】
{desc_str}
【Foreign keys】
{fk_str}
【Question】
{query}
【Evidence】
{evidence}

Decompose the question into sub questions, considering 【Constraints】, and generate the SQL after thinking step by step:
"""

# Decomposition template for Spider dataset
DECOMPOSE_TEMPLATE_SPIDER = """
Given a 【Database schema】 description, and the 【Question】, you need to use valid SQLite and understand the database, and then generate the corresponding SQL.

==========

【Database schema】
# Table: stadium
[
  (Stadium_ID, stadium id. Value examples: [1, 2, 3, 4, 5, 6].),
  (Location, location. Value examples: ['Stirling Albion', 'Raith Rovers', "Queen's Park", 'Peterhead', 'East Fife', 'Brechin City'].),
  (Name, name. Value examples: ["Stark's Park", 'Somerset Park', 'Recreation Park', 'Hampden Park', 'Glebe Park', 'Gayfield Park'].),
  (Capacity, capacity. Value examples: [52500, 11998, 10104, 4125, 4000, 3960].),
  (Highest, highest. Value examples: [4812, 2363, 1980, 1763, 1125, 1057].),
  (Lowest, lowest. Value examples: [1294, 1057, 533, 466, 411, 404].),
  (Average, average. Value examples: [2106, 1477, 864, 730, 642, 638].)
]
# Table: concert
[
  (concert_ID, concert id. Value examples: [1, 2, 3, 4, 5, 6].),
  (concert_Name, concert name. Value examples: ['Week 1', 'Week 2', 'Super bootcamp', 'Home Visits', 'Auditions'].),
  (Theme, theme. Value examples: ['Wide Awake', 'Party All Night', 'Happy Tonight', 'Free choice 2', 'Free choice', 'Bleeding Love'].),
  (Stadium_ID, stadium id. Value examples: ['2', '9', '7', '10', '1'].),
  (Year, year. Value examples: ['2015', '2014'].)
]
【Foreign keys】
concert.`Stadium_ID` = stadium.`Stadium_ID`
【Question】
Show the stadium name and the number of concerts in each stadium.

SQL
```sql
SELECT T1.`Name`, COUNT(*) FROM stadium AS T1 JOIN concert AS T2 ON T1.`Stadium_ID` = T2.`Stadium_ID` GROUP BY T1.`Stadium_ID`
```

Question Solved.

==========

【Database schema】
{desc_str}
【Foreign keys】
{fk_str}
【Question】
{query}

SQL

"""

# SQL Refinement template for fixing errors
REFINER_TEMPLATE = """
【Instruction】
When executing SQL below, some errors occurred, please fix up SQL based on query and database info.
Solve the task step by step if you need to. Using SQL format in the code block, and indicate script type in the code block.
When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
{constraints}
【Query】
-- {query}
【Evidence】
{evidence}
【Database info】
{desc_str}
【Foreign keys】
{fk_str}
【old SQL】
```sql
{sql}
```
【SQLite error】 
{sqlite_error}
【Exception class】
{exception_class}

Now please fixup old SQL and generate new SQL again.
【correct SQL】
"""

# One-shot example template 1
ONESHOT_TEMPLATE_1 = """
Given a 【Database schema】 description, a knowledge 【Evidence】 and the 【Question】, you need to use valid SQLite and understand the database and knowledge, and then decompose the question into subquestions for text-to-SQL generation.
When generating SQL, we should always consider constraints:
{constraints}

==========

【Database schema】
# Table: frpm
[
  (CDSCode, CDSCode. Value examples: ['01100170109835', '01100170112607'].),
  (Charter School (Y/N), Charter School (Y/N). Value examples: [1, 0, None]. And 0: N;. 1: Y),
  (Enrollment (Ages 5-17), Enrollment (Ages 5-17). Value examples: [5271.0, 4734.0, 4718.0].),
  (Free Meal Count (Ages 5-17), Free Meal Count (Ages 5-17). Value examples: [3864.0, 2637.0, 2573.0]. And eligible free rate = Free Meal Count / Enrollment)
]
# Table: satscores
[
  (cds, California Department Schools. Value examples: ['10101080000000', '10101080109991'].),
  (sname, school name. Value examples: ['None', 'Middle College High', 'John F. Kennedy High', 'Independence High', 'Foothill High'].),
  (NumTstTakr, Number of Test Takers in this school. Value examples: [24305, 4942, 1, 0, 280]. And number of test takers in each school),
  (AvgScrMath, average scores in Math. Value examples: [699, 698, 289, None, 492]. And average scores in Math),
  (NumGE1500, Number of Test Takers Whose Total SAT Scores Are Greater or Equal to 1500. Value examples: [5837, 2125, 0, None, 191]. And Number of Test Takers Whose Total SAT Scores Are Greater or Equal to 1500. And commonsense evidence: Excellence Rate = NumGE1500 / NumTstTakr)
]
【Foreign keys】
frpm.`CDSCode` = satscores.`cds`
【Question】
List school names of charter schools with an SAT excellence rate over the average.
【Evidence】
Charter schools refers to `Charter School (Y/N)` = 1 in the table frpm; Excellence rate = NumGE1500 / NumTstTakr


Decompose the question into sub questions, considering 【Constraints】, and generate the SQL after thinking step by step:
Sub question 1: Get the average value of SAT excellence rate of charter schools.
SQL
```sql
SELECT AVG(CAST(T2.`NumGE1500` AS REAL) / T2.`NumTstTakr`)
    FROM frpm AS T1
    INNER JOIN satscores AS T2
    ON T1.`CDSCode` = T2.`cds`
    WHERE T1.`Charter School (Y/N)` = 1
```

Sub question 2: List out school names of charter schools with an SAT excellence rate over the average.
SQL
```sql
SELECT T2.`sname`
  FROM frpm AS T1
  INNER JOIN satscores AS T2
  ON T1.`CDSCode` = T2.`cds`
  WHERE T2.`sname` IS NOT NULL
  AND T1.`Charter School (Y/N)` = 1
  AND CAST(T2.`NumGE1500` AS REAL) / T2.`NumTstTakr` > (
    SELECT AVG(CAST(T4.`NumGE1500` AS REAL) / T4.`NumTstTakr`)
    FROM frpm AS T3
    INNER JOIN satscores AS T4
    ON T3.`CDSCode` = T4.`cds`
    WHERE T3.`Charter School (Y/N)` = 1
  )
```

Question Solved.

==========

【Database schema】
{desc_str}
【Foreign keys】
{fk_str}
【Question】
{query}
【Evidence】
{evidence}

Decompose the question into sub questions, considering 【Constraints】, and generate the SQL after thinking step by step:
"""

# Zero-shot template for direct SQL generation
ZEROSHOT_TEMPLATE = """
Given a 【Database schema】 description, a knowledge 【Evidence】 and the 【Question】, you need to use valid SQLite and understand the database and knowledge, and then generate SQL.
You can write answer in script blocks, and indicate script type in it, like this:
```sql
SELECT column_a
FROM table_b
```
When generating SQL, we should always consider constraints:
{constraints}

Now let's start!

【Database schema】
{desc_str}
【Foreign keys】
{fk_str}
【Question】
{query}
【Evidence】
{evidence}
【Answer】
"""


def format_decompose_template(desc_str: str, fk_str: str, query: str, evidence: str, dataset: str = "bird") -> str:
    """
    Format the decomposition template with actual values.
    
    Args:
        desc_str: Database schema description
        fk_str: Foreign key constraints
        query: User query
        evidence: Evidence/hints
        dataset: Dataset type ("bird" or "spider")
    
    Returns:
        Formatted template string
    """
    template = DECOMPOSE_TEMPLATE_BIRD if dataset == "bird" else DECOMPOSE_TEMPLATE_SPIDER
    
    return template.format(
        constraints=SQL_CONSTRAINTS,
        desc_str=desc_str,
        fk_str=fk_str,
        query=query,
        evidence=evidence
    )


def format_refiner_template(query: str, evidence: str, desc_str: str, fk_str: str, 
                           sql: str, sqlite_error: str, exception_class: str) -> str:
    """
    Format the refiner template for SQL error correction.
    
    Args:
        query: Original user query
        evidence: Evidence/hints
        desc_str: Database schema description
        fk_str: Foreign key constraints
        sql: The SQL that failed
        sqlite_error: Error message from SQLite
        exception_class: Exception class name
    
    Returns:
        Formatted template string
    """
    return REFINER_TEMPLATE.format(
        constraints=SQL_CONSTRAINTS,
        query=query,
        evidence=evidence or "None",
        desc_str=desc_str,
        fk_str=fk_str,
        sql=sql,
        sqlite_error=sqlite_error,
        exception_class=exception_class
    )


def format_zeroshot_template(desc_str: str, fk_str: str, query: str, evidence: str) -> str:
    """
    Format the zero-shot template for direct SQL generation.
    
    Args:
        desc_str: Database schema description
        fk_str: Foreign key constraints
        query: User query
        evidence: Evidence/hints
    
    Returns:
        Formatted template string
    """
    return ZEROSHOT_TEMPLATE.format(
        constraints=SQL_CONSTRAINTS,
        desc_str=desc_str,
        fk_str=fk_str,
        query=query,
        evidence=evidence or "None"
    )