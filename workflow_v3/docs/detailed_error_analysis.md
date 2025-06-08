# Detailed Error Analysis - Text-to-SQL System Failures

## Overview
This document provides a detailed analysis of system failures with specific identification of which agents/steps failed and supporting log evidence for quick debugging.

## Error Type 1: Extra Column Selection

### Failed Examples: 1, 3, 15, 23

#### Example 1: Extra Column Selection in Rate Calculation
**Question:** "Please list the lowest three eligible free rates for students aged 5-17 in continuation schools."

**Failed Agent/Step:** SQL Generator Agent
- **Location:** `sql_generator_agent.py` - SQL generation step
- **Log Evidence (Line 4870):**
  ```
  Generated SQL:
  SELECT "School Name", "Free Meal Count (Ages 5-17)", "Enrollment (Ages 5-17)" FROM frpm WHERE "Educational Option Type" = 'Continuation School'
  ```
- **Expected SQL:**
  ```sql
  SELECT `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` FROM frpm WHERE `Educational Option Type` = 'Continuation School' AND `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` IS NOT NULL ORDER BY `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` ASC LIMIT 3
  ```

**Root Cause:** SQL Generator included contextual columns (School Name) and raw components instead of calculated rate

**Bug Location:** 
- File: `sql_generator_agent.py`
- Method: `_generate_sql_from_schema_linking`
- Issue: Column selection logic doesn't strictly follow query intent

---

#### Example 3: Full Address Instead of Street Only
**Question:** "What is the mailing street address of the K-12 school with the most students enrolled in the free or reduced price meal program?"

**Failed Agent/Step:** SQL Generator Agent
- **Location:** `sql_generator_agent.py` - Column selection phase
- **Log Evidence (Line 1571):**
  ```
  Generated SQL:
  SELECT s.MailStreet, s.MailCity, s.MailState, s.MailZip FROM frpm f INNER JOIN schools s ON f.CDSCode = s.CDSCode ORDER BY f."FRPM Count (K-12)" DESC LIMIT 1;
  ```
- **Expected:** Only `MailStreet` column

**Root Cause:** SQL Generator interpreted "mailing street address" as full address

**Bug Location:**
- File: `sql_generator_agent.py` 
- Method: Column selection logic in SQL generation
- Issue: Over-interpretation of specific field requests

---

## Error Type 2: Query Over-Engineering

### Failed Examples: 15, 19, 22

#### Example 15: Unnecessary GROUP BY Aggregation
**Question:** "What is the active elementary district that has the highest average score in Reading?"

**Failed Agent/Step:** SQL Generator Agent
- **Location:** `sql_generator_agent.py` - Query structure decision
- **Log Evidence (Line 4658):**
  ```
  Generated SQL:
  SELECT t1.dname, AVG(t1.AvgScrRead) AS avg_reading_score FROM satscores t1 INNER JOIN schools t2 ON t1.dname = t2.District WHERE t2.StatusType = 'Active' AND t1.AvgScrRead IS NOT NULL GROUP BY t1.dname ORDER BY avg_reading_score DESC LIMIT 1
  ```
- **Expected Pattern:** Simple `ORDER BY AvgScrRead DESC LIMIT 1`

**Root Cause:** SQL Generator added unnecessary GROUP BY when finding single highest value

**Bug Location:**
- File: `sql_generator_agent.py`
- Method: Query complexity decision logic
- Issue: Prefers complex aggregation over simple ordering

---

#### Example 19: MAX() Subquery Instead of ORDER BY
**Question:** "What is the phone number of the school that has the highest average score in Math?"

**Failed Agent/Step:** SQL Generator Agent  
- **Log Evidence (Line 2602):**
  ```
  Generated SQL:
  SELECT Phone FROM schools WHERE CDSCode IN ( SELECT cds FROM satscores WHERE AvgScrMath = (SELECT MAX(AvgScrMath) FROM satscores) )
  ```
- **Result:** 0 rows (equality check failed)

**Root Cause:** Complex subquery with exact equality failed due to NULL or precision issues

**Bug Location:**
- File: `sql_generator_agent.py`
- Method: Optimization pattern selection
- Issue: Chooses complex MAX() pattern over robust ORDER BY LIMIT

---

## Error Type 3: Range Condition Misinterpretation

### Failed Examples: 21

#### Example 21: Split Range Condition Across Columns
**Question:** "In Los Angeles how many schools have more than 500 free meals but less than 700 free or reduced price meals for K-12?"

**Failed Agent/Step:** Schema Linker Agent
- **Location:** `schema_linker_agent.py` - Column mapping phase
- **Log Evidence (Line 1377):**
  ```
  Generated SQL:
  SELECT COUNT(DISTINCT CDSCode) AS school_count FROM frpm WHERE "County Name" = 'Los Angeles' AND "Free Meal Count (K-12)" > 500 AND "FRPM Count (K-12)" < 700;
  ```
- **Expected:** Both conditions on same column `"Free Meal Count (K-12)"`

**Root Cause:** Schema Linker mapped "free or reduced price meals" to different column

**Bug Location:**
- File: `schema_linker_agent.py`
- Method: `_map_query_terms_to_columns`
- Issue: Doesn't detect range pattern context

---

## Error Type 4: NULL Handling Issues

### Failed Examples: 22

#### Example 22: NULL School Name Returned
**Question:** "Which school in Contra Costa County has the most SAT takers?"

**Failed Agent/Step:** SQL Generator Agent
- **Location:** `sql_generator_agent.py` - Filter generation
- **Log Evidence (Line 1546):**
  ```
  Predicted first 5 rows:
  2025-06-03 00:39:46,619 - __main__ - ERROR -   (None,)
  ```
- **Missing Filter:** `sname IS NOT NULL`

**Root Cause:** SQL Generator didn't add NULL filter for entity name queries

**Bug Location:**
- File: `sql_generator_agent.py`
- Method: WHERE clause generation
- Issue: Missing automatic NULL filtering for name fields

---

## Error Type 5: Multi-Statement Generation

### Failed Examples: 83

#### Example 83: Multiple SELECT Statements
**Question:** Complex query about magnet schools with K-8 grade span

**Failed Agent/Step:** SQL Generator Agent
- **Location:** `sql_generator_agent.py` - SQL structure generation
- **Log Evidence:**
  ```
  Generated SQL:
  -- 1. Count of schools with Magnet program, K-8 grade span, and Multiple Provision Types 
  SELECT COUNT(DISTINCT t1.CDSCode) AS magnet_k8_multiple_provision_school_count FROM schools t1 INNER JOIN frpm t2 ON t1.CDSCode = t2.CDSCode WHERE t1.Magnet = 1 AND LOWER(TRIM(t1.GSserved)) = 'k-8' AND LOWER(TRIM(t2."NSLP Provision Status")) = 'multiple provision types'; 
  -- 2. Number of cities with at least one K-8 school 
  SELECT COUNT(DISTINCT City) AS k8_city_count FROM schools WHERE LOWER(TRIM(GSserved)) = 'k-8'; 
  -- 3. Number of K-8 schools per city 
  SELECT City, COUNT(DISTINCT CDSCode) AS k8_school_count FROM schools WHERE LOWER(TRIM(GSserved)) = 'k-8' GROUP BY City;
  ```

**Root Cause:** SQL Generator created multiple statements instead of single query

**Bug Location:**
- File: `sql_generator_agent.py`
- Method: Complex query handling
- Issue: No enforcement of single-statement rule

---

## Error Type 6: Column Reference Errors

### Failed Examples: 92

#### Example 92: Non-existent Column A11
**Question:** "List out the no. of districts that have female average salary is more than 6000 but less than 10000?"

**Failed Agent/Step:** Schema Linker Agent
- **Location:** `schema_linker_agent.py` - Column identification
- **Log Evidence (Line 2815):**
  ```
  2025-06-02 15:49:25,815 - __main__ - ERROR - Error: Predicted SQL failed: no such column: A11
  ```

**Root Cause:** Schema Linker selected non-existent column

**Bug Location:**
- File: `schema_linker_agent.py`
- Method: Column validation against schema
- Issue: No schema validation before passing to SQL Generator

---

## Error Type 7: Complex Multi-Table Issues

### Failed Examples: 25

#### Example 25: Multiple Compounding Errors
**Question:** "Name schools in Riverside which the average of average math score for SAT is grater than 400, what is the funding type of these schools?"

**Failed Agents/Steps:** Multiple agents failed
1. **Schema Linker Agent** - Wrong table selection
   - **Log Evidence:** Used `schools.FundingType` instead of `frpm.Charter Funding Type`
   
2. **Query Analyzer Agent** - Evidence misinterpretation
   - **Evidence Given:** "Average of average math = sum(average math scores) / count(schools)."
   - **Generated:** Simple filter instead of GROUP BY with aggregation
   
3. **Schema Linker Agent** - Geographic granularity error
   - **Generated:** `County = 'Riverside'` instead of `District Name LIKE 'Riverside%'`

**Bug Locations:**
- File: `schema_linker_agent.py` - Table priority rules
- File: `query_analyzer_agent.py` - Evidence parsing logic
- File: `schema_linker_agent.py` - Geographic filter mapping

---

## Quick Debugging Guide

### For Column Selection Errors:
1. Check `sql_generator_agent.py` → `_generate_sql_from_schema_linking` method
2. Look for column justification logic
3. Search logs for "Generated SQL:" to see actual output

### For Query Complexity Errors:
1. Check `sql_generator_agent.py` → Query optimization patterns
2. Look for ORDER BY vs subquery decision logic
3. Search logs for aggregation patterns

### For Schema Linking Errors:
1. Check `schema_linker_agent.py` → `_map_query_terms_to_columns`
2. Verify table selection logic
3. Search logs for "schema_linking" XML blocks

### For NULL Handling:
1. Check `sql_generator_agent.py` → WHERE clause generation
2. Look for NULL filtering rules
3. Search logs for "IS NOT NULL" presence

### For Multi-Statement Issues:
1. Check `sql_generator_agent.py` → SQL structure validation
2. Look for statement counting logic
3. Search logs for multiple SELECT keywords

### For Column Validation:
1. Check `schema_linker_agent.py` → Schema validation
2. Verify column existence checks
3. Search logs for "no such column" errors

## Recommended Fixes Priority

1. **Critical (Week 1)**
   - Add column existence validation in Schema Linker
   - Enforce single-statement generation in SQL Generator
   - Add strict column selection validation

2. **High (Week 2)**
   - Implement range pattern detection
   - Add NULL filtering for entity queries
   - Create query simplification preferences

3. **Medium (Week 3)**
   - Enhance evidence parsing in Query Analyzer
   - Implement table source priority rules
   - Add geographic granularity detection

## Testing Recommendations

For each fix, test with the specific failed examples mentioned:
- Column Selection: Test with examples 1, 3, 15, 23
- Query Complexity: Test with examples 15, 19, 22
- Range Conditions: Test with example 21
- NULL Handling: Test with example 22
- Multi-Statement: Test with example 83
- Column Validation: Test with example 92
- Multi-Table Logic: Test with example 25