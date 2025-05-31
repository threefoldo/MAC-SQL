"""
Prompts module for text-to-SQL agents.

This module contains versioned prompt templates for all agents in the workflow.
Each agent has its own prompt file with multiple versions for A/B testing and improvement.
"""

from .prompt_loader import PromptLoader

# Legacy constants for backwards compatibility
MAX_ROUND = 3
SUBQ_PATTERN = r"Sub question\s*\d+\s*:"

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

【Evidence Priority Rules】
- When evidence provides a formula (e.g., "rate = count / total"), ALWAYS use the formula instead of pre-calculated columns
- Evidence formulas override direct column matches - trust the evidence as domain knowledge
- If evidence defines a calculation, implement it exactly as specified
- Example: If evidence says "rate = A / B", calculate it even if a "rate" column exists

【Comparison Operator Rules】
- For existence checks (e.g., "has items meeting criteria X"), use > 0 NOT >= specific_sample_value
- Sample values in schema are EXAMPLES, not thresholds for queries
- "Greater than or equal to X" in query means actual comparison to X, not to sample values from schema
- When a column counts occurrences (e.g., "NumberOfX"), query "has X" means use > 0, not >= sample_value

【Error Context Preservation】
- When retrying after errors, address the SPECIFIC issues identified by the evaluator
- Common retry fixes:
  - "NULL values in results" → Add IS NOT NULL conditions
  - "Wrong number of columns" → Check if selecting from correct table  
  - "Zero results" → Verify filter values match exactly with data
  - "Wrong calculation" → Check if using evidence formula correctly
"""

__all__ = ['PromptLoader', 'SQL_CONSTRAINTS', 'MAX_ROUND', 'SUBQ_PATTERN']