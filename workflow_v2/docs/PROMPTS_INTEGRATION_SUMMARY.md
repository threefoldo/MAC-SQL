# Prompts Integration Summary

## Overview
Successfully integrated proven prompt templates and SQL generation best practices from `core/const.py` into the `workflow_v2` agents.

## Changes Made

### 1. Created `prompts.py` Module
- **Location**: `workflow_v2/src/prompts.py`
- **Purpose**: Centralized module for all prompt templates and SQL constraints
- **Contents**:
  - `MAX_ROUND = 3` - Maximum retry attempts
  - `SQL_CONSTRAINTS` - Comprehensive SQL generation rules
  - `SUBQ_PATTERN` - Regex for parsing decomposed queries
  - Templates: `DECOMPOSE_TEMPLATE_BIRD`, `DECOMPOSE_TEMPLATE_SPIDER`, `REFINER_TEMPLATE`, etc.
  - Helper functions: `format_decompose_template()`, `format_refiner_template()`, `format_zeroshot_template()`

### 2. Updated SQL Generator Agent
- **File**: `workflow_v2/src/sql_generator_agent.py`
- **Changes**:
  - Integrated `SQL_CONSTRAINTS` into system message
  - Added `refine_sql()` method for error correction using `REFINER_TEMPLATE`
  - Enhanced retry logic with `MAX_ROUND` support
  - Added support for refiner prompt in context

### 3. Updated Query Analyzer Agent
- **File**: `workflow_v2/src/query_analyzer_agent.py`
- **Changes**:
  - Integrated `SQL_CONSTRAINTS` into system message
  - Added `SUBQ_PATTERN` for parsing decomposed queries
  - Enhanced decomposition guidelines
  - Improved evidence handling

### 4. Updated Schema Linker Agent
- **File**: `workflow_v2/src/schema_linker_agent.py`
- **Changes**:
  - Integrated `SQL_CONSTRAINTS` into system message
  - Agents now consider SQL generation constraints when selecting schema elements

## Key SQL Constraints Applied

1. **Column Selection**: Select only needed columns without unnecessary data
2. **Table Usage**: Avoid including unnecessary tables in FROM/JOIN clauses
3. **Aggregation Order**: Use JOIN before MAX/MIN functions
4. **NULL Handling**: Use `WHERE column IS NOT NULL` when appropriate
5. **Distinct Values**: Use `GROUP BY` before `ORDER BY` for distinct values
6. **Type Conversion**: Use CAST for type conversions in SQLite
7. **Column Qualification**: Always qualify columns with table aliases

## Benefits

1. **Consistent SQL Generation**: All agents now follow the same proven SQL generation rules
2. **Better Error Handling**: SQL refinement capability using proven templates
3. **Improved Query Decomposition**: Structured approach to breaking down complex queries
4. **Reduced Errors**: Systematic application of constraints prevents common SQL mistakes
5. **Maintainability**: Centralized prompts make updates easier

## Testing

Created comprehensive tests in `workflow_v2/tests/test_prompts_simple.py` to verify:
- All constants are properly defined
- Template formatting functions work correctly
- SQL constraints are properly integrated
- All expected exports are available

## Usage Example

```python
from prompts import format_refiner_template

# When SQL fails, use the refiner template
refined_prompt = format_refiner_template(
    query="What is the average salary?",
    evidence="Salary is in column A11",
    desc_str=schema_description,
    fk_str=foreign_keys,
    sql=failed_sql,
    sqlite_error="no such column: salary",
    exception_class="SQLException"
)
```

## Future Enhancements

1. Add more one-shot and few-shot examples
2. Create dataset-specific templates (BIRD vs Spider)
3. Add prompt versioning for A/B testing
4. Create prompt optimization utilities