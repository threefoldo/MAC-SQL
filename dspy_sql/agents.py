"""
DSPy Agents for Text-to-SQL
This module contains the implementations of the three main agents:
1. SchemaExtractor - Selects relevant tables and columns
2. SqlDecomposer - Decomposes complex questions into sub-questions
3. SqlValidator - Validates and refines generated SQL
"""

import sqlite3
import logging
from func_timeout import func_set_timeout, FunctionTimedOut
import dspy

# Import from original codebase
from core.utils import parse_json, parse_sql_from_string, add_prefix

# Import from local modules
from dspy_sql.models import (
    schema_extractor_signature,
    sql_decomposer_signature,
    sql_validator_signature
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SchemaExtractor(dspy.Module):
    """DSPy module to extract relevant schema from database"""
    
    def __init__(self, lm=None):
        super().__init__()
        self.lm = lm
        # Define a chain of thought predictor for schema extraction
        self.predictor = dspy.ChainOfThought(
            schema_extractor_signature,
            lm=lm
        )
    
    def forward(self, db_id, query, db_schema, foreign_keys, evidence=""):
        """Extract the relevant schema based on the query"""
        result = self.predictor(
            db_id=db_id,
            query=query,
            db_schema=db_schema,
            foreign_keys=foreign_keys, 
            evidence=evidence
        )
        
        # Try to parse JSON from the result
        try:
            extracted_schema = parse_json(result.extracted_schema)
            return dspy.Prediction(extracted_schema=extracted_schema)
        except Exception as e:
            logger.error(f"Error parsing extracted schema: {e}")
            return dspy.Prediction(extracted_schema={})


class SqlDecomposer(dspy.Module):
    """DSPy module to decompose SQL queries using chain of thought reasoning"""
    
    def __init__(self, dataset_name="bird", lm=None):
        super().__init__()
        self.lm = lm
        self.dataset_name = dataset_name
        # Define a chain of thought predictor for SQL decomposition
        self.predictor = dspy.ChainOfThought(
            sql_decomposer_signature,
            lm=lm
        )
    
    def forward(self, query, schema_info, foreign_keys, evidence=""):
        """Decompose the query into sub-questions and generate SQL"""
        result = self.predictor(
            query=query,
            schema_info=schema_info,
            foreign_keys=foreign_keys,
            evidence=evidence
        )
        
        # Try to extract SQL from the result
        try:
            sql = parse_sql_from_string(result.sql)
            return dspy.Prediction(
                sub_questions=result.sub_questions,
                sql=sql
            )
        except Exception as e:
            logger.error(f"Error parsing SQL: {e}")
            return dspy.Prediction(
                sub_questions=result.sub_questions,
                sql=f"error: {str(e)}"
            )


class SqlValidator(dspy.Module):
    """DSPy module to validate and refine SQL queries"""
    
    def __init__(self, data_path, dataset_name="bird", lm=None):
        super().__init__()
        self.lm = lm
        self.data_path = data_path
        self.dataset_name = dataset_name
        
        # Define a chain of thought predictor for SQL validation
        self.predictor = dspy.ChainOfThought(
            sql_validator_signature,
            lm=lm
        )
    
    @func_set_timeout(120)
    def _execute_sql(self, sql: str, db_id: str) -> dict:
        """Execute SQL with timeout and error handling"""
        db_path = f"{self.data_path}/{db_id}/{db_id}.sqlite"
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            return {
                "sql": str(sql),
                "data": result[:5],
                "sqlite_error": "",
                "exception_class": ""
            }
        except sqlite3.Error as er:
            return {
                "sql": str(sql),
                "sqlite_error": str(' '.join(er.args)),
                "exception_class": str(er.__class__)
            }
        except Exception as e:
            return {
                "sql": str(sql),
                "sqlite_error": str(e.args),
                "exception_class": str(type(e).__name__)
            }
    
    def _is_need_refine(self, exec_result: dict) -> bool:
        """Determine if SQL needs refinement based on execution results"""
        # Spider dataset handling
        if self.dataset_name == 'spider':
            if 'data' not in exec_result:
                return True
            return False
        
        # BIRD and other datasets: check for empty results or None values
        data = exec_result.get('data', None)
        if data is not None:
            if len(data) == 0:
                exec_result['sqlite_error'] = 'no data selected'
                return True
            for t in data:
                for n in t:
                    if n is None:
                        exec_result['sqlite_error'] = 'exist None value, you can add `NOT NULL` in SQL'
                        return True
            return False
        else:
            return True
    
    def forward(self, query, sql, schema_info, foreign_keys, db_id, evidence=""):
        """Validate and refine SQL if necessary"""
        # Skip refinement if SQL contains "error"
        if 'error' in sql:
            return dspy.Prediction(
                refined_sql=sql,
                explanation="SQL contains errors, skipping refinement"
            )
        
        # Execute SQL to check for errors
        try:
            error_info = self._execute_sql(sql, db_id)
        except (Exception, FunctionTimedOut) as e:
            logger.warning(f"SQL execution timeout or error: {str(e)}")
            # Return original SQL if execution fails
            return dspy.Prediction(
                refined_sql=sql,
                explanation="SQL execution timed out or failed"
            )
        
        # Check if refinement is needed
        if not self._is_need_refine(error_info):
            # SQL execution succeeded, no refinement needed
            return dspy.Prediction(
                refined_sql=sql,
                explanation="SQL execution successful, no refinement needed"
            )
        
        # Format error info for the predictor
        error_info_str = (
            f"Error type: {error_info.get('exception_class', '')}\n"
            f"Error message: {error_info.get('sqlite_error', '')}"
        )
        
        # Refinement needed - pass error info to the predictor
        result = self.predictor(
            query=query,
            sql=sql,
            schema_info=schema_info,
            foreign_keys=foreign_keys,
            error_info=error_info_str,
            evidence=evidence
        )
        
        # Try to extract refined SQL
        try:
            refined_sql = parse_sql_from_string(result.refined_sql)
            return dspy.Prediction(
                refined_sql=refined_sql,
                explanation=result.explanation
            )
        except Exception as e:
            logger.error(f"Error parsing refined SQL: {e}")
            return dspy.Prediction(
                refined_sql=sql,  # Fall back to original
                explanation=f"Failed to parse refined SQL: {e}"
            )