# -*- coding: utf-8 -*-
"""SQL execution utilities for text-to-SQL tasks."""

import sqlite3
from typing import Dict, List, Any, Optional, Tuple
from func_timeout import func_set_timeout, FunctionTimedOut


class SQLExecutor:
    """
    A utility class for executing SQL queries against SQLite databases.
    
    This class handles SQL execution, error handling, and timeout management.
    """
    
    def __init__(self, data_path: str, dataset_name: str):
        """
        Initialize the SQL executor.
        
        Args:
            data_path: Path to the database files
            dataset_name: Name of the dataset (e.g., 'bird', 'spider')
        """
        self.data_path = data_path
        self.dataset_name = dataset_name
    
    @func_set_timeout(120)  # 2-minute timeout
    def execute_sql(self, sql: str, db_id: str) -> Dict[str, Any]:
        """
        Execute a SQL query against the database.
        
        Args:
            sql: The SQL query to execute
            db_id: Database identifier
            
        Returns:
            Dictionary with execution results or error information
        """
        # Get database connection with proper path based on dataset
        if self.dataset_name == "bird":
            db_path = f"{self.data_path}/dev_databases/{db_id}/{db_id}.sqlite"
        elif self.dataset_name == "spider":
            db_path = f"{self.data_path}/database/{db_id}/{db_id}.sqlite"
        else:
            db_path = f"{self.data_path}/{db_id}/{db_id}.sqlite"
            
        print(f"[SQLExecutor] Connecting to database: {db_path}")
        
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            conn.close()
            return {
                "sql": str(sql),
                "data": result[:5],  # Return at most 5 rows
                "column_names": column_names,
                "row_count": len(result),
                "sqlite_error": "",
                "exception_class": "",
                "success": True
            }
        except sqlite3.Error as er:
            conn.close()
            return {
                "sql": str(sql),
                "sqlite_error": str(' '.join(er.args)),
                "exception_class": str(er.__class__),
                "success": False
            }
        except Exception as e:
            conn.close()
            return {
                "sql": str(sql),
                "sqlite_error": str(e.args),
                "exception_class": str(type(e).__name__),
                "success": False
            }
    
    def is_valid_result(self, exec_result: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Determine if SQL execution result is valid.
        
        Args:
            exec_result: Result from execute_sql
            
        Returns:
            Tuple of (is_valid, reason_if_invalid)
        """
        # Check for execution errors
        if not exec_result.get('success', False):
            return False, exec_result.get('sqlite_error', 'Unknown error')
        
        # For Spider dataset, consider any non-error result valid
        if self.dataset_name == 'spider':
            return True, ""
        
        # For other datasets, check for empty results or NULL values
        data = exec_result.get('data', [])
        
        # Empty result set might be a problem
        if len(data) == 0:
            return False, "No data returned from query"
        
        # Check for NULL values in results
        for row in data:
            for value in row:
                if value is None:
                    return False, "Result contains NULL values, consider adding NOT NULL constraint"
        
        return True, ""
    
    def safe_execute(self, sql: str, db_id: str) -> Dict[str, Any]:
        """
        Safely execute SQL with timeout handling.
        
        Args:
            sql: The SQL query to execute
            db_id: Database identifier
            
        Returns:
            Execution result with additional timeout information
        """
        try:
            result = self.execute_sql(sql, db_id)
            result['timeout'] = False
            result['is_valid_result'] = True  # Add this for consistent response format
            result['validation_message'] = ""
            return result
        except FunctionTimedOut:
            return {
                "sql": str(sql),
                "sqlite_error": "Execution timed out (>120 seconds)",
                "exception_class": "FunctionTimedOut", 
                "timeout": True,
                "success": False,
                "is_valid_result": False,
                "validation_message": "Query execution timed out"
            }
        except Exception as e:
            return {
                "sql": str(sql),
                "sqlite_error": f"Unexpected error: {str(e)}",
                "exception_class": str(type(e).__name__),
                "timeout": False,
                "success": False,
                "is_valid_result": False,
                "validation_message": f"Unexpected error: {str(e)}"
            }