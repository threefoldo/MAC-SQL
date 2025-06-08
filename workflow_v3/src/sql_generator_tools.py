"""
SQL Generator Tools for use with AutoGen agents.

This module provides various tools that the SQL Generator LLM can call:
1. Schema inspection tools - check table/column information
2. SQL execution tool - execute SQL and get actual results for iterative improvement
"""

import logging
from typing import Dict, Any, Optional, List
from database_schema_manager import DatabaseSchemaManager
from keyvalue_memory import KeyValueMemory
from sql_executor import SQLExecutor
from memory_content_types import ExecutionResult
from query_tree_manager import QueryTreeManager
from task_context_manager import TaskContextManager


class SQLGeneratorTools:
    """
    Collection of tools for SQL generation that can be used by AutoGen agents.
    """
    
    def __init__(self, memory: KeyValueMemory, logger: Optional[logging.Logger] = None):
        self.memory = memory
        self.logger = logger or logging.getLogger(__name__)
        self.schema_manager = DatabaseSchemaManager(memory)
        self.tree_manager = QueryTreeManager(memory)
        self.task_manager = TaskContextManager(memory)
    
    async def check_table_columns(self, table_name: str) -> Dict[str, Any]:
        """
        Check table and column information from the schema.
        
        This tool allows the LLM to verify table existence and get column details.
        
        Args:
            table_name: The name of the table to check
            
        Returns:
            Dictionary containing:
            - exists: boolean indicating if table exists
            - columns: list of column details if table exists
            - similar_tables: list of similar table names if table not found
            - error: error message if any
        """
        try:
            self.logger.info(f"Checking schema for table: {table_name}")
            
            # Get all tables
            all_tables = await self.schema_manager.get_all_tables()
            
            if not all_tables:
                return {
                    "exists": False,
                    "error": "No schema information available",
                    "columns": [],
                    "similar_tables": []
                }
            
            # Check if table exists (case-insensitive first)
            table_info = None
            exact_name = None
            
            for tbl_name, tbl_info in all_tables.items():
                if tbl_name.lower() == table_name.lower():
                    table_info = tbl_info
                    exact_name = tbl_name
                    break
            
            if table_info:
                # Table found - return column information
                columns = []
                # Handle both dict and TableSchema objects
                if hasattr(table_info, 'columns'):
                    # TableSchema object
                    for col_name, col_info in table_info.columns.items():
                        columns.append({
                            "name": col_name,
                            "type": col_info.dataType,
                            "description": "",
                            "is_primary": col_info.isPrimaryKey,
                            "is_foreign": col_info.isForeignKey,
                            "nullable": col_info.nullable
                        })
                else:
                    # Dictionary format (legacy)
                    for col in table_info.get("columns", []):
                        columns.append({
                            "name": col["name"],
                            "type": col.get("type", "unknown"),
                            "description": col.get("description", ""),
                            "is_primary": col.get("is_primary", False),
                            "is_foreign": col.get("is_foreign", False),
                            "nullable": col.get("nullable", True)
                        })
                
                return {
                    "exists": True,
                    "exact_name": exact_name,
                    "columns": columns,
                    "column_count": len(columns),
                    "primary_keys": [c["name"] for c in columns if c["is_primary"]],
                    "foreign_keys": [c["name"] for c in columns if c["is_foreign"]]
                }
            else:
                # Table not found - find similar tables
                similar_tables = []
                table_lower = table_name.lower()
                
                for tbl_name in all_tables.keys():
                    tbl_lower = tbl_name.lower()
                    # Check for substring match or similar names
                    if (table_lower in tbl_lower or 
                        tbl_lower in table_lower or
                        self._calculate_similarity(table_lower, tbl_lower) > 0.7):
                        similar_tables.append(tbl_name)
                
                return {
                    "exists": False,
                    "error": f"Table '{table_name}' not found in schema",
                    "columns": [],
                    "similar_tables": similar_tables[:5],  # Return top 5 similar
                    "available_tables": list(all_tables.keys()) if len(all_tables) < 20 else f"{len(all_tables)} tables available"
                }
                
        except Exception as e:
            self.logger.error(f"Error checking table columns: {str(e)}", exc_info=True)
            return {
                "exists": False,
                "error": f"Error checking schema: {str(e)}",
                "columns": [],
                "similar_tables": []
            }
    
    async def check_column_exists(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """
        Check if a specific column exists in a table.
        
        Args:
            table_name: The name of the table
            column_name: The name of the column to check
            
        Returns:
            Dictionary containing:
            - exists: boolean indicating if column exists
            - column_info: details about the column if it exists
            - similar_columns: list of similar column names if not found
            - table_exists: boolean indicating if the table exists
        """
        try:
            self.logger.info(f"Checking if column '{column_name}' exists in table '{table_name}'")
            
            # First check if table exists
            table_result = await self.check_table_columns(table_name)
            
            if not table_result["exists"]:
                return {
                    "table_exists": False,
                    "exists": False,
                    "error": table_result.get("error", f"Table '{table_name}' not found"),
                    "similar_tables": table_result.get("similar_tables", [])
                }
            
            # Table exists, check for column
            columns = table_result.get("columns", [])
            exact_name = table_result.get("exact_name", table_name)
            
            # Check for exact match (case-insensitive)
            column_info = None
            exact_col_name = None
            
            for col in columns:
                if col["name"].lower() == column_name.lower():
                    column_info = col
                    exact_col_name = col["name"]
                    break
            
            if column_info:
                return {
                    "table_exists": True,
                    "exists": True,
                    "exact_table_name": exact_name,
                    "exact_column_name": exact_col_name,
                    "column_info": column_info
                }
            else:
                # Column not found - find similar columns
                similar_columns = []
                col_lower = column_name.lower()
                
                for col in columns:
                    col_name_lower = col["name"].lower()
                    if (col_lower in col_name_lower or 
                        col_name_lower in col_lower or
                        self._calculate_similarity(col_lower, col_name_lower) > 0.7):
                        similar_columns.append(col["name"])
                
                return {
                    "table_exists": True,
                    "exists": False,
                    "exact_table_name": exact_name,
                    "error": f"Column '{column_name}' not found in table '{exact_name}'",
                    "similar_columns": similar_columns[:5],
                    "available_columns": [col["name"] for col in columns]
                }
                
        except Exception as e:
            self.logger.error(f"Error checking column existence: {str(e)}", exc_info=True)
            return {
                "table_exists": False,
                "exists": False,
                "error": f"Error checking column: {str(e)}"
            }
    
    async def execute_sql(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query and return results.
        
        This tool allows the SQL Generator to test SQL queries and see actual results,
        enabling iterative improvement during generation.
        
        Args:
            sql: The SQL query to execute
            
        Returns:
            Dictionary containing:
            - status: 'success' or 'error'
            - columns: list of column names if successful
            - data: list of result rows if successful  
            - row_count: number of rows returned
            - execution_time: time taken to execute
            - error: error message if execution failed
        """
        try:
            self.logger.info(f"Executing SQL: {sql}")
            
            # Get task context to get database name
            task_context = await self.task_manager.get()
            if not task_context:
                return {
                    "status": "error",
                    "error": "No task context found",
                    "row_count": 0,
                    "columns": [],
                    "data": []
                }
            
            db_name = task_context.databaseName
            if not db_name:
                return {
                    "status": "error",
                    "error": "No database name in task context",
                    "row_count": 0,
                    "columns": [],
                    "data": []
                }
            
            # Get data path and dataset name from database schema
            schema_summary = await self.schema_manager.get_schema_summary()
            data_path = None
            dataset_name = "bird"  # Default to bird
            
            if schema_summary and "metadata" in schema_summary:
                metadata = schema_summary["metadata"]
                data_path = metadata.get("data_path")
                dataset_name = metadata.get("dataset_name", "bird")
            
            if not data_path:
                # Try to infer from common patterns
                self.logger.warning("No data_path in database schema metadata, using default")
                data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
            
            self.logger.debug(f"Executing SQL on database {db_name}")
            self.logger.debug(f"Using data_path: {data_path}, dataset_name: {dataset_name}")
            
            executor = SQLExecutor(data_path, dataset_name)
            
            result_dict = executor.execute_sql(sql, db_name)
            
            if result_dict.get("error"):
                execution_result = {
                    "status": "error",
                    "error": result_dict["error"],
                    "row_count": 0,
                    "columns": [],
                    "data": []
                }
            else:
                execution_result = {
                    "status": "success",
                    "columns": result_dict.get("column_names", []),
                    "data": result_dict.get("data", []),
                    "row_count": len(result_dict.get("data", [])),
                    "execution_time": result_dict.get("execution_time")
                }
                
                # Save successful SQL execution to shared memory
                try:
                    current_node_id = await self.tree_manager.get_current_node_id()
                    if current_node_id:
                        # Create ExecutionResult object
                        exec_result_obj = ExecutionResult(
                            data=execution_result["data"],
                            rowCount=execution_result["row_count"],
                            error=None
                        )
                        
                        # Save to query tree node
                        await self.tree_manager.update_node_result(current_node_id, exec_result_obj, success=True)
                        
                        # Also store SQL that was successfully executed
                        await self.tree_manager.update_node_sql(current_node_id, sql)
                        
                        self.logger.info(f"Saved successful SQL execution to node {current_node_id}: {execution_result['row_count']} rows")
                    else:
                        self.logger.warning("No current node ID found - could not save execution result to shared memory")
                        
                except Exception as e:
                    self.logger.error(f"Error saving execution result to shared memory: {str(e)}", exc_info=True)
                    # Don't fail the tool call - just log the error
                    
            return execution_result
                
        except Exception as e:
            self.logger.error(f"Error executing SQL: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "row_count": 0,
                "columns": [],
                "data": []
            }
    
    async def list_all_tables(self) -> Dict[str, Any]:
        """
        List all available tables in the schema.
        
        Returns:
            Dictionary containing:
            - tables: list of table names with descriptions
            - count: total number of tables
            - error: error message if any
        """
        try:
            self.logger.info("Listing all tables in schema")
            
            all_tables = await self.schema_manager.get_all_tables()
            
            if not all_tables:
                return {
                    "tables": [],
                    "count": 0,
                    "error": "No schema information available"
                }
            
            tables_list = []
            for table_name, table_info in all_tables.items():
                # Handle both dict and TableSchema objects
                if hasattr(table_info, 'columns'):
                    # TableSchema object
                    column_count = len(table_info.columns)
                    description = ""
                else:
                    # Dictionary format (legacy)
                    column_count = len(table_info.get("columns", []))
                    description = table_info.get("description", "")
                
                tables_list.append({
                    "name": table_name,
                    "description": description,
                    "column_count": column_count
                })
            
            # Sort by name for consistency
            tables_list.sort(key=lambda x: x["name"])
            
            return {
                "tables": tables_list,
                "count": len(tables_list)
            }
            
        except Exception as e:
            self.logger.error(f"Error listing tables: {str(e)}", exc_info=True)
            return {
                "tables": [],
                "count": 0,
                "error": f"Error listing tables: {str(e)}"
            }
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate simple string similarity ratio.
        
        Returns a value between 0 and 1, where 1 is identical.
        """
        if str1 == str2:
            return 1.0
        
        # Simple character overlap ratio
        set1 = set(str1)
        set2 = set(str2)
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        if union == 0:
            return 0.0
            
        return intersection / union


def create_sql_generator_tools(memory: KeyValueMemory, logger: Optional[logging.Logger] = None) -> List[Dict[str, Any]]:
    """
    Factory function to create all SQL generator tools for AutoGen.
    
    Returns a list of tool dictionaries that can be used with FunctionTool.
    """
    tools_instance = SQLGeneratorTools(memory, logger)
    
    return [
        {
            "function": tools_instance.check_table_columns,
            "name": "check_table_columns",
            "description": "Check if a table exists and get its column information"
        },
        {
            "function": tools_instance.check_column_exists,
            "name": "check_column_exists", 
            "description": "Check if a specific column exists in a table"
        },
        {
            "function": tools_instance.execute_sql,
            "name": "execute_sql",
            "description": "Execute SQL query and return actual results for verification and iterative improvement"
        },
        {
            "function": tools_instance.list_all_tables,
            "name": "list_all_tables",
            "description": "List all available tables in the database schema"
        }
    ]