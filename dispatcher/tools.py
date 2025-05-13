import os
import json
import sqlite3
import traceback
from typing import Dict, List, Any, Optional, Tuple, Union

from .schema_manager import SchemaManager
from .sql_executor import SQLExecutor

# Configuration for database access
DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
BIRD_DATA_PATH = os.path.join(DATA_PATH, "bird")
BIRD_DB_DIRECTORY = os.path.join(BIRD_DATA_PATH, "dev_databases")
BIRD_TABLES_JSON = os.path.join(BIRD_DATA_PATH, "dev_tables.json")

SPIDER_DATA_PATH = os.path.join(DATA_PATH, "spider")
SPIDER_DB_DIRECTORY = os.path.join(SPIDER_DATA_PATH, "database")
SPIDER_TABLES_JSON = os.path.join(SPIDER_DATA_PATH, "tables.json")

# Initialize schema managers for different datasets
bird_schema_manager = SchemaManager(
    data_path=BIRD_DATA_PATH,
    tables_json_path=BIRD_TABLES_JSON,
    dataset_name="bird",
    lazy=True
)

# Initialize Spider schema manager if it exists
spider_schema_manager = None
if os.path.exists(SPIDER_DATA_PATH) and os.path.exists(SPIDER_TABLES_JSON):
    spider_schema_manager = SchemaManager(
        data_path=SPIDER_DATA_PATH,
        tables_json_path=SPIDER_TABLES_JSON,
        dataset_name="spider",
        lazy=True
    )

# Initialize SQL executors
# For Bird dataset, databases are in BIRD_DB_DIRECTORY, not directly in BIRD_DATA_PATH
bird_sql_executor = SQLExecutor(BIRD_DB_DIRECTORY, "bird")
spider_sql_executor = SQLExecutor(SPIDER_DB_DIRECTORY, "spider") if os.path.exists(SPIDER_DATA_PATH) else None

# Default schema manager and executor (using Bird)
default_schema_manager = bird_schema_manager
default_sql_executor = bird_sql_executor

# Tool for QueryUnderstandingAgent
def read_database_schema_and_records(
    db_id: str,
    dataset_name: str = "bird", 
    table_names: Union[List[str], None] = None,
    include_sample_data: bool = False
) -> Dict[str, Any]:
    """
    Reads database schema and optionally sample records for specified tables.
    
    Args:
        db_id: Database identifier
        dataset_name: Dataset name ('bird' or 'spider')
        table_names: List of tables to include (None for all tables)
        include_sample_data: Whether to include sample data in the output
        
    Returns:
        Dictionary with schema information
    """
    # Select the appropriate schema manager
    schema_manager = default_schema_manager
    if dataset_name.lower() == "spider" and spider_schema_manager:
        schema_manager = spider_schema_manager

    # Check if the database ID exists in the schema manager
    if db_id not in schema_manager.db2dbjsons:
        print(f"Database ID '{db_id}' not found in schema_manager.db2dbjsons")
        # Available database IDs
        available_dbs = list(schema_manager.db2dbjsons.keys())
        print(f"Available database IDs: {available_dbs[:5]}...")
        return {"error": f"Database ID '{db_id}' not found. Available databases: {available_dbs[:5]}..."}
        
    # Generate schema description using the schema manager
    # The method returns (schema_xml, fk_infos, chosen_schema)
    print(f"Generating schema description for database '{db_id}'...")
    _, fk_infos, tables_info = schema_manager.generate_schema_description(
        db_id=db_id,
        selected_schema={},  # Empty means keep all tables/columns
        use_gold_schema=False
    )
    
    # Filter tables if requested
    if table_names is not None:
        tables_info = {table: columns for table, columns in tables_info.items() 
                      if table in table_names}
    
    # Format the output schema
    output_schema = {}
    for table, columns in tables_info.items():
        # Get column types from database description
        column_types = {}
        
        # Load the database info if needed
        if db_id not in schema_manager.db2infos:
            schema_manager.db2infos[db_id] = schema_manager._load_single_db_info(db_id)
        
        # Extract column descriptions and types
        table_descriptions = schema_manager.db2infos[db_id]["desc_dict"].get(table, [])
        for col_name, full_col_name, _ in table_descriptions:
            column_types[col_name] = full_col_name
        
        # Get foreign keys
        table_fk_info = schema_manager.db2infos[db_id]["fk_dict"].get(table, [])
        foreign_keys = [(from_col, to_table, to_col) for from_col, to_table, to_col in table_fk_info]
        
        # Build output for this table
        output_schema[table] = {
            "columns": {col: column_types.get(col, "UNKNOWN") for col in columns},
            "description": f"Table storing {table} information.",
            "foreign_keys": foreign_keys
        }
        
        # Add sample data if requested
        if include_sample_data:
            try:
                # Get the correct database path
                db_path = get_db_path(db_id, dataset_name)
                
                # Connect directly to the database
                conn = sqlite3.connect(db_path)
                conn.text_factory = lambda b: b.decode(errors="ignore")
                cursor = conn.cursor()
                
                # Execute a query to get sample data
                sample_query = f"SELECT * FROM {table} LIMIT 5"
                cursor.execute(sample_query)
                
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Create a result object similar to SQLExecutor.safe_execute
                result = {
                    "success": True,
                    "data": rows,
                    "column_names": column_names,
                    "row_count": len(rows)
                }
                
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"Error getting sample data: {str(e)}")
                result = {"success": False, "sqlite_error": str(e)}
            
            if result.get("success", False):
                column_names = result.get("column_names", [])
                rows = result.get("data", [])
                
                # Convert to list of dictionaries
                sample_data = []
                for row in rows:
                    sample_data.append(dict(zip(column_names, row)))
                
                output_schema[table]["sample_data"] = sample_data
    
    if not output_schema:
        return {"error": "No schema found for the specified tables or database."}
    
    return output_schema

# Helper function to get the correct database path
def get_db_path(db_id: str, dataset_name: str = "bird") -> str:
    """
    Get the correct database path for the given database ID and dataset.
    
    Args:
        db_id: Database identifier
        dataset_name: Dataset name ('bird' or 'spider')
        
    Returns:
        Database file path
    """
    # Debug: Print the current working directory and other paths
    print(f"Current directory: {os.getcwd()}")
    print(f"BIRD_DATA_PATH: {BIRD_DATA_PATH}")
    print(f"BIRD_DB_DIRECTORY: {BIRD_DB_DIRECTORY}")
    
    if dataset_name.lower() == "bird":
        # The actual path should be:
        # BIRD_DB_DIRECTORY/db_id/db_id.sqlite
        db_path = os.path.join(BIRD_DB_DIRECTORY, db_id, f"{db_id}.sqlite")
        print(f"Checking database path: {db_path}, exists: {os.path.exists(db_path)}")
        return db_path
    elif dataset_name.lower() == "spider":
        db_path = os.path.join(SPIDER_DB_DIRECTORY, db_id, f"{db_id}.sqlite")
        print(f"Checking database path: {db_path}, exists: {os.path.exists(db_path)}")
        return db_path
    else:
        db_path = os.path.join(DATA_PATH, db_id, f"{db_id}.sqlite")
        print(f"Checking database path: {db_path}, exists: {os.path.exists(db_path)}")
        return db_path

# Tool for SQLGenerationAgent
def execute_sql_and_return_output(
    sql_query: str, 
    db_id: str, 
    dataset_name: str = "bird"
) -> Dict[str, Any]:
    """
    Executes a SQL query against the specified database and returns its output or an error.
    
    Args:
        sql_query: The SQL query to execute
        db_id: Database identifier
        dataset_name: Dataset name ('bird' or 'spider')
        
    Returns:
        Dictionary with execution results or error information
    """
    print(f"\n🤖 Executing SQL: {sql_query}")
    
    try:
        # Get the correct database path
        db_path = get_db_path(db_id, dataset_name)
        print(f" Using database path: {db_path}")
        
        # Connect directly to the database
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            row_count = len(rows)
            
            # Format the result for compatibility with the agent
            formatted_result = []
            for row in rows:
                formatted_result.append(dict(zip(column_names, row)))
            
            print(f" Query Result: {formatted_result[:5]}{'...' if row_count > 5 else ''}")
            print(f" Total Rows: {row_count}")
            
            # Close the connection
            cursor.close()
            conn.close()
            
            return {
                "result": formatted_result,
                "row_count": row_count,
                "column_names": column_names,
                "success": True
            }
            
        except sqlite3.Error as er:
            # Handle SQLite errors
            cursor.close()
            conn.close()
            
            error_message = str(' '.join(er.args))
            print(f" Query Execution Error: {error_message}")
            
            return {
                "error": error_message,
                "query_executed": sql_query,
                "success": False,
                "timeout": False
            }
            
        except Exception as e:
            # Handle other errors
            cursor.close()
            conn.close()
            
            error_message = str(e)
            print(f" Unexpected Error: {error_message}")
            
            return {
                "error": error_message,
                "query_executed": sql_query,
                "success": False,
                "timeout": False
            }
            
    except Exception as e:
        print(f" Error accessing database: {str(e)}")
        return {
            "error": str(e),
            "query_executed": sql_query,
            "success": False
        }