"""
Tools for text-to-SQL workflow agents.

This module contains custom tool implementations for the agents in the text-to-SQL workflow.
These tools are used by the agents to interact with the database schema and execute SQL queries.
"""

import json
from typing import Dict, Any, List

# Function to configure the agent tools based on schema_manager and sql_executor
def configure_agent_tools(schema_manager, sql_executor):
    """
    Configure the agent tools based on schema_manager and sql_executor.
    
    Args:
        schema_manager: SchemaManager instance
        sql_executor: SQLExecutor instance
        
    Returns:
        Dictionary with configured tool functions
    """
    
    # Tool implementation for schema selection and management
    async def get_initial_database_schema(db_id: str) -> str:
        """
        Retrieves the full database schema information for a given database.
        
        Args:
            db_id: The database identifier
            
        Returns:
            JSON string with full schema information
        """
        print(f"[Tool] Loading schema for database: {db_id}")
        
        # Load database information using SchemaManager
        if db_id not in schema_manager.db2infos:
            schema_manager.db2infos[db_id] = schema_manager._load_single_db_info(db_id)
        
        # Get database information
        db_info = schema_manager.db2dbjsons.get(db_id, {})
        if not db_info:
            return json.dumps({"error": f"Database '{db_id}' not found"})
        
        # Determine if schema is complex enough to need pruning
        is_complex = schema_manager._is_complex_schema(db_id)
        
        # Generate full schema description (without pruning)
        full_schema_str, full_fk_str, _ = schema_manager.generate_schema_description(
            db_id, {}, use_gold_schema=False
        )
        
        # Return schema details
        return json.dumps({
            "db_id": db_id,
            "table_count": db_info.get('table_count', 0),
            "total_column_count": db_info.get('total_column_count', 0),
            "avg_column_count": db_info.get('avg_column_count', 0),
            "is_complex_schema": is_complex,
            "full_schema_str": full_schema_str,
            "full_fk_str": full_fk_str
        })

    async def prune_database_schema(db_id: str, pruning_rules: Dict) -> str:
        """
        Applies pruning rules to a database schema.
        
        Args:
            db_id: The database identifier
            pruning_rules: Dictionary with tables and columns to keep
            
        Returns:
            JSON string with pruned schema
        """
        print(f"[Tool] Pruning schema for database {db_id}")
        
        # Generate pruned schema description
        schema_str, fk_str, chosen_schema = schema_manager.generate_schema_description(
            db_id, pruning_rules, use_gold_schema=False
        )
        
        # Return pruned schema
        return json.dumps({
            "db_id": db_id,
            "pruning_applied": True,
            "pruning_rules": pruning_rules,
            "pruned_schema_str": schema_str,
            "pruned_fk_str": fk_str,
            "tables_columns_kept": chosen_schema
        })

    # Tool implementation for SQL execution
    async def execute_sql(sql: str, db_id: str) -> str:
        """
        Executes a SQL query on the specified database.
        
        Args:
            sql: The SQL query to execute
            db_id: The database identifier
            
        Returns:
            JSON string with execution results
        """
        print(f"[Tool] Executing SQL on database {db_id}: {sql[:100]}...")
        
        # Execute SQL with timeout protection
        result = sql_executor.safe_execute(sql, db_id)
        
        # Add validation information
        is_valid, reason = sql_executor.is_valid_result(result)
        result["is_valid_result"] = is_valid
        result["validation_message"] = reason
        
        # Convert to JSON string
        return json.dumps(result)
    
    # Return dictionary with configured tool functions
    return {
        "get_initial_database_schema": get_initial_database_schema,
        "prune_database_schema": prune_database_schema,
        "execute_sql": execute_sql
    }