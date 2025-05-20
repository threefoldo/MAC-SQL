# -*- coding: utf-8 -*-
"""
SQL Executor Agent - An AI agent that executes and analyzes SQL queries using SQLExecutor.
"""

import asyncio
import json
import time
from typing import List, Dict, Any
from dotenv import load_dotenv

from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient

from sql_executor import SQLExecutor


class SQLExecutorAgent:
    """Agent that executes and analyzes SQL queries using SQLExecutor."""
    
    def __init__(self, sql_executor: SQLExecutor, model: str = "gpt-4o"):
        self.sql_executor = sql_executor
        self.model_client = OpenAIChatCompletionClient(model=model)
        self.agent = self._create_agent()
    
    def _create_agent(self) -> AssistantAgent:
        """Create the SQL execution agent with tools."""
        
        # Define SQL execution and analysis tools
        async def execute_query(sql: str, db_id: str) -> str:
            """Execute a SQL query against a database."""
            result = self.sql_executor.safe_execute(sql, db_id)
            
            if result['success']:
                # Format successful result
                response = {
                    "status": "success",
                    "sql": result['sql'],
                    "column_names": result['column_names'],
                    "data": result['data'],
                    "row_count": result['row_count'],
                    "showing_rows": len(result['data'])
                }
            else:
                # Format error result
                response = {
                    "status": "error",
                    "sql": result['sql'],
                    "error": result['sqlite_error'],
                    "error_type": result['exception_class']
                }
                if result.get('timeout'):
                    response['timeout'] = True
            
            return json.dumps(response, indent=2)
        
        async def validate_query_result(sql: str, db_id: str) -> str:
            """Execute a query and validate its results."""
            result = self.sql_executor.safe_execute(sql, db_id)
            
            response = {
                "sql": result['sql'],
                "execution_success": result['success']
            }
            
            if result['success']:
                is_valid, reason = self.sql_executor.is_valid_result(result)
                response.update({
                    "validation_result": "valid" if is_valid else "invalid",
                    "validation_reason": reason,
                    "row_count": result['row_count'],
                    "has_null_values": "No" if is_valid and "NULL" not in reason else "Yes"
                })
            else:
                response.update({
                    "validation_result": "execution_failed",
                    "error": result['sqlite_error']
                })
            
            return json.dumps(response, indent=2)
        
        async def analyze_table_statistics(table_name: str, db_id: str) -> str:
            """Analyze statistics for a specific table."""
            queries = {
                "row_count": f"SELECT COUNT(*) as count FROM {table_name}",
                "null_analysis": f"SELECT * FROM {table_name} WHERE 1=0",  # Get column names
            }
            
            stats = {"table": table_name}
            
            # Get row count
            count_result = self.sql_executor.safe_execute(queries["row_count"], db_id)
            if count_result['success']:
                stats["total_rows"] = count_result['data'][0][0]
            
            # Get column information
            col_result = self.sql_executor.safe_execute(queries["null_analysis"], db_id)
            if col_result['success']:
                columns = col_result['column_names']
                stats["column_count"] = len(columns)
                stats["columns"] = columns
                
                # Analyze NULL values for each column
                null_counts = {}
                for col in columns[:5]:  # Limit to first 5 columns for performance
                    null_query = f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
                    null_result = self.sql_executor.safe_execute(null_query, db_id)
                    if null_result['success']:
                        null_counts[col] = null_result['data'][0][0]
                
                stats["null_counts"] = null_counts
            
            return json.dumps(stats, indent=2)
        
        async def compare_query_performance(queries: list, db_id: str) -> str:
            """Compare performance of multiple queries."""
            results = []
            for sql in queries:
                start_time = time.time()
                result = self.sql_executor.safe_execute(sql, db_id)
                execution_time = time.time() - start_time
                
                perf_info = {
                    "query": sql[:100] + "..." if len(sql) > 100 else sql,
                    "success": result['success'],
                    "execution_time_seconds": round(execution_time, 3),
                    "row_count": result.get('row_count', 0) if result['success'] else None,
                    "error": result.get('sqlite_error') if not result['success'] else None
                }
                results.append(perf_info)
            
            # Sort by execution time
            results.sort(key=lambda x: x['execution_time_seconds'])
            
            return json.dumps({
                "performance_comparison": results,
                "fastest_query": results[0]['query'] if results else None,
                "slowest_query": results[-1]['query'] if results else None
            }, indent=2)
        
        async def explain_query_plan(sql: str, db_id: str) -> str:
            """Get the query execution plan for a SQL query."""
            # First try to get the query plan
            explain_sql = f"EXPLAIN QUERY PLAN {sql}"
            result = self.sql_executor.safe_execute(explain_sql, db_id)
            
            if result['success']:
                plan_info = {
                    "sql": sql,
                    "query_plan": result['data'],
                    "analysis": "The query plan shows how SQLite will execute this query"
                }
            else:
                # If EXPLAIN fails, just execute the query normally
                normal_result = self.sql_executor.safe_execute(sql, db_id)
                if normal_result['success']:
                    plan_info = {
                        "sql": sql,
                        "note": "Could not generate query plan, but query executes successfully",
                        "row_count": normal_result['row_count']
                    }
                else:
                    plan_info = {
                        "sql": sql,
                        "error": normal_result['sqlite_error']
                    }
            
            return json.dumps(plan_info, indent=2)
        
        async def suggest_indexes(table_name: str, db_id: str) -> str:
            """Suggest potential indexes for a table based on its structure."""
            # Get table structure
            pragma_query = f"PRAGMA table_info({table_name})"
            result = self.sql_executor.safe_execute(pragma_query, db_id)
            
            suggestions = []
            if result['success']:
                columns = result['data']
                for col in columns:
                    col_name = col[1]
                    col_type = col[2]
                    
                    # Suggest indexes for common patterns
                    if col_name.lower().endswith('_id'):
                        suggestions.append({
                            "column": col_name,
                            "reason": "Foreign key column - frequently used in JOINs",
                            "index_sql": f"CREATE INDEX idx_{table_name}_{col_name} ON {table_name}({col_name});"
                        })
                    elif col_name.lower() in ['created_at', 'updated_at', 'date']:
                        suggestions.append({
                            "column": col_name,
                            "reason": "Date column - often used in WHERE clauses for filtering",
                            "index_sql": f"CREATE INDEX idx_{table_name}_{col_name} ON {table_name}({col_name});"
                        })
                    elif col_type in ['INTEGER', 'REAL'] and not col_name.lower().endswith('id'):
                        suggestions.append({
                            "column": col_name,
                            "reason": "Numeric column - potentially used in range queries",
                            "index_sql": f"CREATE INDEX idx_{table_name}_{col_name} ON {table_name}({col_name});"
                        })
            
            return json.dumps({
                "table": table_name,
                "index_suggestions": suggestions[:3]  # Limit to top 3 suggestions
            }, indent=2)
        
        # Create the agent
        return AssistantAgent(
            name="sql_executor_agent",
            model_client=self.model_client,
            tools=[
                execute_query,
                validate_query_result,
                analyze_table_statistics,
                compare_query_performance,
                explain_query_plan,
                suggest_indexes
            ],
            system_message="""You are an expert SQL assistant that helps users:
            - Execute and debug SQL queries
            - Analyze query results and data quality
            - Measure and compare query performance
            - Suggest database optimizations and indexes
            - Explain query execution plans
            
            Always provide clear explanations and actionable recommendations.
            When queries fail, help diagnose the issue and suggest fixes.""",
            reflect_on_tool_use=True,
            model_client_stream=True,
        )
    
    async def query(self, task: str) -> None:
        """Run a query against the SQL executor agent."""
        await Console(self.agent.run_stream(task=task))
    
    async def close(self) -> None:
        """Close the model client connection."""
        await self.model_client.close()


async def main():
    """Example usage of the SQLExecutorAgent."""
    load_dotenv()
    
    # Initialize SQLExecutor
    data_path = "../data/bird/dev_databases"
    dataset_name = "bird"
    
    sql_executor = SQLExecutor(
        data_path=data_path,
        dataset_name=dataset_name
    )
    
    # Create SQLExecutorAgent
    sql_agent = SQLExecutorAgent(sql_executor)
    
    # Example queries
    tasks = [
        "Execute the query 'SELECT * FROM schools LIMIT 3' on the california_schools database",
        "Analyze the statistics for the 'schools' table in california_schools database",
        "Suggest indexes for the 'satscores' table in california_schools database",
        "Compare the performance of these queries on california_schools database: "
        "['SELECT COUNT(*) FROM schools', 'SELECT * FROM schools WHERE County = \"Los Angeles\"']"
    ]
    
    try:
        for task in tasks:
            print(f"\n{'='*50}")
            print(f"Task: {task}")
            print(f"{'='*50}\n")
            await sql_agent.query(task)
    finally:
        await sql_agent.close()


if __name__ == "__main__":
    asyncio.run(main())