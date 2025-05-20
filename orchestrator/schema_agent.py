# -*- coding: utf-8 -*-
"""
Schema Agent - An AI agent that provides database schema information using SchemaManager.
"""

import asyncio
import json
from typing import List, Dict, Any
from dotenv import load_dotenv

from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient

from schema_manager import SchemaManager


class SchemaAgent:
    """Agent that provides database schema information using SchemaManager."""
    
    def __init__(self, schema_manager: SchemaManager, model: str = "gpt-4o"):
        self.schema_manager = schema_manager
        self.model_client = OpenAIChatCompletionClient(model=model)
        self.agent = self._create_agent()
    
    def _create_agent(self) -> AssistantAgent:
        """Create the schema agent with tools."""
        
        # Define schema-related tools
        async def list_databases() -> str:
            """List all available databases in the dataset."""
            db_ids = list(self.schema_manager.db2dbjsons.keys())
            return f"Available databases: {json.dumps(db_ids[:10])}..." if len(db_ids) > 10 else f"Available databases: {json.dumps(db_ids)}"
        
        async def get_database_info(db_id: str) -> str:
            """Get basic information about a specific database."""
            if db_id not in self.schema_manager.db2dbjsons:
                return f"Database '{db_id}' not found."
            
            db_info = self.schema_manager.db2dbjsons[db_id]
            summary = {
                "db_id": db_id,
                "table_count": db_info['table_count'],
                "total_columns": db_info['total_column_count'],
                "avg_columns_per_table": db_info['avg_column_count'],
                "table_names": db_info['table_names_original']
            }
            return json.dumps(summary, indent=2)
        
        async def get_table_schema(db_id: str, table_name: str) -> str:
            """Get detailed schema information for a specific table."""
            if db_id not in self.schema_manager.db2dbjsons:
                return f"Database '{db_id}' not found."
            
            # Load database info if not already loaded
            if db_id not in self.schema_manager.db2infos:
                self.schema_manager.db2infos[db_id] = self.schema_manager._load_single_db_info(db_id)
            
            db_info = self.schema_manager.db2infos[db_id]
            
            if table_name not in db_info['desc_dict']:
                return f"Table '{table_name}' not found in database '{db_id}'."
            
            # Get table information
            columns_desc = db_info['desc_dict'][table_name]
            columns_val = db_info['value_dict'][table_name]
            primary_keys = db_info['pk_dict'][table_name]
            foreign_keys = db_info['fk_dict'][table_name]
            
            result = {
                "table": table_name,
                "columns": [],
                "primary_keys": primary_keys,
                "foreign_keys": []
            }
            
            # Add column information
            for (col_name, full_col_name, _), (_, values_str) in zip(columns_desc, columns_val):
                col_info = {
                    "name": col_name,
                    "description": full_col_name,
                    "sample_values": values_str
                }
                result["columns"].append(col_info)
            
            # Add foreign key information
            for from_col, to_table, to_col in foreign_keys:
                fk_info = {
                    "from_column": from_col,
                    "to_table": to_table,
                    "to_column": to_col
                }
                result["foreign_keys"].append(fk_info)
            
            return json.dumps(result, indent=2)
        
        async def generate_schema_xml(db_id: str, table_names: List[str] = None) -> str:
            """Generate XML schema description for a database or specific tables."""
            if db_id not in self.schema_manager.db2dbjsons:
                return f"Database '{db_id}' not found."
            
            # Create selected schema dict
            if table_names:
                selected_schema = {table: "keep_all" for table in table_names}
            else:
                # Include all tables
                db_info = self.schema_manager.db2dbjsons[db_id]
                selected_schema = {table: "keep_all" for table in db_info['table_names_original']}
            
            # Generate schema XML
            schema_xml, fk_infos, chosen_schema = self.schema_manager.generate_schema_description(
                db_id, selected_schema, use_gold_schema=False
            )
            
            return schema_xml
        
        async def analyze_database_complexity(db_id: str) -> str:
            """Analyze the complexity of a database schema."""
            if db_id not in self.schema_manager.db2dbjsons:
                return f"Database '{db_id}' not found."
            
            db_info = self.schema_manager.db2dbjsons[db_id]
            
            # Determine complexity
            is_complex = self.schema_manager._is_complex_schema(db_id)
            
            analysis = {
                "db_id": db_id,
                "is_complex": is_complex,
                "metrics": {
                    "table_count": db_info['table_count'],
                    "avg_columns_per_table": db_info['avg_column_count'],
                    "max_columns_in_table": db_info['max_column_count'],
                    "total_columns": db_info['total_column_count']
                },
                "complexity_reasoning": "Complex" if is_complex else "Simple",
                "recommendation": "Schema pruning recommended" if is_complex else "No pruning needed"
            }
            
            return json.dumps(analysis, indent=2)
        
        async def find_tables_by_keyword(keyword: str) -> str:
            """Find tables that contain a specific keyword in their name or columns."""
            results = []
            
            for db_id, db_info in self.schema_manager.db2dbjsons.items():
                matching_tables = []
                
                # Check table names
                for table_name in db_info['table_names_original']:
                    if keyword.lower() in table_name.lower():
                        matching_tables.append({
                            "table": table_name,
                            "match_type": "table_name"
                        })
                
                # Check column names
                for idx, (tb_idx, col_name) in enumerate(db_info['column_names_original']):
                    if keyword.lower() in col_name.lower():
                        if tb_idx >= 0:
                            table_name = db_info['table_names_original'][tb_idx]
                            matching_tables.append({
                                "table": table_name,
                                "column": col_name,
                                "match_type": "column_name"
                            })
                
                if matching_tables:
                    results.append({
                        "database": db_id,
                        "matches": matching_tables
                    })
            
            if not results:
                return f"No tables or columns found containing '{keyword}'"
            
            return json.dumps(results[:5], indent=2)  # Limit to 5 databases
        
        # Create the agent
        return AssistantAgent(
            name="schema_agent",
            model_client=self.model_client,
            tools=[
                list_databases,
                get_database_info,
                get_table_schema,
                generate_schema_xml,
                analyze_database_complexity,
                find_tables_by_keyword
            ],
            system_message="""You are an advanced database schema analysis expert. 
            You can:
            - List and describe available databases
            - Analyze database complexity and provide recommendations
            - Find tables and columns by keywords
            - Identify relationships between tables
            - Generate detailed schema documentation
            - Provide insights about database design patterns
            
            Use the available tools to provide comprehensive schema analysis.""",
            reflect_on_tool_use=True,
            model_client_stream=True,
        )
    
    async def query(self, task: str) -> None:
        """Run a query against the schema agent."""
        await Console(self.agent.run_stream(task=task))
    
    async def close(self) -> None:
        """Close the model client connection."""
        await self.model_client.close()


async def main():
    """Example usage of the SchemaAgent."""
    load_dotenv()
    
    # Initialize SchemaManager
    data_path = "../data/bird"
    tables_json_path = f"{data_path}/dev_tables.json"
    dataset_name = "bird"
    
    schema_manager = SchemaManager(
        data_path=data_path,
        tables_json_path=tables_json_path,
        dataset_name=dataset_name,
        lazy=True
    )
    
    # Create SchemaAgent
    schema_agent = SchemaAgent(schema_manager)
    
    # Example queries
    queries = [
        "What databases are available?",
        "Tell me about the 'california_schools' database",
        "Show me the schema for the 'schools' table in the 'california_schools' database",
        "Analyze the complexity of the 'california_schools' database",
        "Find all tables that contain the word 'student'"
    ]
    
    try:
        for query in queries:
            print(f"\n{'='*50}")
            print(f"Query: {query}")
            print(f"{'='*50}\n")
            await schema_agent.query(query)
    finally:
        await schema_agent.close()


if __name__ == "__main__":
    asyncio.run(main())