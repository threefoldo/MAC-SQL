"""
Database schema manager for text-to-SQL tree orchestration.

This module provides easy access to database schema data stored in KeyValueMemory.
"""

import logging
from typing import Dict, List, Optional, Any

from keyvalue_memory import KeyValueMemory
from memory_content_types import TableSchema, ColumnInfo
from schema_reader import SchemaReader


class DatabaseSchemaManager:
    """Manages database schema data in memory."""
    
    def __init__(self, memory: KeyValueMemory):
        """
        Initialize the database schema manager.
        
        Args:
            memory: The KeyValueMemory instance to use for storage
        """
        self.memory = memory
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def initialize(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Initialize an empty database schema.
        
        Args:
            metadata: Optional metadata including data_path and dataset_name
        """
        schema = {
            "tables": {},
            "metadata": metadata or {}
        }
        await self.memory.set("databaseSchema", schema)
        self.logger.info("Initialized empty database schema")
    
    async def add_table(self, table_schema: TableSchema) -> None:
        """
        Add a table to the database schema.
        
        Args:
            table_schema: The table schema to add
        """
        schema = await self.memory.get("databaseSchema")
        if not schema:
            schema = {"tables": {}}
        
        schema["tables"][table_schema.name] = table_schema.to_dict()
        await self.memory.set("databaseSchema", schema)
        self.logger.info(f"Added table '{table_schema.name}' to schema")
    
    async def get_table(self, table_name: str) -> Optional[TableSchema]:
        """
        Get schema for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema if found, None otherwise
        """
        schema = await self.memory.get("databaseSchema")
        if schema and "tables" in schema and table_name in schema["tables"]:
            return TableSchema.from_dict(table_name, schema["tables"][table_name])
        return None
    
    async def get_all_tables(self) -> Dict[str, TableSchema]:
        """
        Get all tables in the schema.
        
        Returns:
            Dictionary mapping table names to TableSchema objects
        """
        schema = await self.memory.get("databaseSchema")
        if not schema or "tables" not in schema:
            return {}
        
        return {
            name: TableSchema.from_dict(name, data)
            for name, data in schema["tables"].items()
        }
    
    async def get_table_names(self) -> List[str]:
        """
        Get all table names in the schema.
        
        Returns:
            List of table names
        """
        tables = await self.get_all_tables()
        return list(tables.keys())
    
    async def get_column(self, table_name: str, column_name: str) -> Optional[ColumnInfo]:
        """
        Get information for a specific column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            ColumnInfo if found, None otherwise
        """
        table = await self.get_table(table_name)
        if table and column_name in table.columns:
            return table.columns[column_name]
        return None
    
    async def get_columns(self, table_name: str) -> Dict[str, ColumnInfo]:
        """
        Get all columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary mapping column names to ColumnInfo objects
        """
        table = await self.get_table(table_name)
        if table:
            return table.columns
        return {}
    
    async def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        columns = await self.get_columns(table_name)
        return [name for name, col in columns.items() if col.isPrimaryKey]
    
    async def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of foreign key relationships
        """
        columns = await self.get_columns(table_name)
        foreign_keys = []
        
        for name, col in columns.items():
            if col.isForeignKey and col.references:
                foreign_keys.append({
                    'column': name,
                    'references_table': col.references['table'],
                    'references_column': col.references['column']
                })
        
        return foreign_keys
    
    async def find_relationships(self, table1: str, table2: str) -> List[Dict[str, Any]]:
        """
        Find relationships between two tables.
        
        Args:
            table1: First table name
            table2: Second table name
            
        Returns:
            List of relationships (foreign key connections)
        """
        relationships = []
        
        # Check table1 -> table2
        fks1 = await self.get_foreign_keys(table1)
        for fk in fks1:
            if fk['references_table'] == table2:
                relationships.append({
                    'from_table': table1,
                    'from_column': fk['column'],
                    'to_table': table2,
                    'to_column': fk['references_column'],
                    'type': 'foreign_key'
                })
        
        # Check table2 -> table1
        fks2 = await self.get_foreign_keys(table2)
        for fk in fks2:
            if fk['references_table'] == table1:
                relationships.append({
                    'from_table': table2,
                    'from_column': fk['column'],
                    'to_table': table1,
                    'to_column': fk['references_column'],
                    'type': 'foreign_key'
                })
        
        return relationships
    
    async def get_sample_data(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get sample data for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of sample rows if available
        """
        table = await self.get_table(table_name)
        if table:
            return table.sampleData
        return None
    
    async def set_sample_data(self, table_name: str, sample_data: List[Dict[str, Any]]) -> None:
        """
        Set sample data for a table.
        
        Args:
            table_name: Name of the table
            sample_data: List of sample rows
        """
        schema = await self.memory.get("databaseSchema")
        if schema and "tables" in schema and table_name in schema["tables"]:
            schema["tables"][table_name]["sampleData"] = sample_data
            await self.memory.set("databaseSchema", schema)
            self.logger.info(f"Set sample data for table '{table_name}'")
    
    async def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Metadata dictionary if available
        """
        table = await self.get_table(table_name)
        if table:
            return table.metadata
        return None
    
    async def set_table_metadata(self, table_name: str, metadata: Dict[str, Any]) -> None:
        """
        Set metadata for a table.
        
        Args:
            table_name: Name of the table
            metadata: Metadata dictionary
        """
        schema = await self.memory.get("databaseSchema")
        if schema and "tables" in schema and table_name in schema["tables"]:
            schema["tables"][table_name]["metadata"] = metadata
            await self.memory.set("databaseSchema", schema)
            self.logger.info(f"Set metadata for table '{table_name}'")
    
    async def search_columns_by_type(self, data_type: str) -> List[Dict[str, str]]:
        """
        Search for columns by data type across all tables.
        
        Args:
            data_type: The data type to search for
            
        Returns:
            List of dictionaries with table and column information
        """
        results = []
        tables = await self.get_all_tables()
        
        for table_name, table in tables.items():
            for col_name, col_info in table.columns.items():
                if col_info.dataType.lower() == data_type.lower():
                    results.append({
                        'table': table_name,
                        'column': col_name,
                        'dataType': col_info.dataType
                    })
        
        return results
    
    async def get_schema_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the database schema.
        
        Returns:
            Dictionary with schema statistics
        """
        tables = await self.get_all_tables()
        
        total_columns = 0
        total_primary_keys = 0
        total_foreign_keys = 0
        
        for table in tables.values():
            total_columns += len(table.columns)
            for col in table.columns.values():
                if col.isPrimaryKey:
                    total_primary_keys += 1
                if col.isForeignKey:
                    total_foreign_keys += 1
        
        return {
            'table_count': len(tables),
            'total_columns': total_columns,
            'total_primary_keys': total_primary_keys,
            'total_foreign_keys': total_foreign_keys,
            'average_columns_per_table': total_columns / len(tables) if tables else 0
        }
    
    async def load_from_schema_reader(self, schema_reader: SchemaReader, db_id: str) -> None:
        """
        Load database schema from SchemaReader and populate it into memory.
        
        Args:
            schema_reader: The SchemaReader instance with loaded database information
            db_id: The database ID to load
        """
        # Initialize schema with metadata
        metadata = {
            "data_path": schema_reader.data_path,
            "dataset_name": schema_reader.dataset_name,
            "database_id": db_id
        }
        await self.initialize(metadata)
        
        # Load database info if not already loaded
        if db_id not in schema_reader.db2infos:
            db_info = schema_reader._load_single_db_info(db_id)
            schema_reader.db2infos[db_id] = db_info
        else:
            db_info = schema_reader.db2infos[db_id]
        
        # Get the database JSON info for column types
        db_json = schema_reader.db2dbjsons.get(db_id, {})
        
        # Extract information from schema_reader
        desc_dict = db_info['desc_dict']    # table -> [(column_name, full_column_name, extra_desc)]
        value_dict = db_info['value_dict']  # table -> [(column_name, value_examples_str)]
        pk_dict = db_info['pk_dict']        # table -> [primary_key_column_names]
        fk_dict = db_info['fk_dict']        # table -> [(from_col, to_table, to_col)]
        
        # Get column types from the JSON data
        column_types = {}  # {(table_idx, col_name): type}
        if 'column_types' in db_json:
            for col_idx, col_type in enumerate(db_json['column_types']):
                if col_idx < len(db_json['column_names_original']):
                    table_idx, col_name = db_json['column_names_original'][col_idx]
                    if table_idx >= 0:  # -1 means special columns like *
                        column_types[(table_idx, col_name)] = col_type
        
        # Get table names mapping
        table_names = db_json.get('table_names_original', [])
        
        # Process each table
        for table_idx, table_name in enumerate(table_names):
            if table_name not in desc_dict:
                continue
                
            # Get column descriptions
            columns_desc = desc_dict[table_name]
            columns_val = value_dict.get(table_name, [])
            primary_keys = pk_dict.get(table_name, [])
            foreign_keys = fk_dict.get(table_name, [])
            
            # Build columns dictionary
            columns = {}
            
            # Create a mapping of column names to their typical values
            column_values_map = {}
            for col_name, values_str in columns_val:
                if values_str and values_str != '[]':
                    try:
                        import ast
                        values = ast.literal_eval(values_str)
                        if isinstance(values, list):
                            column_values_map[col_name] = values
                    except:
                        pass
            
            # Process each column
            for col_idx, (col_name, full_col_name, extra_desc) in enumerate(columns_desc):
                # Get column type
                col_type = column_types.get((table_idx, col_name), "TEXT")
                
                # Determine if it's a primary key
                is_primary_key = col_name in primary_keys
                
                # Determine if it's a foreign key and get references
                is_foreign_key = False
                references = None
                for fk_col, to_table, to_col in foreign_keys:
                    if fk_col == col_name:
                        is_foreign_key = True
                        references = {
                            "table": to_table,
                            "column": to_col
                        }
                        break
                
                # Get typical values for this column
                typical_values = column_values_map.get(col_name)
                
                # Create ColumnInfo
                column_info = ColumnInfo(
                    dataType=col_type,
                    nullable=True,  # Default to nullable, can be refined later
                    isPrimaryKey=is_primary_key,
                    isForeignKey=is_foreign_key,
                    references=references,
                    typicalValues=typical_values
                )
                
                columns[col_name] = column_info
            
            # Create TableSchema without sample data (we use typicalValues instead)
            table_schema = TableSchema(
                name=table_name,
                columns=columns,
                sampleData=None,  # We use typicalValues in columns instead
                metadata={
                    "description": f"Table {table_name} from {db_id} database",
                    "column_count": len(columns)
                }
            )
            
            # Add table to schema
            await self.add_table(table_schema)
        
        self.logger.info(f"Loaded schema for database '{db_id}' with {len(table_names)} tables")