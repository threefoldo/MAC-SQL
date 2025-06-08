"""
Test cases for SQLGeneratorTools - Tool functionality verification.

Tests the individual tools that SQLGeneratorAgent uses to interact with schema
and validate SQL queries.
"""

import asyncio
import pytest
import sys
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from keyvalue_memory import KeyValueMemory
from memory_content_types import TableSchema, ColumnInfo
from database_schema_manager import DatabaseSchemaManager
from sql_generator_tools import SQLGeneratorTools, create_sql_generator_tools


class TestSQLGeneratorTools:
    """Test cases for SQLGeneratorTools functionality"""
    
    async def setup_test_schema(self, memory: KeyValueMemory):
        """Setup test schema with sample tables"""
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        
        # Create users table
        users_table = TableSchema(
            name="users",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "name": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "email": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "age": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(users_table)
        
        # Create orders table with foreign key
        orders_table = TableSchema(
            name="orders",
            columns={
                "order_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "user_id": ColumnInfo(dataType="INTEGER", nullable=False, isPrimaryKey=False, isForeignKey=True,
                                    references={"table": "users", "column": "id"}),
                "amount": ColumnInfo(dataType="REAL", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "created_at": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(orders_table)
        
        return schema_manager
    
    @pytest.mark.asyncio
    async def test_check_table_columns_exists(self):
        """Test checking columns for existing table"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # Check existing table
        result = await tools.check_table_columns("users")
        
        assert result["exists"] is True
        assert result["exact_name"] == "users"
        assert len(result["columns"]) == 4
        assert result["column_count"] == 4
        
        # Check column details
        column_names = [col["name"] for col in result["columns"]]
        assert "id" in column_names
        assert "name" in column_names
        assert "email" in column_names
        assert "age" in column_names
        
        # Check primary key
        assert "id" in result["primary_keys"]
        assert len(result["primary_keys"]) == 1
        
        print("✓ Table column check for existing table works")
    
    @pytest.mark.asyncio
    async def test_check_table_columns_not_exists(self):
        """Test checking columns for non-existing table"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # Check non-existing table
        result = await tools.check_table_columns("nonexistent")
        
        assert result["exists"] is False
        assert "not found" in result["error"].lower()
        assert "similar_tables" in result
        assert len(result["columns"]) == 0
        
        print("✓ Table column check for non-existing table works")
    
    @pytest.mark.asyncio
    async def test_check_table_columns_case_insensitive(self):
        """Test that table checking is case insensitive"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # Check with different case
        result = await tools.check_table_columns("USERS")
        
        assert result["exists"] is True
        assert result["exact_name"] == "users"  # Should return actual case
        assert len(result["columns"]) == 4
        
        print("✓ Case insensitive table checking works")
    
    @pytest.mark.asyncio
    async def test_check_column_exists(self):
        """Test checking if specific column exists"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # Check existing column
        result = await tools.check_column_exists("users", "name")
        
        assert result["table_exists"] is True
        assert result["exists"] is True
        assert result["exact_table_name"] == "users"
        assert result["exact_column_name"] == "name"
        assert result["column_info"]["type"] == "TEXT"
        
        print("✓ Column existence check for existing column works")
    
    @pytest.mark.asyncio
    async def test_check_column_not_exists(self):
        """Test checking non-existing column"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # Check non-existing column
        result = await tools.check_column_exists("users", "nonexistent_col")
        
        assert result["table_exists"] is True
        assert result["exists"] is False
        assert "not found" in result["error"].lower()
        assert "available_columns" in result
        assert len(result["available_columns"]) == 4
        
        print("✓ Column existence check for non-existing column works")
    
    @pytest.mark.asyncio
    async def test_check_column_case_insensitive(self):
        """Test case insensitive column checking"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # Check with different case
        result = await tools.check_column_exists("users", "NAME")
        
        assert result["exists"] is True
        assert result["exact_column_name"] == "name"  # Should return actual case
        
        print("✓ Case insensitive column checking works")
    
    @pytest.mark.asyncio
    async def test_list_all_tables(self):
        """Test listing all tables"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # List all tables
        result = await tools.list_all_tables()
        
        assert "tables" in result
        assert result["count"] == 2
        assert len(result["tables"]) == 2
        
        table_names = [table["name"] for table in result["tables"]]
        assert "users" in table_names
        assert "orders" in table_names
        
        # Check table info
        users_info = next(t for t in result["tables"] if t["name"] == "users")
        assert users_info["column_count"] == 4
        
        orders_info = next(t for t in result["tables"] if t["name"] == "orders")
        assert orders_info["column_count"] == 4
        
        print("✓ List all tables works")
    
    @pytest.mark.asyncio
    async def test_validate_sql_basic(self):
        """Test basic SQL validation"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # Test valid SQL
        valid_sql = "SELECT name FROM users WHERE age > 18"
        result = await tools.validate_sql(valid_sql)
        
        # Should complete without errors (actual validation depends on SQL validator implementation)
        assert "is_valid" in result
        assert "errors" in result
        assert "warnings" in result
        
        print("✓ SQL validation works")
    
    @pytest.mark.asyncio
    async def test_foreign_key_detection(self):
        """Test detection of foreign keys"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        tools = SQLGeneratorTools(memory)
        
        # Check orders table which has foreign key
        result = await tools.check_table_columns("orders")
        
        assert result["exists"] is True
        assert "user_id" in result["foreign_keys"]
        
        # Check user_id column specifically
        col_result = await tools.check_column_exists("orders", "user_id")
        assert col_result["exists"] is True
        assert col_result["column_info"]["is_foreign"] is True
        
        print("✓ Foreign key detection works")
    
    @pytest.mark.asyncio
    async def test_create_sql_generator_tools_factory(self):
        """Test the factory function for creating tools"""
        memory = KeyValueMemory()
        await self.setup_test_schema(memory)
        
        # Test factory function
        tool_configs = create_sql_generator_tools(memory)
        
        assert len(tool_configs) == 4
        
        # Check tool names
        tool_names = [config["name"] for config in tool_configs]
        expected_tools = ["check_table_columns", "check_column_exists", "validate_sql", "list_all_tables"]
        
        for expected_tool in expected_tools:
            assert expected_tool in tool_names
        
        # Check that each tool has required fields
        for config in tool_configs:
            assert "function" in config
            assert "name" in config
            assert "description" in config
            assert callable(config["function"])
        
        print("✓ Tool factory function works")
    
    @pytest.mark.asyncio
    async def test_error_handling_no_schema(self):
        """Test error handling when no schema is available"""
        memory = KeyValueMemory()
        # Don't setup schema
        
        tools = SQLGeneratorTools(memory)
        
        # Test table check with no schema
        result = await tools.check_table_columns("users")
        
        assert result["exists"] is False
        assert "no schema" in result["error"].lower()
        
        # Test list tables with no schema
        list_result = await tools.list_all_tables()
        assert list_result["count"] == 0
        assert "no schema" in list_result["error"].lower()
        
        print("✓ Error handling for missing schema works")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("SQLGeneratorTools Tests")
    print("="*60)
    
    # Run tests
    asyncio.run(pytest.main([__file__, "-v", "-s"]))