"""
Test cases for DatabaseSchemaManager - TESTING_PLAN.md Layer 1.2.

Tests schema storage and lookup operations:
- Store database schema at correct memory location ('databaseSchema')
- Add tables with complete column information
- Store foreign key relationships correctly
- Cache sample data and metadata properly
- Retrieve schema elements by various queries
- Handle multiple databases simultaneously
"""

import asyncio
import pytest
import os
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from keyvalue_memory import KeyValueMemory
from database_schema_manager import DatabaseSchemaManager
from memory_content_types import TableSchema, ColumnInfo
from schema_reader import SchemaReader


class TestDatabaseSchemaManager:
    """Test database schema management - TESTING_PLAN.md Layer 1.2."""
    
    @pytest.fixture
    async def memory(self):
        """Create a fresh KeyValueMemory instance."""
        mem = KeyValueMemory()
        yield mem
        await mem.clear()
    
    @pytest.fixture
    async def manager(self, memory):
        """Create a DatabaseSchemaManager instance."""
        mgr = DatabaseSchemaManager(memory)
        await mgr.initialize()
        return mgr
    
    @pytest.mark.asyncio
    async def test_memory_location_and_initialization(self, memory, manager):
        """Test schema storage at correct memory location."""
        # CRITICAL: Verify storage at correct memory location
        raw_data = await memory.get("databaseSchema")  # Must be exact key
        assert raw_data is not None
        assert "tables" in raw_data
        assert "metadata" in raw_data
        
        # Should start empty
        tables = await manager.get_all_tables()
        assert len(tables) == 0
        
        print("✅ Schema memory location and initialization tests passed")
    
    @pytest.mark.asyncio
    async def test_table_operations_with_complete_column_info(self, memory, manager):
        """Test adding tables with complete column information."""
        # Create table schema with complete column information
        customers_table = TableSchema(
            name="customers",
            columns={
                "id": ColumnInfo(
                    dataType="INTEGER",
                    nullable=False,
                    isPrimaryKey=True,
                    isForeignKey=False,
                    typicalValues=[1, 2, 3, 100, 999]
                ),
                "name": ColumnInfo(
                    dataType="VARCHAR(100)",
                    nullable=False,
                    isPrimaryKey=False,
                    isForeignKey=False,
                    typicalValues=["John Doe", "Jane Smith", "Bob Johnson"]
                ),
                "email": ColumnInfo(
                    dataType="VARCHAR(100)",
                    nullable=True,
                    isPrimaryKey=False,
                    isForeignKey=False,
                    typicalValues=["john@example.com", "jane@company.com", None]
                )
            },
            sampleData=[
                {"id": 1, "name": "John Doe", "email": "john@example.com"},
                {"id": 2, "name": "Jane Smith", "email": None}
            ],
            metadata={"rowCount": 1000, "lastUpdated": "2025-01-01"}
        )
        
        # Add table
        await manager.add_table(customers_table)
        
        # Verify storage location and structure
        raw_data = await memory.get("databaseSchema")
        assert "tables" in raw_data
        assert "customers" in raw_data["tables"]
        assert "columns" in raw_data["tables"]["customers"]
        assert "sampleData" in raw_data["tables"]["customers"]
        assert "metadata" in raw_data["tables"]["customers"]
        
        # Retrieve and verify
        retrieved = await manager.get_table("customers")
        assert retrieved.name == "customers"
        assert len(retrieved.columns) == 3
        assert retrieved.columns["id"].isPrimaryKey == True
        assert retrieved.columns["id"].dataType == "INTEGER"
        
        # Verify TableSchema and ColumnInfo serialization
        assert len(retrieved.sampleData) == 2
        assert retrieved.metadata["rowCount"] == 1000
        
        print("✅ Table operations with complete column info tests passed")
    
    @pytest.mark.asyncio
    async def test_foreign_key_relationships_storage(self, memory, manager):
        """Test foreign key relationships are stored correctly."""
        # First add customers table
        customers_table = TableSchema(
            name="customers",
            columns={
                "id": ColumnInfo(
                    dataType="INTEGER",
                    nullable=False,
                    isPrimaryKey=True,
                    isForeignKey=False
                )
            }
        )
        await manager.add_table(customers_table)
        
        # Add orders table with foreign key
        orders_table = TableSchema(
            name="orders",
            columns={
                "id": ColumnInfo(
                    dataType="INTEGER",
                    nullable=False,
                    isPrimaryKey=True,
                    isForeignKey=False
                ),
                "customer_id": ColumnInfo(
                    dataType="INTEGER",
                    nullable=False,
                    isPrimaryKey=False,
                    isForeignKey=True,
                    references={"table": "customers", "column": "id"}
                ),
                "total": ColumnInfo(
                    dataType="DECIMAL(10,2)",
                    nullable=False,
                    isPrimaryKey=False,
                    isForeignKey=False
                )
            }
        )
        
        await manager.add_table(orders_table)
        
        # Verify foreign key storage in memory
        raw_data = await memory.get("databaseSchema")
        orders_data = raw_data["tables"]["orders"]
        customer_id_data = orders_data["columns"]["customer_id"]
        assert customer_id_data["isForeignKey"] == True
        assert customer_id_data["references"]["table"] == "customers"
        assert customer_id_data["references"]["column"] == "id"
        
        # Test retrieval operations
        customer_id_col = await manager.get_column("orders", "customer_id")
        assert customer_id_col.isForeignKey == True
        assert customer_id_col.references["table"] == "customers"
        
        # Get foreign keys
        fks = await manager.get_foreign_keys("orders")
        assert len(fks) == 1
        assert fks[0]["column"] == "customer_id"
        assert fks[0]["references_table"] == "customers"
        assert fks[0]["references_column"] == "id"
        
        print("✅ Foreign key relationships storage tests passed")
    
    @pytest.mark.asyncio
    async def test_cache_sample_data_and_metadata(self, memory, manager):
        """Test caching of sample data and metadata properly."""
        # Create table with rich sample data and metadata
        products_table = TableSchema(
            name="products",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False),
                "name": ColumnInfo(dataType="VARCHAR(200)", nullable=False,
                                 isPrimaryKey=False, isForeignKey=False),
                "price": ColumnInfo(dataType="DECIMAL(10,2)", nullable=False,
                                  isPrimaryKey=False, isForeignKey=False),
                "category_id": ColumnInfo(
                    dataType="INTEGER", nullable=True,
                    isPrimaryKey=False, isForeignKey=True,
                    references={"table": "categories", "column": "id"}
                )
            },
            sampleData=[
                {"id": 1, "name": "Laptop", "price": 999.99, "category_id": 1},
                {"id": 2, "name": "Mouse", "price": 29.99, "category_id": 2},
                {"id": 3, "name": "Keyboard", "price": 79.99, "category_id": 2},
                {"id": 4, "name": "Monitor", "price": 299.99, "category_id": 1},
                {"id": 5, "name": "USB Cable", "price": 9.99, "category_id": 3}
            ],
            metadata={
                "rowCount": 50000,
                "indexes": ["idx_name", "idx_category"],
                "constraints": ["price > 0"],
                "lastAnalyzed": "2025-01-01T10:00:00"
            }
        )
        
        await manager.add_table(products_table)
        
        # Verify sample data is cached
        sample_data = await manager.get_sample_data("products")
        assert len(sample_data) == 5
        assert sample_data[0]["name"] == "Laptop"
        assert sample_data[0]["price"] == 999.99
        
        # Verify metadata is cached
        metadata = await manager.get_table_metadata("products")
        assert metadata["rowCount"] == 50000
        assert "idx_name" in metadata["indexes"]
        assert metadata["lastAnalyzed"] == "2025-01-01T10:00:00"
        
        # Update metadata
        await manager.set_table_metadata("products", {
            "rowCount": 51000,
            "lastAnalyzed": "2025-01-02T10:00:00"
        })
        
        # Verify update
        updated_metadata = await manager.get_table_metadata("products")
        assert updated_metadata["rowCount"] == 51000
        assert updated_metadata["lastAnalyzed"] == "2025-01-02T10:00:00"
        
        print("✅ Sample data and metadata caching tests passed")
    
    @pytest.mark.asyncio
    async def test_retrieve_schema_elements_by_queries(self, memory, manager):
        """Test retrieving schema elements by various queries."""
        # Setup test schema
        await self._setup_test_schema(manager)
        
        # Test get_table_names
        names = await manager.get_table_names()
        assert set(names) == {"customers", "orders", "products"}
        
        # Test get_columns
        customer_columns = await manager.get_columns("customers")
        assert len(customer_columns) == 3
        assert "id" in customer_columns
        assert "name" in customer_columns
        assert "email" in customer_columns
        
        # Test get_primary_keys
        pks = await manager.get_primary_keys("orders")
        assert pks == ["id"]
        
        # Test search_columns_by_type
        int_columns = await manager.search_columns_by_type("INTEGER")
        assert len(int_columns) >= 4  # At least id columns from all tables
        
        # Test find_relationships
        relationships = await manager.find_relationships("orders", "customers")
        assert len(relationships) == 1
        assert relationships[0]["from_table"] == "orders"
        assert relationships[0]["from_column"] == "customer_id"
        assert relationships[0]["to_table"] == "customers"
        assert relationships[0]["to_column"] == "id"
        
        # Test get_schema_summary
        summary = await manager.get_schema_summary()
        assert summary["table_count"] == 3
        assert summary["total_columns"] >= 8
        assert summary["total_primary_keys"] == 3
        assert summary["total_foreign_keys"] >= 1
        
        print("✅ Schema element retrieval tests passed")
    
    @pytest.mark.asyncio
    async def test_handle_multiple_databases(self, memory, manager):
        """Test handling multiple databases simultaneously."""
        # Note: Current implementation stores all tables in one schema
        # This test verifies that tables from different logical databases
        # can coexist and be managed properly
        
        # Add tables representing different databases
        # Database 1: E-commerce
        await manager.add_table(TableSchema(
            name="ecom_customers",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False)
            },
            metadata={"database": "ecommerce"}
        ))
        
        # Database 2: Analytics
        await manager.add_table(TableSchema(
            name="analytics_events",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False)
            },
            metadata={"database": "analytics"}
        ))
        
        # Verify both exist
        all_tables = await manager.get_all_tables()
        assert "ecom_customers" in all_tables
        assert "analytics_events" in all_tables
        
        # Verify metadata
        ecom_meta = await manager.get_table_metadata("ecom_customers")
        assert ecom_meta["database"] == "ecommerce"
        
        analytics_meta = await manager.get_table_metadata("analytics_events")
        assert analytics_meta["database"] == "analytics"
        
        print("✅ Multiple database handling tests passed")
    
    @pytest.mark.asyncio
    async def test_schema_reader_integration(self, memory, manager):
        """Test loading schema from SchemaReader."""
        # Check if BIRD data exists
        data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
        if not os.path.exists(data_path):
            pytest.skip("BIRD dataset not found")
            return
        
        # Initialize schema reader
        tables_json_path = os.path.join(data_path, "dev_tables.json")
        if not os.path.exists(tables_json_path):
            pytest.skip("dev_tables.json not found")
            return
            
        schema_reader = SchemaReader(
            data_path=data_path,
            tables_json_path=tables_json_path,
            dataset_name="bird",
            lazy=True
        )
        
        # Load california_schools database
        db_id = "california_schools"
        await manager.load_from_schema_reader(schema_reader, db_id)
        
        # Verify tables were loaded
        table_names = await manager.get_table_names()
        assert len(table_names) > 0
        print(f"Loaded {len(table_names)} tables: {table_names}")
        
        # Check specific tables
        expected_tables = ["frpm", "satscores", "schools"]
        for table_name in expected_tables:
            table = await manager.get_table(table_name)
            assert table is not None
            assert len(table.columns) > 0
            print(f"Table '{table_name}' has {len(table.columns)} columns")
        
        print("✅ Schema reader integration tests passed")
    
    async def _setup_test_schema(self, manager):
        """Setup a test schema with multiple tables."""
        # Customers table
        customers = TableSchema(
            name="customers",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False),
                "name": ColumnInfo(dataType="VARCHAR(100)", nullable=False,
                                 isPrimaryKey=False, isForeignKey=False),
                "email": ColumnInfo(dataType="VARCHAR(100)", nullable=True,
                                  isPrimaryKey=False, isForeignKey=False)
            }
        )
        await manager.add_table(customers)
        
        # Orders table
        orders = TableSchema(
            name="orders",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False),
                "customer_id": ColumnInfo(
                    dataType="INTEGER", nullable=False,
                    isPrimaryKey=False, isForeignKey=True,
                    references={"table": "customers", "column": "id"}
                ),
                "total": ColumnInfo(dataType="DECIMAL(10,2)", nullable=False,
                                  isPrimaryKey=False, isForeignKey=False)
            }
        )
        await manager.add_table(orders)
        
        # Products table
        products = TableSchema(
            name="products",
            columns={
                "id": ColumnInfo(dataType="INTEGER", nullable=False,
                               isPrimaryKey=True, isForeignKey=False),
                "name": ColumnInfo(dataType="VARCHAR(200)", nullable=False,
                                 isPrimaryKey=False, isForeignKey=False),
                "price": ColumnInfo(dataType="DECIMAL(10,2)", nullable=False,
                                  isPrimaryKey=False, isForeignKey=False)
            }
        )
        await manager.add_table(products)


if __name__ == "__main__":
    asyncio.run(pytest.main([__file__, "-v", "-s"]))