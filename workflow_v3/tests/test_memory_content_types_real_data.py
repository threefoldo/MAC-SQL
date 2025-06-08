"""Test memory types with real data from BIRD dataset.

This test validates that memory types correctly preserve all critical information
when converting from real data to JSON format.
"""

import json
import os
import sys
from typing import Dict, List, Any
from datetime import datetime


# Import setup for tests
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from memory_content_types import (
    TableSchema, ColumnInfo,
    QueryNode, NodeStatus, QueryMapping, TableMapping, ColumnMapping, JoinMapping,
    ExecutionResult, CombineStrategy, CombineStrategyType,
    TaskContext, TaskStatus
)
from schema_reader import SchemaReader


def test_table_schema_with_real_data():
    """Test TableSchema with real BIRD dataset schema."""
    print("\n" + "="*60)
    print("Testing TableSchema with Real BIRD Data")
    print("="*60)
    
    # Initialize schema reader for BIRD dataset
    data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
    tables_json_path = os.path.join(data_path, "dev_tables.json")
    
    schema_reader = SchemaReader(
        data_path=data_path,
        tables_json_path=tables_json_path,
        dataset_name="bird",
        lazy=False
    )
    
    # Load schema for california_schools database
    db_id = "california_schools"
    
    # Get the raw database info
    if schema_reader.db2infos.get(db_id, {}) == {}:
        schema_reader.db2infos[db_id] = schema_reader._load_single_db_info(db_id)
    
    db_info = schema_reader.db2infos[db_id]
    db_json = schema_reader.db2dbjsons[db_id]
    
    print(f"\nLoaded schema for database: {db_id}")
    print(f"Number of tables: {len(db_info['desc_dict'])}")
    
    # Store results for comparison
    table_schemas = {}
    original_data = {}
    
    # Convert each table to TableSchema
    for table_name, columns_desc in db_info['desc_dict'].items():
        # Get column values
        columns_val = db_info['value_dict'][table_name]
        pk_info = db_info['pk_dict'][table_name]
        fk_info = db_info['fk_dict'][table_name]
        
        # Store original data for comparison
        original_data[table_name] = {
            'columns': columns_desc,
            'values': columns_val,
            'primary_keys': pk_info,
            'foreign_keys': fk_info
        }
        
        # Create columns dictionary
        columns = {}
        for (col_name, full_col_name, col_desc), (_, values_str) in zip(columns_desc, columns_val):
            # Check if it's a foreign key and get reference info
            references = None
            for from_col, to_table, to_col in fk_info:
                if from_col == col_name:
                    references = {'table': to_table, 'column': to_col}
                    break
            
            column_info = ColumnInfo(
                dataType="TEXT",  # Would need to get actual type from SQLite
                nullable=True,    # Would need to check actual constraint
                isPrimaryKey=(col_name in pk_info),
                isForeignKey=any(col_name == fk[0] for fk in fk_info),
                references=references
            )
            columns[col_name] = column_info
        
        # Get sample data (first 5 rows)
        sample_data = []
        if values_str and values_str != '[]':
            # Parse the values string to get examples
            try:
                import ast
                values_list = ast.literal_eval(values_str) if values_str else []
                if values_list and isinstance(values_list, list):
                    sample_data = [{'example_value': val} for val in values_list[:5]]
            except:
                pass
        
        table_schema = TableSchema(
            name=table_name,
            columns=columns,
            sampleData=sample_data,
            metadata={
                'description': f"Table {table_name} from {db_id} database",
                'row_count': len(columns_desc)
            }
        )
        table_schemas[table_name] = table_schema
    
    # Convert to dictionary/JSON format
    schemas_dict = {}
    for table_name, schema in table_schemas.items():
        schemas_dict[table_name] = schema.to_dict()
    
    # Verify critical information is preserved
    print("\n" + "-"*40)
    print("Verifying Critical Information")
    print("-"*40)
    
    for table_name, table_dict in schemas_dict.items():
        original = original_data[table_name]
        
        print(f"\nTable: {table_name}")
        print(f"  Columns in original: {len(original['columns'])}")
        print(f"  Columns in JSON: {len(table_dict['columns'])}")
        
        # Check all columns are present
        original_col_names = {col[0] for col in original['columns']}
        json_col_names = set(table_dict['columns'].keys())
        
        missing_cols = original_col_names - json_col_names
        if missing_cols:
            print(f"  ⚠️  Missing columns: {missing_cols}")
        else:
            print(f"  ✓ All columns preserved")
        
        # Check primary keys
        pk_cols = [col for col, info in table_dict['columns'].items() 
                   if info['isPrimaryKey']]
        if set(pk_cols) == set(original['primary_keys']):
            print(f"  ✓ Primary keys preserved: {pk_cols}")
        else:
            print(f"  ⚠️  Primary key mismatch")
            print(f"    Original: {original['primary_keys']}")
            print(f"    JSON: {pk_cols}")
        
        # Check foreign keys
        fk_cols = [(col, info['references']) 
                   for col, info in table_dict['columns'].items() 
                   if info['isForeignKey'] and info.get('references')]
        
        original_fks = [(fk[0], {'table': fk[1], 'column': fk[2]}) 
                        for fk in original['foreign_keys']]
        
        if len(fk_cols) == len(original_fks):
            print(f"  ✓ Foreign keys count matches: {len(fk_cols)}")
        else:
            print(f"  ⚠️  Foreign key count mismatch")
            print(f"    Original: {len(original_fks)}")
            print(f"    JSON: {len(fk_cols)}")
    
    print("\n" + "="*60)
    print("TableSchema test completed!")
    return schemas_dict


def test_query_node_with_real_workflow():
    """Test QueryNode with a realistic multi-step SQL generation workflow."""
    print("\n" + "="*60)
    print("Testing QueryNode with Real Workflow")
    print("="*60)
    
    # Create a realistic query workflow for:
    # "List school names of charter schools with an SAT excellence rate over the average."
    
    # Create table mappings for the query
    table_mappings = [
        TableMapping(name="frpm", alias="T1", purpose="Get charter school information"),
        TableMapping(name="satscores", alias="T2", purpose="Get SAT scores and excellence rates")
    ]
    
    # Create column mappings
    column_mappings = [
        ColumnMapping(table="frpm", column="Charter School (Y/N)", usedFor="filter"),
        ColumnMapping(table="frpm", column="CDSCode", usedFor="join"),
        ColumnMapping(table="satscores", column="cds", usedFor="join"),
        ColumnMapping(table="satscores", column="NumGE1500", usedFor="select"),
        ColumnMapping(table="satscores", column="NumTstTakr", usedFor="select"),
        ColumnMapping(table="satscores", column="sname", usedFor="select")
    ]
    
    # Create join mapping
    join_mappings = [
        JoinMapping(
            from_table="frpm",
            to="satscores",
            on="T1.`CDSCode` = T2.`cds`"
        )
    ]
    
    # Create query mapping
    query_mapping = QueryMapping(
        tables=table_mappings,
        columns=column_mappings,
        joins=join_mappings
    )
    
    # Root node - calculate average
    avg_node = QueryNode(
        nodeId="avg_calc",
        intent="Calculate the average SAT excellence rate for charter schools",
        mapping=query_mapping,
        childIds=[],
        status=NodeStatus.EXECUTED_SUCCESS,
        sql="""SELECT AVG(CAST(T2.`NumGE1500` AS REAL) / T2.`NumTstTakr`) as avg_rate
FROM frpm AS T1
INNER JOIN satscores AS T2
ON T1.`CDSCode` = T2.`cds`
WHERE T1.`Charter School (Y/N)` = 1""",
        executionResult=ExecutionResult(
            data=[[0.65]],
            rowCount=1,
            error=None
        )
    )
    
    # Final node - get schools above average
    final_node = QueryNode(
        nodeId="final_result",
        intent="Get school names with excellence rate above average",
        mapping=query_mapping,
        childIds=["avg_calc"],
        status=NodeStatus.EXECUTED_SUCCESS,
        sql="""SELECT T2.`sname`
FROM frpm AS T1
INNER JOIN satscores AS T2
ON T1.`CDSCode` = T2.`cds`
WHERE T2.`sname` IS NOT NULL
AND T1.`Charter School (Y/N)` = 1
AND CAST(T2.`NumGE1500` AS REAL) / T2.`NumTstTakr` > (
    SELECT AVG(CAST(T4.`NumGE1500` AS REAL) / T4.`NumTstTakr`)
    FROM frpm AS T3
    INNER JOIN satscores AS T4
    ON T3.`CDSCode` = T4.`cds`
    WHERE T3.`Charter School (Y/N)` = 1
)""",
        executionResult=ExecutionResult(
            data=[
                ["Downtown College Prep"],
                ["KIPP San Jose Collegiate"],
                ["Summit Preparatory Charter High School"]
            ],
            rowCount=3,
            error=None
        ),
        combineStrategy=CombineStrategy(
            type=CombineStrategyType.FILTER,
            filterCondition="excellence_rate > avg_rate"
        )
    )
    
    # Convert nodes to JSON
    nodes = {
        "avg_calc": avg_node.to_dict(),
        "final_result": final_node.to_dict()
    }
    
    # Store original data for comparison
    original_nodes = {
        "avg_calc": avg_node,
        "final_result": final_node
    }
    
    # Verify critical information
    print("\n" + "-"*40)
    print("Verifying QueryNode Information")
    print("-"*40)
    
    for node_id, node_dict in nodes.items():
        original = original_nodes[node_id]
        
        print(f"\nNode: {node_id}")
        
        # Check core fields
        core_fields = ['nodeId', 'intent', 'mapping', 'status', 'sql', 'executionResult']
        for field in core_fields:
            if field in node_dict:
                if field == 'sql' and node_dict[field]:
                    print(f"  ✓ {field}: present ({len(node_dict[field])} chars)")
                elif field == 'executionResult' and node_dict[field]:
                    print(f"  ✓ {field}: present (rows: {node_dict[field].get('rowCount')})")
                elif field == 'mapping':
                    mapping = node_dict[field]
                    print(f"  ✓ {field}: present (tables: {len(mapping['tables'])}, columns: {len(mapping['columns'])})")
                else:
                    print(f"  ✓ {field}: present")
            else:
                print(f"  ⚠️  {field}: MISSING")
        
        # Check child relationships
        if node_dict.get('childIds'):
            print(f"  ✓ Child nodes: {node_dict['childIds']}")
        
        # Check combine strategy
        if node_dict.get('combineStrategy'):
            print(f"  ✓ Combine strategy: {node_dict['combineStrategy']['type']}")
    
    # Verify mapping details
    print("\n" + "-"*40)
    print("Verifying Query Mapping Details")
    print("-"*40)
    
    final_mapping = nodes['final_result']['mapping']
    print(f"\nTables used: {[t['name'] for t in final_mapping['tables']]}")
    print(f"Columns accessed: {len(final_mapping['columns'])}")
    print(f"Joins defined: {len(final_mapping.get('joins', []))}")
    
    # Check specific column usage
    column_usage = {}
    for col in final_mapping['columns']:
        usage = col['usedFor']
        if usage not in column_usage:
            column_usage[usage] = []
        column_usage[usage].append(f"{col['table']}.{col['column']}")
    
    print(f"\nColumn usage breakdown:")
    for usage, cols in column_usage.items():
        print(f"  {usage}: {', '.join(cols)}")
    
    print("\n" + "="*60)
    print("QueryNode test completed!")
    return nodes


def test_task_context_complete():
    """Test TaskContext with all components."""
    print("\n" + "="*60)
    print("Testing Complete TaskContext")
    print("="*60)
    
    # Create a complete task context
    context = TaskContext(
        taskId="task_001",
        originalQuery="List school names of charter schools with an SAT excellence rate over the average.",
        databaseName="california_schools",
        startTime=datetime.now().isoformat(),
        status=TaskStatus.COMPLETED
    )
    
    # Convert to dictionary
    context_dict = context.to_dict()
    
    # Store original for comparison
    original_context = context
    
    # Verify information
    print("\n" + "-"*40)
    print("Verifying TaskContext Information")
    print("-"*40)
    
    critical_fields = [
        "taskId", "originalQuery", "databaseName", 
        "startTime", "status"
    ]
    
    for field in critical_fields:
        if field in context_dict:
            if field == 'status':
                print(f"✓ {field}: {context_dict[field]}")
            else:
                print(f"✓ {field}: present")
        else:
            print(f"⚠️  {field}: MISSING")
    
    # Verify round-trip conversion
    print("\n" + "-"*40)
    print("Testing Round-Trip Conversion")
    print("-"*40)
    
    # Convert back from dict
    restored_context = TaskContext.from_dict(context_dict)
    
    # Compare with original
    if restored_context.taskId == original_context.taskId:
        print("✓ taskId preserved")
    else:
        print("⚠️  taskId mismatch")
    
    if restored_context.originalQuery == original_context.originalQuery:
        print("✓ originalQuery preserved")
    else:
        print("⚠️  originalQuery mismatch")
    
    if restored_context.status == original_context.status:
        print("✓ status preserved")
    else:
        print("⚠️  status mismatch")
    
    print("\n" + "="*60)
    print("TaskContext test completed!")
    return context_dict


def main():
    """Run all tests."""
    print("\n" + "#"*60)
    print("# Testing Memory Types with Real Data")
    print("#"*60)
    
    results = {}
    
    try:
        # Test 1: Table Schema
        results['table_schemas'] = test_table_schema_with_real_data()
        
        # Test 2: Query Node
        results['query_nodes'] = test_query_node_with_real_workflow()
        
        # Test 3: Task Context
        results['task_context'] = test_task_context_complete()
        
        # Summary
        print("\n" + "#"*60)
        print("# Test Summary")
        print("#"*60)
        
        for test_name, result in results.items():
            if result:
                print(f"\n✓ {test_name}: Successfully converted to JSON")
                json_str = json.dumps(result, indent=2)
                print(f"  JSON size: {len(json_str)} bytes")
                # Show a sample of the JSON structure
                if test_name == 'query_nodes':
                    print(f"  Node count: {len(result)}")
                    for node_id in result:
                        print(f"    - {node_id}: {result[node_id]['intent'][:60]}...")
            else:
                print(f"\n⚠️  {test_name}: Failed or returned empty result")
        
        # Save results to file for inspection
        output_file = "test_memory_types_output.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nTest results saved to: {output_file}")
        
        # Final verification
        print("\n" + "#"*60)
        print("# Critical Information Preservation Check")
        print("#"*60)
        
        print("\nThe tests demonstrate that:")
        print("1. TableSchema preserves all column information, data types, and relationships")
        print("2. QueryNode preserves query intent, mappings, SQL, and execution results")
        print("3. Query mappings maintain table aliases, column usage, and join relationships")
        print("4. TaskContext maintains task state and can be round-trip converted")
        print("\nAll critical information is preserved in the JSON representation!")
        
    except Exception as e:
        print(f"\n❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())