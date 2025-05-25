#!/usr/bin/env python3
"""
Memory Consistency Verification

This script performs detailed verification of memory data consistency
throughout the workflow to ensure each agent receives correct data.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from keyvalue_memory import KeyValueMemory
from text_to_sql_workflow import TextToSQLWorkflow
from memory_content_types import QueryNode, NodeStatus, ExecutionResult

class MemoryConsistencyChecker:
    """Checks memory consistency throughout workflow execution."""
    
    def __init__(self, workflow: TextToSQLWorkflow):
        self.workflow = workflow
        self.memory = workflow.memory
        self.node_id = None
        
    async def verify_task_initialization(self) -> Dict[str, Any]:
        """Verify task context is properly initialized."""
        task_context = await self.memory.get("taskContext")
        
        checks = {
            "has_task_context": task_context is not None,
            "has_task_id": False,
            "has_database_name": False,
            "has_query": False,
            "task_id_format": False
        }
        
        if task_context:
            checks["has_task_id"] = "taskId" in task_context and task_context["taskId"]
            checks["has_database_name"] = "databaseName" in task_context and task_context["databaseName"]
            checks["has_query"] = "originalQuery" in task_context and task_context["originalQuery"]
            
            if checks["has_task_id"]:
                task_id = task_context["taskId"]
                checks["task_id_format"] = isinstance(task_id, str) and len(task_id) > 0
        
        return checks
    
    async def verify_database_schema(self) -> Dict[str, Any]:
        """Verify database schema is properly loaded."""
        schema = await self.memory.get("databaseSchema")
        
        checks = {
            "has_schema": schema is not None,
            "has_tables": False,
            "has_metadata": False,
            "table_count": 0,
            "has_columns": False,
            "has_foreign_keys": False
        }
        
        if schema:
            checks["has_tables"] = "tables" in schema and isinstance(schema["tables"], dict)
            checks["has_metadata"] = "metadata" in schema and isinstance(schema["metadata"], dict)
            
            if checks["has_tables"]:
                tables = schema["tables"]
                checks["table_count"] = len(tables)
                
                # Check if tables have proper structure
                for table_name, table_data in tables.items():
                    if "columns" in table_data and table_data["columns"]:
                        checks["has_columns"] = True
                        
                        # Check for foreign keys
                        for col_name, col_info in table_data["columns"].items():
                            if col_info.get("isForeignKey"):
                                checks["has_foreign_keys"] = True
                                break
                    
                    if checks["has_columns"] and checks["has_foreign_keys"]:
                        break
        
        return checks
    
    async def verify_query_analysis(self) -> Dict[str, Any]:
        """Verify query analysis results."""
        analysis = await self.memory.get("query_analysis")
        tree = await self.workflow.tree_manager.get_tree()
        
        checks = {
            "has_analysis": analysis is not None,
            "has_intent": False,
            "has_complexity": False,
            "has_tree": tree is not None,
            "has_root_node": False,
            "node_has_intent": False
        }
        
        if analysis:
            checks["has_intent"] = "intent" in analysis and analysis["intent"]
            checks["has_complexity"] = "complexity" in analysis and analysis["complexity"]
        
        if tree:
            checks["has_root_node"] = "rootId" in tree and tree["rootId"]
            
            if checks["has_root_node"]:
                root_id = tree["rootId"]
                self.node_id = root_id  # Store for later checks
                
                if "nodes" in tree and root_id in tree["nodes"]:
                    node_data = tree["nodes"][root_id]
                    checks["node_has_intent"] = "intent" in node_data and node_data["intent"]
        
        return checks
    
    async def verify_schema_linking(self) -> Dict[str, Any]:
        """Verify schema linking results."""
        tree = await self.workflow.tree_manager.get_tree()
        
        checks = {
            "has_tree": tree is not None,
            "node_has_mapping": False,
            "mapping_has_tables": False,
            "mapping_has_columns": False,
            "mapping_has_joins": False,
            "tables_exist_in_schema": False
        }
        
        if tree and self.node_id and "nodes" in tree and self.node_id in tree["nodes"]:
            node_data = tree["nodes"][self.node_id]
            
            if "mapping" in node_data and node_data["mapping"]:
                mapping = node_data["mapping"]
                checks["node_has_mapping"] = True
                
                checks["mapping_has_tables"] = "tables" in mapping and len(mapping["tables"]) > 0
                checks["mapping_has_columns"] = "columns" in mapping and len(mapping["columns"]) > 0
                
                # Joins are only required for multi-table queries
                table_count = len(mapping["tables"]) if "tables" in mapping else 0
                if table_count > 1:
                    checks["mapping_has_joins"] = "joins" in mapping and len(mapping["joins"]) > 0
                else:
                    checks["mapping_has_joins"] = True  # Not required for single table queries
                
                # Verify mapped tables exist in schema
                if checks["mapping_has_tables"]:
                    schema = await self.memory.get("databaseSchema")
                    if schema and "tables" in schema:
                        schema_table_names = set(schema["tables"].keys())
                        mapped_table_names = set(t["name"] for t in mapping["tables"])
                        checks["tables_exist_in_schema"] = mapped_table_names.issubset(schema_table_names)
        
        return checks
    
    async def verify_sql_generation(self) -> Dict[str, Any]:
        """Verify SQL generation results."""
        tree = await self.workflow.tree_manager.get_tree()
        
        checks = {
            "has_tree": tree is not None,
            "node_has_sql": False,
            "sql_not_empty": False,
            "sql_has_select": False,
            "sql_has_from": False,
            "sql_syntax_basic": False,
            "references_mapped_tables": False
        }
        
        if tree and self.node_id and "nodes" in tree and self.node_id in tree["nodes"]:
            node_data = tree["nodes"][self.node_id]
            
            if "sql" in node_data and node_data["sql"]:
                sql = node_data["sql"]
                checks["node_has_sql"] = True
                checks["sql_not_empty"] = len(sql.strip()) > 0
                
                sql_upper = sql.upper()
                checks["sql_has_select"] = "SELECT" in sql_upper
                checks["sql_has_from"] = "FROM" in sql_upper
                checks["sql_syntax_basic"] = sql.count("(") == sql.count(")")
                
                # Check if SQL references tables from mapping
                if "mapping" in node_data and node_data["mapping"]:
                    mapping = node_data["mapping"]
                    if "tables" in mapping:
                        mapped_table_names = [t["name"] for t in mapping["tables"]]
                        checks["references_mapped_tables"] = any(table in sql for table in mapped_table_names)
        
        return checks
    
    async def verify_sql_execution(self) -> Dict[str, Any]:
        """Verify SQL execution results."""
        tree = await self.workflow.tree_manager.get_tree()
        
        checks = {
            "has_tree": tree is not None,
            "node_has_execution_result": False,
            "execution_successful": False,
            "has_data": False,
            "has_row_count": False,
            "row_count_matches_data": False,
            "no_error": False
        }
        
        if tree and self.node_id and "nodes" in tree and self.node_id in tree["nodes"]:
            node_data = tree["nodes"][self.node_id]
            
            if "executionResult" in node_data and node_data["executionResult"]:
                exec_result = node_data["executionResult"]
                checks["node_has_execution_result"] = True
                
                checks["no_error"] = not exec_result.get("error")
                checks["has_row_count"] = "rowCount" in exec_result
                checks["has_data"] = "data" in exec_result and isinstance(exec_result["data"], list)
                
                if checks["has_row_count"] and checks["has_data"]:
                    row_count = exec_result["rowCount"]
                    data_length = len(exec_result["data"])
                    checks["row_count_matches_data"] = row_count == data_length
                
                checks["execution_successful"] = (
                    checks["no_error"] and 
                    checks["has_row_count"] and 
                    checks["row_count_matches_data"]
                )
        
        return checks
    
    async def verify_sql_evaluation(self) -> Dict[str, Any]:
        """Verify SQL evaluation results."""
        # Look for node-specific analysis
        analysis_key = f"node_{self.node_id}_analysis" if self.node_id else None
        analysis = None
        
        if analysis_key:
            analysis = await self.memory.get(analysis_key)
        
        # Also check for general analysis
        if not analysis:
            analysis = await self.memory.get("execution_analysis")
        
        checks = {
            "has_analysis": analysis is not None,
            "has_answers_intent": False,
            "has_result_quality": False,
            "has_result_summary": False,
            "has_confidence_score": False,
            "answers_intent_valid": False,
            "result_quality_valid": False,
            "confidence_in_range": False
        }
        
        if analysis:
            checks["has_answers_intent"] = "answers_intent" in analysis
            checks["has_result_quality"] = "result_quality" in analysis
            checks["has_result_summary"] = "result_summary" in analysis
            checks["has_confidence_score"] = "confidence_score" in analysis
            
            if checks["has_answers_intent"]:
                intent = analysis["answers_intent"]
                checks["answers_intent_valid"] = intent in ["yes", "no", "partially"]
            
            if checks["has_result_quality"]:
                quality = analysis["result_quality"]
                checks["result_quality_valid"] = quality in ["excellent", "good", "acceptable", "poor"]
            
            if checks["has_confidence_score"]:
                confidence = analysis["confidence_score"]
                checks["confidence_in_range"] = (
                    isinstance(confidence, (int, float)) and 
                    0 <= confidence <= 1
                )
        
        return checks
    
    async def run_full_verification(self) -> Dict[str, Any]:
        """Run complete memory consistency verification."""
        print("🔍 RUNNING MEMORY CONSISTENCY VERIFICATION")
        print("="*60)
        
        results = {}
        
        # Step 1: Task initialization
        print("1. Verifying task initialization...")
        results["task_init"] = await self.verify_task_initialization()
        self._print_check_results("Task Initialization", results["task_init"])
        
        # Step 2: Database schema
        print("\n2. Verifying database schema...")
        results["database_schema"] = await self.verify_database_schema()
        self._print_check_results("Database Schema", results["database_schema"])
        
        # Step 3: Query analysis
        print("\n3. Verifying query analysis...")
        results["query_analysis"] = await self.verify_query_analysis()
        self._print_check_results("Query Analysis", results["query_analysis"])
        
        # Step 4: Schema linking
        print("\n4. Verifying schema linking...")
        results["schema_linking"] = await self.verify_schema_linking()
        self._print_check_results("Schema Linking", results["schema_linking"])
        
        # Step 5: SQL generation
        print("\n5. Verifying SQL generation...")
        results["sql_generation"] = await self.verify_sql_generation()
        self._print_check_results("SQL Generation", results["sql_generation"])
        
        # Step 6: SQL execution
        print("\n6. Verifying SQL execution...")
        results["sql_execution"] = await self.verify_sql_execution()
        self._print_check_results("SQL Execution", results["sql_execution"])
        
        # Step 7: SQL evaluation
        print("\n7. Verifying SQL evaluation...")
        results["sql_evaluation"] = await self.verify_sql_evaluation()
        self._print_check_results("SQL Evaluation", results["sql_evaluation"])
        
        # Overall summary
        print("\n" + "="*60)
        print("OVERALL VERIFICATION SUMMARY")
        print("="*60)
        
        total_checks = 0
        passed_checks = 0
        
        for step_name, step_results in results.items():
            step_total = len(step_results)
            step_passed = sum(1 for v in step_results.values() if v)
            total_checks += step_total
            passed_checks += step_passed
            
            status = "✅ PASS" if step_passed == step_total else "⚠️  PARTIAL" if step_passed > 0 else "❌ FAIL"
            print(f"{step_name:<20} {status} ({step_passed}/{step_total})")
        
        overall_status = "✅ EXCELLENT" if passed_checks == total_checks else "⚠️  GOOD" if passed_checks > total_checks * 0.8 else "❌ NEEDS ATTENTION"
        print(f"\nOverall: {overall_status} ({passed_checks}/{total_checks} checks passed)")
        
        return results
    
    def _print_check_results(self, section_name: str, checks: Dict[str, Any]):
        """Print check results for a section."""
        passed = sum(1 for v in checks.values() if v)
        total = len(checks)
        
        for check_name, result in checks.items():
            status = "✅" if result else "❌"
            print(f"  {status} {check_name}")
        
        print(f"  Result: {passed}/{total} checks passed")


async def test_memory_consistency():
    """Run a comprehensive memory consistency test."""
    from dotenv import load_dotenv
    load_dotenv()
    
    # Initialize workflow
    data_path = "/home/norman/work/text-to-sql/MAC-SQL/data/bird"
    tables_json_path = str(Path(data_path) / "dev_tables.json")
    
    workflow = TextToSQLWorkflow(
        data_path=data_path,
        tables_json_path=tables_json_path,
        dataset_name="bird"
    )
    
    # Test query
    query = "What is the highest eligible free rate for K-12 students in schools located in Alameda County?"
    db_name = "california_schools"
    
    try:
        # Run complete workflow
        await workflow.task_manager.initialize("consistency_test", query, db_name)
        await workflow.initialize_database(db_name)
        
        # Process sequentially
        await workflow.query_analyzer.run(query)
        
        tree = await workflow.tree_manager.get_tree()
        if not tree or "rootId" not in tree:
            raise RuntimeError("Failed to create query tree")
        
        node_id = tree["rootId"]
        
        await workflow.schema_linker.run(f"node:{node_id} - Link query to database schema")
        await workflow.sql_generator.run(f"node:{node_id} - Generate SQL query")
        await workflow.sql_evaluator.run(f"node:{node_id} - Analyze SQL execution results")
        
        # Run verification
        checker = MemoryConsistencyChecker(workflow)
        results = await checker.run_full_verification()
        
        return results
        
    except Exception as e:
        print(f"❌ Error during consistency test: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.WARNING)
    
    results = asyncio.run(test_memory_consistency())
    sys.exit(0 if results else 1)