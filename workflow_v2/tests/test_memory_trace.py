#!/usr/bin/env python3
"""
Memory Trace Test - Verify data flow through workflow steps

This script traces memory operations to ensure data integrity throughout
the text-to-SQL workflow. It checks:
1. Data is correctly stored by each agent
2. Data is correctly retrieved by subsequent agents
3. Data transformations are consistent
4. No data loss between workflow steps
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
from memory_content_types import QueryNode, NodeStatus

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Suppress verbose logging
logging.getLogger('autogen_core').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)

class MemoryTracer:
    """Traces memory operations during workflow execution."""
    
    def __init__(self, workflow: TextToSQLWorkflow):
        self.workflow = workflow
        self.memory = workflow.memory
        self.traces = []
        self.step_count = 0
        
    async def trace_memory_state(self, step_name: str, description: str = ""):
        """Capture current memory state."""
        self.step_count += 1
        
        # Get all memory contents
        memory_data = await self.memory.show_all(format="json")
        
        # Get query tree
        tree = await self.workflow.tree_manager.get_tree()
        
        trace_entry = {
            "step": self.step_count,
            "name": step_name,
            "description": description,
            "memory_keys": list(memory_data.keys()) if memory_data else [],
            "tree_status": self._analyze_tree_state(tree),
            "memory_size": len(memory_data) if memory_data else 0,
            "critical_data": self._extract_critical_data(memory_data, tree)
        }
        
        self.traces.append(trace_entry)
        print(f"📊 TRACE {self.step_count}: {step_name}")
        if description:
            print(f"   {description}")
        print(f"   Memory keys: {len(trace_entry['memory_keys'])}")
        print(f"   Tree status: {trace_entry['tree_status']['summary']}")
        
    def _analyze_tree_state(self, tree: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze current query tree state."""
        if not tree or "nodes" not in tree:
            return {"summary": "No tree", "nodes": 0}
        
        nodes = tree["nodes"]
        node_count = len(nodes)
        
        statuses = {}
        has_intent = 0
        has_mapping = 0
        has_sql = 0
        has_execution = 0
        
        for node_id, node_data in nodes.items():
            status = node_data.get("status", "unknown")
            statuses[status] = statuses.get(status, 0) + 1
            
            if node_data.get("intent"):
                has_intent += 1
            if node_data.get("mapping"):
                has_mapping += 1
            if node_data.get("sql"):
                has_sql += 1
            if node_data.get("executionResult"):
                has_execution += 1
        
        summary = f"{node_count} nodes"
        if has_sql > 0:
            summary += f", {has_sql} with SQL"
        if has_execution > 0:
            summary += f", {has_execution} executed"
        
        return {
            "summary": summary,
            "nodes": node_count,
            "has_intent": has_intent,
            "has_mapping": has_mapping,
            "has_sql": has_sql,
            "has_execution": has_execution,
            "statuses": statuses
        }
    
    def _extract_critical_data(self, memory_data: Dict[str, Any], tree: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract critical data points for validation."""
        critical = {
            "task_context": None,
            "database_schema": None,
            "query_analysis": None,
            "schema_mapping": None,
            "generated_sql": None,
            "execution_result": None,
            "evaluation_analysis": None
        }
        
        if memory_data:
            # Extract key memory items
            if "taskContext" in memory_data:
                task_ctx = memory_data["taskContext"]["value"]
                critical["task_context"] = {
                    "task_id": task_ctx.get("taskId"),
                    "database": task_ctx.get("databaseName"),
                    "query": task_ctx.get("query", "")[:50] + "..." if len(task_ctx.get("query", "")) > 50 else task_ctx.get("query", "")
                }
            
            if "databaseSchema" in memory_data:
                schema = memory_data["databaseSchema"]["value"]
                critical["database_schema"] = {
                    "tables": len(schema.get("tables", {})),
                    "has_metadata": "metadata" in schema
                }
            
            if "query_analysis" in memory_data:
                analysis = memory_data["query_analysis"]["value"]
                critical["query_analysis"] = {
                    "intent": analysis.get("intent", "")[:50] + "..." if len(analysis.get("intent", "")) > 50 else analysis.get("intent", ""),
                    "complexity": analysis.get("complexity")
                }
            
            # Check for node-specific analysis
            for key, data in memory_data.items():
                if key.startswith("node_") and key.endswith("_analysis"):
                    analysis = data["value"]
                    critical["evaluation_analysis"] = {
                        "answers_intent": analysis.get("answers_intent"),
                        "result_quality": analysis.get("result_quality"),
                        "confidence": analysis.get("confidence_score")
                    }
                    break
        
        # Extract tree data
        if tree and "nodes" in tree:
            for node_id, node_data in tree["nodes"].items():
                if node_data.get("sql"):
                    sql = node_data["sql"]
                    critical["generated_sql"] = {
                        "length": len(sql),
                        "preview": sql[:100] + "..." if len(sql) > 100 else sql,
                        "has_select": "SELECT" in sql.upper(),
                        "has_from": "FROM" in sql.upper()
                    }
                
                if node_data.get("executionResult"):
                    exec_result = node_data["executionResult"]
                    critical["execution_result"] = {
                        "row_count": exec_result.get("rowCount", 0),
                        "has_error": bool(exec_result.get("error")),
                        "error_preview": exec_result.get("error", "")[:50] if exec_result.get("error") else None
                    }
                
                if node_data.get("mapping"):
                    mapping = node_data["mapping"]
                    critical["schema_mapping"] = {
                        "tables": len(mapping.get("tables", [])),
                        "columns": len(mapping.get("columns", [])),
                        "joins": len(mapping.get("joins", []))
                    }
        
        return critical
    
    def validate_data_flow(self) -> List[str]:
        """Validate data flow consistency across traces."""
        issues = []
        
        if len(self.traces) < 4:
            issues.append("Insufficient traces for full validation")
            return issues
        
        # Check task context persistence
        task_contexts = [t["critical_data"]["task_context"] for t in self.traces if t["critical_data"]["task_context"]]
        if not task_contexts:
            issues.append("No task context found in any trace")
        elif len(set(tc["task_id"] for tc in task_contexts)) > 1:
            issues.append("Task ID changed during workflow")
        
        # Check schema loading
        schemas = [t["critical_data"]["database_schema"] for t in self.traces if t["critical_data"]["database_schema"]]
        if not schemas:
            issues.append("No database schema found in traces")
        elif any(s["tables"] == 0 for s in schemas):
            issues.append("Database schema missing tables")
        
        # Check progressive data building
        steps_with_sql = [i for i, t in enumerate(self.traces) if t["critical_data"]["generated_sql"]]
        steps_with_execution = [i for i, t in enumerate(self.traces) if t["critical_data"]["execution_result"]]
        steps_with_evaluation = [i for i, t in enumerate(self.traces) if t["critical_data"]["evaluation_analysis"]]
        
        # SQL should appear after initialization and analysis (step 3+)
        if steps_with_sql and min(steps_with_sql) > 4:
            issues.append("SQL generation appears too late in workflow")
        
        if steps_with_execution and not steps_with_sql:
            issues.append("Execution result without SQL generation")
        
        if steps_with_evaluation and not steps_with_execution:
            issues.append("Evaluation without execution")
        
        # Check data consistency
        for i in range(1, len(self.traces)):
            prev_trace = self.traces[i-1]
            curr_trace = self.traces[i]
            
            # Task context should not disappear
            if prev_trace["critical_data"]["task_context"] and not curr_trace["critical_data"]["task_context"]:
                issues.append(f"Task context lost between steps {i} and {i+1}")
            
            # Schema should not disappear
            if prev_trace["critical_data"]["database_schema"] and not curr_trace["critical_data"]["database_schema"]:
                issues.append(f"Database schema lost between steps {i} and {i+1}")
        
        return issues
    
    def print_summary(self):
        """Print trace summary."""
        print("\n" + "="*80)
        print("MEMORY TRACE SUMMARY")
        print("="*80)
        
        print(f"Total steps traced: {len(self.traces)}")
        
        # Data flow overview
        print("\nData Flow Overview:")
        for trace in self.traces:
            critical = trace["critical_data"]
            status_items = []
            
            if critical["task_context"]:
                status_items.append("task")
            if critical["database_schema"]:
                status_items.append("schema")
            if critical["query_analysis"]:
                status_items.append("analysis")
            if critical["schema_mapping"]:
                status_items.append("mapping")
            if critical["generated_sql"]:
                status_items.append("sql")
            if critical["execution_result"]:
                status_items.append("execution")
            if critical["evaluation_analysis"]:
                status_items.append("evaluation")
            
            status_str = " + ".join(status_items) if status_items else "empty"
            print(f"  Step {trace['step']:2d}: {trace['name']:<20} | {status_str}")
        
        # Validation
        print("\nData Flow Validation:")
        issues = self.validate_data_flow()
        if issues:
            print("❌ Issues found:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("✅ No data flow issues detected")
        
        # Critical data summary
        print("\nFinal State:")
        if self.traces:
            final_critical = self.traces[-1]["critical_data"]
            for key, value in final_critical.items():
                if value:
                    print(f"  ✓ {key}: {value}")
                else:
                    print(f"  ✗ {key}: missing")


async def test_memory_trace():
    """Run a memory trace test."""
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
    
    # Initialize tracer
    tracer = MemoryTracer(workflow)
    
    # Test query
    query = "What is the highest eligible free rate for K-12 students in schools located in Alameda County?"
    db_name = "california_schools"
    
    print(f"🔍 TRACING WORKFLOW EXECUTION")
    print(f"Query: {query}")
    print(f"Database: {db_name}")
    print("="*80)
    
    try:
        # Trace initial state
        await tracer.trace_memory_state("initial", "Workflow initialized")
        
        # Initialize task and database
        await workflow.task_manager.initialize("trace_test", query, db_name)
        await workflow.initialize_database(db_name)
        await tracer.trace_memory_state("initialized", "Task and database initialized")
        
        # Step 1: Query Analyzer
        await workflow.query_analyzer.run(query)
        await tracer.trace_memory_state("query_analyzed", "Query analyzed and tree created")
        
        # Get node ID for subsequent steps
        tree = await workflow.tree_manager.get_tree()
        if not tree or "rootId" not in tree:
            raise RuntimeError("Failed to create query tree")
        
        node_id = tree["rootId"]
        
        # Step 2: Schema Linker
        task = f"node:{node_id} - Link query to database schema"
        await workflow.schema_linker.run(task)
        await tracer.trace_memory_state("schema_linked", "Schema linked to query intent")
        
        # Step 3: SQL Generator
        task = f"node:{node_id} - Generate SQL query"
        await workflow.sql_generator.run(task)
        await tracer.trace_memory_state("sql_generated", "SQL generated from mapping")
        
        # Step 4: SQL Evaluator
        task = f"node:{node_id} - Analyze SQL execution results"
        await workflow.sql_evaluator.run(task)
        await tracer.trace_memory_state("sql_evaluated", "SQL executed and evaluated")
        
        # Final validation trace
        await tracer.trace_memory_state("completed", "Workflow completed")
        
        # Print summary
        tracer.print_summary()
        
        # Detailed memory dump
        print("\n" + "="*80)
        print("DETAILED MEMORY DUMP")
        print("="*80)
        memory_content = await workflow.memory.show_all(format="table")
        print(memory_content)
        
        return True
        
    except Exception as e:
        print(f"❌ Error during memory trace: {e}")
        import traceback
        traceback.print_exc()
        
        # Still show partial traces
        if tracer.traces:
            tracer.print_summary()
        
        return False


if __name__ == "__main__":
    success = asyncio.run(test_memory_trace())
    sys.exit(0 if success else 1)