#!/usr/bin/env python
"""
Text-to-SQL Command Line Interface

Simple command-line interface for running text-to-SQL queries using the complete workflow.

Usage:
    python run_text_to_sql.py "Your query here" [database_name]
    
Examples:
    python run_text_to_sql.py "What is the highest eligible free rate in Alameda County?"
    python run_text_to_sql.py "Find the top 5 schools with highest SAT scores" california_schools
"""

import os
import sys
import asyncio
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from text_to_sql_workflow import run_text_to_sql


def setup_environment():
    """Setup environment variables and logging."""
    # Load environment variables
    load_dotenv()
    
    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY not found in environment")
        print("Please run: source .env && export OPENAI_API_KEY")
        print("Or set the API key in your .env file")
        sys.exit(1)
    
    print("✓ Environment setup complete")


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Text-to-SQL Command Line Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_text_to_sql.py "What schools are in Alameda County?"
  python run_text_to_sql.py "Find top 5 schools by SAT scores" california_schools
  python run_text_to_sql.py "How many students per county?" --sequential
        """
    )
    
    parser.add_argument(
        "query",
        help="Natural language query to convert to SQL"
    )
    
    parser.add_argument(
        "database",
        nargs="?",
        default="california_schools",
        help="Database name (default: california_schools)"
    )
    
    parser.add_argument(
        "--data-path",
        default="/home/norman/work/text-to-sql/MAC-SQL/data/bird",
        help="Path to database files"
    )
    
    parser.add_argument(
        "--dataset",
        default="bird",
        choices=["bird", "spider"],
        help="Dataset name (default: bird)"
    )
    
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Use sequential workflow instead of coordinator"
    )
    
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress detailed output"
    )
    
    parser.add_argument(
        "--output",
        choices=["json", "text"],
        default="text",
        help="Output format (default: text)"
    )
    
    args = parser.parse_args()
    
    # Setup environment
    setup_environment()
    
    # Display input
    if not args.quiet:
        print(f"\n{'='*80}")
        print(f"TEXT-TO-SQL WORKFLOW")
        print(f"{'='*80}")
        print(f"Query: {args.query}")
        print(f"Database: {args.database}")
        print(f"Mode: {'Sequential' if args.sequential else 'Coordinator'}")
        print(f"{'='*80}")
    
    try:
        # Run the workflow
        results = await run_text_to_sql(
            query=args.query,
            db_name=args.database,
            data_path=args.data_path,
            dataset_name=args.dataset,
            use_coordinator=not args.sequential,
            display_results=not args.quiet and args.output == "text"
        )
        
        # Output results
        if args.output == "json":
            import json
            print(json.dumps(results, indent=2, default=str))
        elif not args.quiet:
            print(f"\n{'='*80}")
            print("WORKFLOW COMPLETED SUCCESSFULLY")
            print(f"{'='*80}")
            
            # Show summary
            final_results = results.get("final_results", [])
            if final_results:
                result = final_results[0]
                if result.get("sql"):
                    print(f"\nGenerated SQL:")
                    print(result["sql"])
                
                if result.get("execution_result"):
                    exec_result = result["execution_result"]
                    print(f"\nExecution: {exec_result.get('rowCount', 0)} rows returned")
                    
                    if exec_result.get("data") and len(exec_result["data"]) > 0:
                        print("\nFirst few results:")
                        for i, row in enumerate(exec_result["data"][:3]):
                            print(f"  {row}")
                
                if result.get("analysis"):
                    analysis = result["analysis"]
                    print(f"\nEvaluation:")
                    print(f"  ✓ Answers intent: {analysis.get('answers_intent', 'Unknown')}")
                    print(f"  ✓ Quality: {analysis.get('result_quality', 'Unknown')}")
                    
                    # Show validation status
                    answers_intent = analysis.get('answers_intent', '')
                    result_quality = analysis.get('result_quality', '')
                    
                    if answers_intent in ['yes', 'partially'] and result_quality in ['excellent', 'good', 'acceptable']:
                        print(f"  🎉 WORKFLOW SUCCESS: Query answered correctly!")
                    elif answers_intent == 'no':
                        print(f"  ⚠ WORKFLOW WARNING: Query may not be fully answered")
                    elif result_quality == 'poor':
                        print(f"  ⚠ WORKFLOW WARNING: Result quality is poor")
        
        return 0
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        if not args.quiet:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)