#!/usr/bin/env python3
"""
Script to run all agent-specific tests

Runs tests for the 4 core agent tools and their integration.
"""

import src.sys as sys
import src.subprocess as subprocess
from src.pathlib import Path

# Test files to run
AGENT_TEST_FILES = [
    "test_agents_structure.py",
    "test_agent_tools.py",
    "test_layer4_agents.py"
]

def run_tests():
    """Run all agent tests"""
    print("=" * 80)
    print("Running MAC-SQL Agent Tool Tests")
    print("=" * 80)
    
    tests_dir = Path(__file__).parent
    total_passed = 0
    total_failed = 0
    failed_files = []
    
    for test_file in AGENT_TEST_FILES:
        print(f"\n📋 Running {test_file}...")
        print("-" * 60)
        
        test_path = tests_dir / test_file
        if not test_path.exists():
            print(f"❌ Test file not found: {test_file}")
            failed_files.append(test_file)
            continue
        
        try:
            # Run pytest on the file
            result = subprocess.run(
                [sys.executable, "-m", "pytest", str(test_path), "-v", "--tb=short"],
                capture_output=True,
                text=True
            )
            
            # Parse output for results
            output_lines = result.stdout.split('\n')
            
            # Look for test results
            passed = 0
            failed = 0
            
            for line in output_lines:
                if " PASSED" in line:
                    passed += 1
                elif " FAILED" in line:
                    failed += 1
            
            # Summary line
            for line in output_lines:
                if "passed" in line and ("failed" in line or "error" in line):
                    print(f"📊 {line.strip()}")
                    break
            
            if failed == 0 and passed > 0:
                print(f"✅ All tests passed in {test_file}")
            elif failed > 0:
                print(f"❌ Some tests failed in {test_file}")
                failed_files.append(test_file)
                # Print failed test details
                print("\nFailed tests:")
                for line in output_lines:
                    if "FAILED" in line and "::" in line:
                        print(f"  - {line.strip()}")
            
            total_passed += passed
            total_failed += failed
            
        except Exception as e:
            print(f"❌ Error running {test_file}: {str(e)}")
            failed_files.append(test_file)
    
    # Overall summary
    print("\n" + "=" * 80)
    print("OVERALL TEST SUMMARY")
    print("=" * 80)
    print(f"Total tests passed: {total_passed}")
    print(f"Total tests failed: {total_failed}")
    print(f"Test files run: {len(AGENT_TEST_FILES)}")
    
    if failed_files:
        print(f"\n❌ Files with failures:")
        for file in failed_files:
            print(f"  - {file}")
    else:
        print("\n✅ All agent tests passed successfully!")
    
    return len(failed_files) == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)