#!/usr/bin/env python3
"""
Simple test runner for the MAC-SQL workflow v2 test suite.
Runs tests from the tests directory with proper path setup.
"""

import src.sys as sys
import src.os as os
import src.subprocess as subprocess
from src.pathlib import Path

# Add parent directory to Python path so tests can import modules
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

def run_test_file(test_file):
    """Run a single test file and return success status."""
    print(f"\n{'='*60}")
    print(f"Running: {test_file}")
    print(f"{'='*60}")
    
    try:
        # Change to parent directory to run tests
        original_cwd = os.getcwd()
        os.chdir(parent_dir)
        
        # Run the test
        result = subprocess.run([
            sys.executable, 
            f"tests/{test_file}"
        ], capture_output=False, text=True)
        
        # Restore original directory
        os.chdir(original_cwd)
        
        success = result.returncode == 0
        print(f"\n{'✓' if success else '✗'} {test_file}: {'PASSED' if success else 'FAILED'}")
        return success
        
    except Exception as e:
        print(f"\n✗ {test_file}: ERROR - {str(e)}")
        return False

def main():
    """Run all tests in organized layers."""
    print("MAC-SQL Workflow v2 Test Suite")
    print("=" * 60)
    
    # Define test order (dependencies first)
    test_files = [
        # Core component tests
        "test_layer1_memory_content_types.py",
        "test_layer2_memory.py", 
        "test_layer3_managers.py",
        
        # Simple workflow tests
        "test_workflow_simple.py",
        
        # Real data tests
        "test_memory_content_types_real_data.py",
        
        # Advanced scenario tests
        "test_workflow_cases.py",
        "test_multi_database_scenarios.py",
        
        # Integration tests
        "test_integration.py",
    ]
    
    results = []
    for test_file in test_files:
        test_path = parent_dir / "tests" / test_file
        if test_path.exists():
            success = run_test_file(test_file)
            results.append((test_file, success))
        else:
            print(f"\n⚠️  Skipping {test_file} (file not found)")
            results.append((test_file, False))
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_file, success in results:
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"{status:10} {test_file}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"❌ {total - passed} tests failed")
        return 1

if __name__ == "__main__":
    exit(main())