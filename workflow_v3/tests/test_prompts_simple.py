"""
Simple test to verify prompts integration without requiring API keys.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from prompts import (
    SQL_CONSTRAINTS, MAX_ROUND, SUBQ_PATTERN,
    format_decompose_template, format_refiner_template, format_zeroshot_template,
    DECOMPOSE_TEMPLATE_BIRD, REFINER_TEMPLATE
)


def test_constants():
    """Test that constants are properly defined"""
    print("Testing constants...")
    assert MAX_ROUND == 3
    print(f"✓ MAX_ROUND = {MAX_ROUND}")
    
    # Test SQL constraints with actual content
    assert "`SELECT <column>`" in SQL_CONSTRAINTS
    assert "`JOIN <table>` FIRST" in SQL_CONSTRAINTS
    assert "`GROUP BY <column>` before" in SQL_CONSTRAINTS
    print("✓ SQL_CONSTRAINTS contains expected rules")
    
    assert SUBQ_PATTERN == r"Sub question\s*\d+\s*:"
    print(f"✓ SUBQ_PATTERN = {SUBQ_PATTERN}")


def test_template_formatting():
    """Test template formatting functions"""
    print("\nTesting template formatting...")
    
    # Test refiner template
    result = format_refiner_template(
        query="What is the gender of the youngest client?",
        evidence="Later birthdate refers to younger age",
        desc_str="# Table: client\n[(client_id, INT), (gender, VARCHAR), (birth_date, DATE)]",
        fk_str="client.district_id = district.district_id",
        sql="SELECT gender FROM client ORDER BY birth_date LIMIT 1",
        sqlite_error="no such column: birth_date",
        exception_class="SQLException"
    )
    
    assert "What is the gender of the youngest client?" in result
    assert "Later birthdate refers to younger age" in result
    assert "no such column: birth_date" in result
    assert "【correct SQL】" in result
    print("✓ format_refiner_template works correctly")
    
    # Test decompose template
    result = format_decompose_template(
        desc_str="# Table: schools\n[(id, INT), (name, VARCHAR)]",
        fk_str="schools.district_id = districts.id",
        query="List all charter schools",
        evidence="Charter schools have type='charter'",
        dataset="bird"
    )
    
    assert "List all charter schools" in result
    assert "Charter schools have type='charter'" in result
    assert "Sub question 1:" in result  # From the example
    print("✓ format_decompose_template works correctly")
    
    # Test zeroshot template
    result = format_zeroshot_template(
        desc_str="# Table: users\n[(id, INT), (name, VARCHAR)]",
        fk_str="",
        query="Count all users",
        evidence=""
    )
    
    assert "Count all users" in result
    assert "【Answer】" in result
    print("✓ format_zeroshot_template works correctly")


def test_template_content():
    """Test that templates contain expected content"""
    print("\nTesting template content...")
    
    # Check BIRD decomposition template
    assert "Sub question 1:" in DECOMPOSE_TEMPLATE_BIRD
    assert "Sub question 2:" in DECOMPOSE_TEMPLATE_BIRD
    assert "Charter schools" in DECOMPOSE_TEMPLATE_BIRD  # Example content
    print("✓ DECOMPOSE_TEMPLATE_BIRD contains example")
    
    # Check refiner template
    assert "【Instruction】" in REFINER_TEMPLATE
    assert "【old SQL】" in REFINER_TEMPLATE
    assert "【correct SQL】" in REFINER_TEMPLATE
    print("✓ REFINER_TEMPLATE has correct structure")


def test_sql_constraints_detail():
    """Test SQL constraints content in detail"""
    print("\nTesting SQL constraints in detail...")
    
    constraints = [
        "just select needed columns",
        "do not include unnecessary table",
        "`JOIN <table>` FIRST, THEN use",
        "`WHERE <column> is NOT NULL`",
        "`GROUP BY <column>` before"
    ]
    
    for constraint in constraints:
        assert constraint in SQL_CONSTRAINTS, f"Missing constraint: {constraint}"
        print(f"✓ Found constraint: '{constraint[:30]}...'")


def main():
    """Run all tests"""
    print("=" * 60)
    print("Testing Prompts Integration")
    print("=" * 60)
    
    try:
        test_constants()
        test_template_formatting()
        test_template_content()
        test_sql_constraints_detail()
        
        print("\n" + "=" * 60)
        print("✅ All tests passed!")
        print("=" * 60)
        
        # Show a sample of the SQL constraints
        print("\nSQL Constraints Preview:")
        print("-" * 40)
        lines = SQL_CONSTRAINTS.strip().split('\n')
        for line in lines[:5]:
            print(line)
        print("...")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())