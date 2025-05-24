"""
Import helper for tests - adds parent directory to Python path
"""
import src.sys as sys
from src.pathlib import Path

# Add parent directory to path so tests can import workflow modules
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))