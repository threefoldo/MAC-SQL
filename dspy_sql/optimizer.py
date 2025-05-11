"""
Optimizer for DSPy Text-to-SQL
This module contains functionality for optimizing the DSPy Text-to-SQL pipeline.
"""

import json
import sqlite3
import logging
import dspy
from dspy.teleprompt import BootstrapFewShot

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_sql_execution_metric(data_path):
    """
    Create a metric function that evaluates SQL correctness by executing it.
    
    Args:
        data_path: Path to the database files
        
    Returns:
        A metric function that can be used with DSPy optimizers
    """
    def sql_execution_accuracy(example, prediction):
        """Verify if predicted SQL gives same results as gold SQL"""
        try:
            db_id = example.db_id
            db_path = f"{data_path}/{db_id}/{db_id}.sqlite"
            
            # Skip if SQL contains error
            if 'error' in prediction.final_sql:
                return 0.0
                
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Execute gold SQL
            cursor.execute(example.gold_sql)
            gold_results = cursor.fetchall()
            
            # Execute predicted SQL
            cursor.execute(prediction.final_sql)
            pred_results = cursor.fetchall()
            
            # Compare results as sets
            match = set(pred_results) == set(gold_results)
            cursor.close()
            conn.close()
            
            return 1.0 if match else 0.0
        except Exception as e:
            logger.error(f"Error in SQL execution metric: {e}")
            return 0.0
    
    return sql_execution_accuracy


def optimize_pipeline(pipeline, examples, data_path, num_bootstrapped=10):
    """
    Optimize the pipeline using bootstrapped few-shot examples.
    
    Args:
        pipeline: The DSPy pipeline to optimize
        examples: List of training examples
        data_path: Path to the database files
        num_bootstrapped: Number of examples to bootstrap
        
    Returns:
        The optimized pipeline
    """
    # Create execution accuracy metric
    metric = create_sql_execution_metric(data_path)
    
    # Create bootstrapper
    bootstrapper = BootstrapFewShot(
        metric=metric,
        num_examples=num_bootstrapped
    )
    
    # Optimize the pipeline
    logger.info("Starting pipeline optimization...")
    optimized_pipeline = bootstrapper.compile(
        pipeline,
        trainset=examples
    )
    logger.info("Pipeline optimization complete")
    
    return optimized_pipeline


def load_examples(path, limit=None):
    """
    Load examples from a JSON file for optimization.
    
    Args:
        path: Path to the examples JSON file
        limit: Maximum number of examples to load (optional)
        
    Returns:
        A list of DSPy examples
    """
    logger.info(f"Loading examples from {path}")
    
    with open(path, 'r') as f:
        raw_examples = json.load(f)
    
    # Limit number of examples if specified
    if limit:
        raw_examples = raw_examples[:limit]
    
    # Convert to DSPy format
    dspy_examples = []
    for i, ex in enumerate(raw_examples):
        dspy_examples.append(
            dspy.Example(
                db_id=ex.get('db_id'),
                query=ex.get('query'),
                evidence=ex.get('evidence', ''),
                gold_sql=ex.get('SQL', '')
            )
        )
    
    logger.info(f"Loaded {len(dspy_examples)} examples")
    return dspy_examples