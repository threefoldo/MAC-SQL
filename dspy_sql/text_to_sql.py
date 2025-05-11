"""
Main DSPy Text-to-SQL System
This module contains the main system that combines all components.
"""

import os
import logging
from typing import Dict, Any

import dspy
from dspy.predict import Predict

# Import from original codebase
from core.utils import parse_json, parse_sql_from_string

# Import local modules
from dspy_sql.models import create_gemini_lm
from dspy_sql.schema_manager import SchemaManager
from dspy_sql.agents import SchemaExtractor, SqlDecomposer, SqlValidator

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DSPyTextToSQL:
    """
    DSPy-based Text-to-SQL system that combines schema selection,
    query decomposition, and SQL validation/refinement.
    """

    def __init__(self, data_path, tables_json_path=None, dataset_name="bird"):
        self.data_path = data_path
        self.dataset_name = dataset_name

        # Determine tables_json_path if not provided
        if tables_json_path is None:
            if dataset_name.lower() == "bird":
                self.tables_json_path = os.path.join(data_path, "dev_tables.json")
            elif dataset_name.lower() == "spider":
                self.tables_json_path = os.path.join(data_path, "tables.json")
            else:
                raise ValueError(f"Unsupported dataset name: {dataset_name}")
        else:
            self.tables_json_path = tables_json_path

        logger.info(f"Using tables JSON: {self.tables_json_path}")

        # Create schema manager
        self.schema_manager = SchemaManager(data_path, self.tables_json_path)
        
        # Create DSPy language model using Gemini
        self.lm = create_gemini_lm()
        
        # Initialize DSPy modules with the language model
        self.schema_extractor = SchemaExtractor(lm=self.lm)
        self.sql_decomposer = SqlDecomposer(dataset_name=dataset_name, lm=self.lm)
        self.sql_validator = SqlValidator(data_path=data_path, dataset_name=dataset_name, lm=self.lm)
        
        # Define pipeline combining all modules
        # Use Chain instead of TypedChain for compatibility with older dspy versions
        self.text_to_sql_pipeline = dspy.Chain(
            self.schema_extractor,
            self.sql_decomposer,
            self.sql_validator
        )
    
    def process_query(self, db_id, query, evidence=""):
        """
        Process a text-to-SQL query through the entire pipeline.
        
        Args:
            db_id: Database ID
            query: User question/query
            evidence: Additional context or clues
            
        Returns:
            Dictionary with results from each stage and final SQL
        """
        logger.info(f"Processing query: {query} (DB: {db_id})")
        
        # Get database schema
        need_prune = self.schema_manager.is_need_prune(db_id)
        
        # 1. Schema Extraction
        extracted_schema = {}
        if need_prune:
            logger.info("Database is complex, performing schema extraction")
            # Get basic schema for extraction
            basic_schema = self.schema_manager.get_db_schema(db_id)
            
            # Run schema extraction
            extraction_result = self.schema_extractor(
                db_id=db_id,
                query=query,
                db_schema=basic_schema["schema_str"],
                foreign_keys=basic_schema["fk_str"],
                evidence=evidence
            )
            
            # Parse the extracted schema
            try:
                extracted_schema = extraction_result.extracted_schema
                logger.info(f"Extracted schema with {len(extracted_schema)} tables")
            except Exception as e:
                logger.error(f"Error in schema extraction: {e}")
                extracted_schema = {}
        else:
            logger.info("Database is simple, skipping schema extraction")
        
        # Get detailed schema with extracted tables
        schema_info = self.schema_manager.get_db_schema(
            db_id, 
            extracted_schema=extracted_schema
        )
        
        # 2. SQL Decomposition
        logger.info("Decomposing query into sub-questions")
        decomposition_result = self.sql_decomposer(
            query=query,
            schema_info=schema_info["schema_str"],
            foreign_keys=schema_info["fk_str"],
            evidence=evidence
        )
        
        # Extract SQL from result
        sql = decomposition_result.sql
        logger.info(f"Generated initial SQL: {sql[:100]}...")
        
        # 3. SQL Validation
        logger.info("Validating and refining SQL")
        validation_result = self.sql_validator(
            query=query,
            sql=sql,
            schema_info=schema_info["schema_str"],
            foreign_keys=schema_info["fk_str"],
            db_id=db_id,
            evidence=evidence
        )
        
        # Determine final SQL
        final_sql = validation_result.refined_sql if hasattr(validation_result, 'refined_sql') else sql
        logger.info(f"Final SQL: {final_sql[:100]}...")
        
        # Prepare final result
        result = {
            "db_id": db_id,
            "query": query,
            "evidence": evidence,
            "extracted_schema": extracted_schema,
            "schema_str": schema_info["schema_str"],
            "fk_str": schema_info["fk_str"],
            "chosen_db_schem_dict": schema_info["chosen_columns"],
            "sub_questions": decomposition_result.sub_questions,
            "initial_sql": sql,
            "refined_sql": validation_result.refined_sql if hasattr(validation_result, 'refined_sql') else sql,
            "explanation": validation_result.explanation if hasattr(validation_result, 'explanation') else "",
            "final_sql": final_sql
        }
        
        return result
    
    def set_optimized_pipeline(self, optimized_pipeline):
        """
        Replace the current pipeline with an optimized one.
        
        Args:
            optimized_pipeline: The optimized pipeline
            
        Returns:
            self (for chaining)
        """
        self.text_to_sql_pipeline = optimized_pipeline
        return self