"""
Unified data structures for all agents in the Text-to-SQL pipeline.
This module defines all shared data structures to ensure consistency
across different agents and the orchestrator.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
from datetime import datetime
import uuid


# Query Analyzer Data Structures

@dataclass
class ExtractedEntitiesAndIntent:
    """Extracted entities and intent from a query part."""
    metrics: List[str]  # e.g., ["count", "average_salary"]
    dimensions: List[str]  # e.g., ["department", "year"]
    filters: List[Dict[str, Any]]  # e.g., [{"field": "year", "operator": ">", "value": 2020}]
    primary_goal: str  # e.g., "aggregation", "comparison", "ranking"
    confidence: float = 0.0


@dataclass
class QueryPart:
    """Represents a single part of a decomposed query."""
    part_id: str
    processed_natural_language: str
    extracted_entities_and_intent: ExtractedEntitiesAndIntent
    dependencies: List[str] = field(default_factory=list)
    complexity_level: str = "simple"


@dataclass
class QueryPlan:
    """The complete query execution plan."""
    type: str  # "Single_Query" or "Multi_Part_Query"
    parts: List[QueryPart]
    assembly_instructions_for_multi_part: Optional[str] = None


@dataclass
class QueryAnalysisInput:
    """Input to the Query Analyzer Agent."""
    natural_language_query: str
    database_id: str
    database_schema: Optional[Dict[str, Any]] = None
    database_profile: Optional[Dict[str, Any]] = None
    conversation_history: Optional[List[Dict[str, str]]] = None


@dataclass
class QueryAnalysisOutput:
    """Output from the Query Analyzer Agent."""
    query_plan: QueryPlan
    processed_natural_language: str
    extracted_entities_and_intent: ExtractedEntitiesAndIntent
    overall_query_understanding_summary: str
    confidence_score: float


# Schema Linking Data Structures

@dataclass
class SchemaElement:
    """Represents a schema element with its relevance information."""
    element_name: str  # e.g., "schools" or "schools.County"
    element_type: str  # "Table" or "Column"
    table_name: str
    column_name: Optional[str] = None  # None for table elements
    relevance_score: float = 0.0
    mapping_rationale: str = ""
    data_type: Optional[str] = None
    constraints: Optional[List[str]] = None  # PK, FK, NOT NULL, etc.
    value_examples: Optional[List[str]] = None


@dataclass
class JoinPath:
    """Represents a join path between tables."""
    from_table: str
    to_table: str
    from_column: str
    to_column: str
    join_condition: str  # e.g., "schools.CDSCode = satscores.cds"
    join_type: str  # "INNER", "LEFT", "RIGHT"
    confidence: float


@dataclass
class SchemaLinkingInput:
    """Input to the Schema Linking Agent."""
    processed_natural_language: str
    extracted_entities_and_intent: ExtractedEntitiesAndIntent
    database_id: str
    database_schema: Dict[str, Any]
    domain_knowledge_context: Optional[Dict[str, Any]] = None


@dataclass
class SchemaLinkingOutput:
    """Output from the Schema Linking Agent."""
    relevant_schema_elements: List[SchemaElement]
    proposed_join_paths: List[JoinPath]
    overall_linking_confidence: float
    unresolved_elements_notes: List[str]


# SQL Generation Data Structures

@dataclass
class SQLGenerationInput:
    """Input to the SQL Generation Agent."""
    processed_natural_language: str
    linked_schema: SchemaLinkingOutput
    database_id: str
    sql_dialect: str = "PostgreSQL"
    dependent_sub_query_results: Optional[Dict[str, Any]] = None
    refinement_instructions: Optional[str] = None


@dataclass
class SQLGenerationOutput:
    """Output from the SQL Generation Agent."""
    sql_query: str
    generation_confidence: float
    brief_explanation_of_sql_logic: str
    validation_status_self_assessed: str  # "Presumed_Valid" or "Potential_Issues_Noted"
    potential_issues: Optional[List[str]] = None


# SQL Execution and Coherence Data Structures

@dataclass
class QueryResults:
    """Results from SQL execution."""
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    data_types: Optional[List[str]] = None


@dataclass
class PerformanceMetrics:
    """Performance metrics for the execution."""
    execution_time_ms: float
    rows_returned: int
    query_length: int
    memory_usage_mb: Optional[float] = None


@dataclass
class SQLExecutionResult:
    """Result of SQL execution."""
    status: str  # "Success", "Error"
    query_results: Optional[QueryResults] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None  # "Syntax", "Semantic", "Timeout", "Other"
    performance_metrics: Optional[PerformanceMetrics] = None


@dataclass
class CoherenceAssessment:
    """Assessment of result coherence."""
    is_result_satisfactory: bool
    coherence_score: float
    explanation: str
    issues_found: Optional[List[str]] = None


@dataclass
class RefinementSuggestion:
    """Suggestion for query refinement."""
    target_agent: str  # Which agent should retry
    feedback: str  # Specific feedback for retry
    priority: int = 1  # Priority of this suggestion


@dataclass
class SQLExecutionAndCoherenceInput:
    """Input to the SQL Execution and Coherence Agent."""
    sql_to_execute: str
    database_id: str
    original_query_part: QueryPart
    generated_sql_context: SQLGenerationOutput
    database_schema: Dict[str, Any]


@dataclass
class SQLExecutionAndCoherenceOutput:
    """Output from the SQL Execution and Coherence Agent."""
    execution_result: SQLExecutionResult
    coherence_assessment: CoherenceAssessment
    is_final_result_satisfactory_for_part: bool
    refinement_proposals_for_orchestrator: Optional[List[RefinementSuggestion]] = None


# Orchestrator Data Structures

class TaskStatus(Enum):
    """Overall task status."""
    NEW = "NEW"
    ANALYZING_QUERY = "ANALYZING_QUERY"
    PROCESSING_PARTS = "PROCESSING_PARTS"
    REFINING_PART = "REFINING_PART"
    AGGREGATING_RESULTS = "AGGREGATING_RESULTS"
    COMPLETED_SUCCESS = "COMPLETED_SUCCESS"
    COMPLETED_FAILURE = "COMPLETED_FAILURE"


class PartStatus(Enum):
    """Individual query part status."""
    PENDING = "PENDING"
    SCHEMA_LINKING = "SCHEMA_LINKING"
    SQL_GENERATING = "SQL_GENERATING"
    EXECUTING_COHERENCE_CHECKING = "EXECUTING_COHERENCE_CHECKING"
    REFINEMENT_LOOP = "REFINEMENT_LOOP"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class QueryPartState:
    """State tracking for each query part."""
    part_id: str
    status: PartStatus = PartStatus.PENDING
    linked_schema_output: Optional[SchemaLinkingOutput] = None
    generated_sql_output: Optional[SQLGenerationOutput] = None
    execution_coherence_output: Optional[SQLExecutionAndCoherenceOutput] = None
    retry_count: int = 0
    error_messages: List[str] = field(default_factory=list)
    last_error: Optional[str] = None


@dataclass
class TaskState:
    """Overall task state tracking."""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    natural_language_query: str = ""
    database_id: str = ""
    sql_dialect: str = "PostgreSQL"
    overall_task_status: TaskStatus = TaskStatus.NEW
    database_schema_data: Optional[Dict[str, Any]] = None
    query_analysis_output: Optional[QueryAnalysisOutput] = None
    query_parts_states: Dict[str, QueryPartState] = field(default_factory=dict)
    intermediate_results: Dict[str, Any] = field(default_factory=dict)
    current_part_id_processing: Optional[str] = None
    global_retry_count: int = 0
    error_log: List[str] = field(default_factory=list)
    final_sql_queries: List[str] = field(default_factory=list)
    final_data_results: Optional[Any] = None
    final_status_message: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    execution_metadata: Dict[str, Any] = field(default_factory=dict)


# Utility classes for type safety

@dataclass
class DatabaseSchema:
    """Database schema information."""
    database_id: str
    tables: Dict[str, List[Dict[str, Any]]]  # table_name -> columns
    foreign_keys: List[Dict[str, str]]
    primary_keys: Dict[str, List[str]]  # table_name -> pk_columns
    indexes: Optional[Dict[str, List[str]]] = None
    constraints: Optional[Dict[str, List[str]]] = None


# Error types for better error handling

class Text2SQLError(Exception):
    """Base exception for Text-to-SQL pipeline."""
    pass


class QueryAnalysisError(Text2SQLError):
    """Error in query analysis phase."""
    pass


class SchemaLinkingError(Text2SQLError):
    """Error in schema linking phase."""
    pass


class SQLGenerationError(Text2SQLError):
    """Error in SQL generation phase."""
    pass


class SQLExecutionError(Text2SQLError):
    """Error in SQL execution phase."""
    pass


class CoherenceError(Text2SQLError):
    """Error in coherence checking phase."""
    pass


# Database schema definitions (moved from original file)

SCHEMAS = {
    "california_schools": {
        "tables": {
            "frpm": {
                "columns": {
                    "CDSCode": {"type": "TEXT", "primary_key": True},
                    "County Name": {"type": "TEXT"},
                    "District Name": {"type": "TEXT"},
                    "School Name": {"type": "TEXT"},
                    "Educational Option Type": {"type": "TEXT"},
                    "Charter School (Y/N)": {"type": "INTEGER"},
                    "Charter Funding Type": {"type": "TEXT"},
                    "Free Meal Count (K-12)": {"type": "INTEGER"},
                    "FRPM Count (K-12)": {"type": "INTEGER"},
                    "Enrollment (K-12)": {"type": "REAL"},
                    "Free Meal Count (Ages 5-17)": {"type": "INTEGER"},
                    "Enrollment (Ages 5-17)": {"type": "REAL"},
                }
            },
            "schools": {
                "columns": {
                    "CDSCode": {"type": "TEXT", "primary_key": True},
                    "Phone": {"type": "TEXT"},
                    "MailStreet": {"type": "TEXT"},
                    "Zip": {"type": "TEXT"},
                    "OpenDate": {"type": "DATE"},
                }
            },
            "satscores": {
                "columns": {
                    "cds": {"type": "TEXT", "primary_key": True},
                    "AvgScrRead": {"type": "INTEGER"},
                    "AvgScrMath": {"type": "INTEGER"},
                    "AvgScrWrite": {"type": "INTEGER"},
                }
            }
        },
        "foreign_keys": [
            {
                "from_table": "frpm",
                "from_column": "CDSCode",
                "to_table": "schools",
                "to_column": "CDSCode"
            },
            {
                "from_table": "satscores",
                "from_column": "cds",
                "to_table": "schools",
                "to_column": "CDSCode"
            }
        ]
    },
    "financial": {
        "tables": {
            "trans": {
                "columns": {
                    "trans_id": {"type": "INTEGER", "primary_key": True},
                    "account_id": {"type": "INTEGER"},
                    "amount": {"type": "REAL"},
                    "type": {"type": "TEXT"},
                    "date": {"type": "DATE"},
                }
            },
            "account": {
                "columns": {
                    "account_id": {"type": "INTEGER", "primary_key": True},
                    "district_id": {"type": "INTEGER"},
                    "frequency": {"type": "TEXT"},
                    "date": {"type": "DATE"},
                }
            }
        },
        "foreign_keys": [
            {
                "from_table": "trans",
                "from_column": "account_id",
                "to_table": "account",
                "to_column": "account_id"
            }
        ]
    },
    "default": {
        "tables": {
            "Customers": {
                "columns": {
                    "CustomerID": {"type": "INTEGER", "primary_key": True},
                    "Name": {"type": "VARCHAR"},
                    "City": {"type": "VARCHAR"},
                }
            }
        },
        "foreign_keys": []
    }
}


def get_schema_xml(database_id: str) -> str:
    """
    Get schema in XML format for a given database ID.
    
    Args:
        database_id: The database identifier
        
    Returns:
        XML string representation of the schema
    """
    schema_data = SCHEMAS.get(database_id, SCHEMAS["default"])
    
    xml_parts = [f'<Schema>\n    <DatabaseID>{database_id}</DatabaseID>\n    <Tables>']
    
    # Add tables
    for table_name, table_info in schema_data["tables"].items():
        xml_parts.append(f'        <table>\n            <n>{table_name}</n>\n            <Columns>')
        
        # Add columns
        for col_name, col_info in table_info["columns"].items():
            xml_parts.append(f'                <Column>')
            xml_parts.append(f'                    <n>{col_name}</n>')
            xml_parts.append(f'                    <Type>{col_info["type"]}</Type>')
            xml_parts.append(f'                </Column>')
        
        xml_parts.append('            </Columns>')
        
        # Add primary key
        primary_keys = [col for col, info in table_info["columns"].items() 
                       if info.get("primary_key")]
        if primary_keys:
            xml_parts.append(f'            <PrimaryKey>{primary_keys[0]}</PrimaryKey>')
        
        xml_parts.append('        </table>')
    
    xml_parts.append('    </Tables>\n    <ForeignKeys>')
    
    # Add foreign keys
    for fk in schema_data["foreign_keys"]:
        xml_parts.append(f'        <ForeignKey>')
        xml_parts.append(f'            <FromTable>{fk["from_table"]}</FromTable>')
        xml_parts.append(f'            <FromColumn>{fk["from_column"]}</FromColumn>')
        xml_parts.append(f'            <ToTable>{fk["to_table"]}</ToTable>')
        xml_parts.append(f'            <ToColumn>{fk["to_column"]}</ToColumn>')
        xml_parts.append(f'        </ForeignKey>')
    
    xml_parts.append('    </ForeignKeys>\n</Schema>')
    
    return '\n'.join(xml_parts)


# Query patterns for different database contexts
QUERY_PATTERNS = {
    "california_schools": {
        "eligible_free_rate": {
            "keywords": ["eligible free rate", "free meal rate", "free lunch rate"],
            "tables": ["frpm"],
            "calculation": "Free Meal Count (K-12) / Enrollment (K-12)"
        },
        "charter_schools": {
            "keywords": ["charter school", "charter"],
            "tables": ["frpm", "schools"],
            "filter": "Charter School (Y/N) = 1"
        },
        "sat_scores": {
            "keywords": ["sat", "test scores", "academic performance"],
            "tables": ["satscores", "schools"],
            "columns": ["AvgScrRead", "AvgScrMath", "AvgScrWrite"]
        }
    },
    "financial": {
        "large_transactions": {
            "keywords": ["large transaction", "over", "greater than", "above"],
            "tables": ["trans"],
            "columns": ["trans_id", "amount", "type", "date", "account_id"]
        },
        "account_info": {
            "keywords": ["account", "frequency", "district"],
            "tables": ["account", "trans"],
            "join": "account.account_id = trans.account_id"
        }
    }
}