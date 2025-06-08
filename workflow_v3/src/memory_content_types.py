"""
Shared data types and structures for the text-to-SQL memory system.

This module defines the common data structures used across all memory managers.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime


class TaskStatus(Enum):
    """Status of the overall task."""
    INITIALIZING = 'initializing'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'


class NodeStatus(Enum):
    """Status of a query node."""
    CREATED = 'created'
    SQL_GENERATED = 'sql_generated'
    EXECUTED_SUCCESS = 'executed_success'
    EXECUTED_FAILED = 'executed_failed'
    REVISED = 'revised'


class NodeOperationType(Enum):
    """Types of operations that can be performed on nodes."""
    CREATE = 'create'
    GENERATE_SQL = 'generate_sql'
    EXECUTE = 'execute'
    REVISE = 'revise'
    DELETE = 'delete'


@dataclass
class TaskContext:
    """Context information for the current task."""
    taskId: str
    originalQuery: str
    databaseName: str
    startTime: str
    status: TaskStatus
    evidence: Optional[str] = None  # Additional information/hints for the query
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'taskId': self.taskId,
            'originalQuery': self.originalQuery,
            'databaseName': self.databaseName,
            'startTime': self.startTime,
            'status': self.status.value,
            'evidence': self.evidence
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskContext':
        """Create from dictionary."""
        return cls(
            taskId=data['taskId'],
            originalQuery=data['originalQuery'],
            databaseName=data['databaseName'],
            startTime=data['startTime'],
            status=TaskStatus(data['status']),
            evidence=data.get('evidence')  # Optional field
        )


@dataclass
class ColumnInfo:
    """Information about a database column."""
    dataType: str
    nullable: bool
    isPrimaryKey: bool
    isForeignKey: bool
    references: Optional[Dict[str, str]] = None  # {'table': str, 'column': str}
    typicalValues: Optional[List[Any]] = None  # Common/example values for this column
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnInfo':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class TableSchema:
    """Schema information for a database table."""
    name: str
    columns: Dict[str, ColumnInfo]
    sampleData: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None  # rowCount, indexes, etc.
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'columns': {name: col.to_dict() for name, col in self.columns.items()},
            'sampleData': self.sampleData,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> 'TableSchema':
        """Create from dictionary."""
        columns = {name: ColumnInfo.from_dict(col_data) 
                  for name, col_data in data['columns'].items()}
        return cls(
            name=name,
            columns=columns,
            sampleData=data.get('sampleData'),
            metadata=data.get('metadata')
        )


@dataclass
class ExecutionResult:
    """Result from executing a SQL query."""
    data: Any
    rowCount: int
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionResult':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class QueryNode:
    """Represents a node in the query tree with outputs from all agents."""
    # Core identifiers and structure (needed by TaskStatusChecker and tree management)
    nodeId: str
    status: NodeStatus = NodeStatus.CREATED
    parentId: Optional[str] = None
    childIds: List[str] = field(default_factory=list)
    
    # from user or query analyzer
    intent: str = ""
    # Legacy/additional fields
    evidence: Optional[str] = None  # Evidence/hints for this node
        
    # SchemaLinker outputs  
    schema_linking: Dict[str, Any] = field(default_factory=dict)
    
    # QueryAnalyzer outputs
    decomposition: Optional[Dict[str, Any]] = None  # For complex queries with subqueries

    # SQLGenerator outputs
    generation: Dict[str, Any] = field(default_factory=dict)  # SQL, explanation, considerations

    # SQLEvaluator outputs
    evaluation: Dict[str, Any] = field(default_factory=dict)  # execution results, analysis
    
    # Generation attempt tracking
    generation_attempts: int = 0  # Counter for SQL generation attempts
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        result = {
            'nodeId': self.nodeId,
            'status': self.status.value,
            'childIds': self.childIds,
            'intent': self.intent,
            'schema_linking': self.schema_linking,
            'generation': self.generation,
            'evaluation': self.evaluation,
            'generation_attempts': self.generation_attempts
        }
        if self.parentId is not None:
            result['parentId'] = self.parentId
        if self.decomposition is not None:
            result['decomposition'] = self.decomposition
        if self.evidence is not None:
            result['evidence'] = self.evidence
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryNode':
        """Create from dictionary."""
        return cls(
            nodeId=data['nodeId'],
            status=NodeStatus(data['status']),
            parentId=data.get('parentId'),
            childIds=data.get('childIds', []),
            intent=data.get('intent', ''),
            decomposition=data.get('decomposition'),
            schema_linking=data.get('schema_linking', {}),
            generation=data.get('generation', {}),
            evaluation=data.get('evaluation', {}),
            evidence=data.get('evidence'),
            generation_attempts=data.get('generation_attempts', 0)
        )


@dataclass
class NodeOperation:
    """Record of an operation performed on a node."""
    timestamp: str
    nodeId: str
    operation: NodeOperationType
    data: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'timestamp': self.timestamp,
            'nodeId': self.nodeId,
            'operation': self.operation.value,
            'data': self.data
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeOperation':
        """Create from dictionary."""
        return cls(
            timestamp=data['timestamp'],
            nodeId=data['nodeId'],
            operation=NodeOperationType(data['operation']),
            data=data['data']
        )