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


class CombineStrategyType(Enum):
    """Types of strategies for combining child node results."""
    UNION = 'union'
    JOIN = 'join'
    AGGREGATE = 'aggregate'
    FILTER = 'filter'
    CUSTOM = 'custom'


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
class TableMapping:
    """Mapping of a table in a query."""
    name: str
    alias: Optional[str] = None
    purpose: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableMapping':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class ColumnMapping:
    """Mapping of a column in a query."""
    table: str
    column: str
    usedFor: str  # select/filter/join/groupBy/orderBy
    exactValue: Optional[str] = None  # Exact value to use for filters (from typical values)
    dataType: Optional[str] = None  # Data type of the column (e.g., INTEGER, TEXT, REAL)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnMapping':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class JoinMapping:
    """Join relationship between tables."""
    from_table: str  # 'from' is reserved keyword
    to: str
    on: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'from': self.from_table,
            'to': self.to,
            'on': self.on
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JoinMapping':
        """Create from dictionary."""
        return cls(
            from_table=data['from'],
            to=data['to'],
            on=data['on']
        )


@dataclass
class QueryMapping:
    """Complete mapping information for a query."""
    tables: List[TableMapping] = field(default_factory=list)
    columns: List[ColumnMapping] = field(default_factory=list)
    joins: Optional[List[JoinMapping]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        result = {
            'tables': [t.to_dict() for t in self.tables],
            'columns': [c.to_dict() for c in self.columns]
        }
        if self.joins:
            result['joins'] = [j.to_dict() for j in self.joins]
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryMapping':
        """Create from dictionary."""
        tables = [TableMapping.from_dict(t) for t in data.get('tables', [])]
        columns = [ColumnMapping.from_dict(c) for c in data.get('columns', [])]
        joins = None
        if 'joins' in data and data['joins']:
            joins = [JoinMapping.from_dict(j) for j in data['joins']]
        return cls(tables=tables, columns=columns, joins=joins)


@dataclass
class CombineStrategy:
    """Strategy for combining results from child nodes."""
    type: CombineStrategyType
    unionType: Optional[str] = None  # 'UNION' | 'UNION ALL'
    joinOn: Optional[List[str]] = None
    joinType: Optional[str] = None  # 'INNER' | 'LEFT' | 'RIGHT' | 'FULL'
    aggregateFunction: Optional[str] = None  # SUM, COUNT, AVG, etc.
    groupBy: Optional[List[str]] = None
    filterCondition: Optional[str] = None
    template: Optional[str] = None  # For custom strategy
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        result = {'type': self.type.value}
        for key, value in asdict(self).items():
            if key != 'type' and value is not None:
                result[key] = value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CombineStrategy':
        """Create from dictionary."""
        strategy_type = CombineStrategyType(data['type'])
        kwargs = {k: v for k, v in data.items() if k != 'type'}
        return cls(type=strategy_type, **kwargs)


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
    """Represents a sub-query in the query tree."""
    nodeId: str
    intent: str
    mapping: QueryMapping
    childIds: List[str] = field(default_factory=list)
    status: NodeStatus = NodeStatus.CREATED
    sql: Optional[str] = None
    executionResult: Optional[ExecutionResult] = None
    parentId: Optional[str] = None
    combineStrategy: Optional[CombineStrategy] = None
    evidence: Optional[str] = None  # Evidence/hints for this node
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        result = {
            'nodeId': self.nodeId,
            'intent': self.intent,
            'mapping': self.mapping.to_dict(),
            'childIds': self.childIds,
            'status': self.status.value
        }
        if self.sql is not None:
            result['sql'] = self.sql
        if self.executionResult is not None:
            if isinstance(self.executionResult, dict):
                result['executionResult'] = self.executionResult
            else:
                result['executionResult'] = self.executionResult.to_dict()
        if self.parentId is not None:
            result['parentId'] = self.parentId
        if self.combineStrategy is not None:
            result['combineStrategy'] = self.combineStrategy.to_dict()
        if self.evidence is not None:
            result['evidence'] = self.evidence
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryNode':
        """Create from dictionary."""
        node = cls(
            nodeId=data['nodeId'],
            intent=data['intent'],
            mapping=QueryMapping.from_dict(data['mapping']),
            childIds=data.get('childIds', []),
            status=NodeStatus(data['status'])
        )
        if 'sql' in data:
            node.sql = data['sql']
        if 'executionResult' in data:
            node.executionResult = ExecutionResult.from_dict(data['executionResult'])
        if 'parentId' in data:
            node.parentId = data['parentId']
        if 'combineStrategy' in data:
            node.combineStrategy = CombineStrategy.from_dict(data['combineStrategy'])
        if 'evidence' in data:
            node.evidence = data['evidence']
        return node


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