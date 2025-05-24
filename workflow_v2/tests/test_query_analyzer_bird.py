"""
Test cases for QueryAnalyzerAgent using real BIRD dataset examples

Tests query analysis, decomposition, and tree structure creation with actual
text-to-SQL examples from the BIRD benchmark.
"""

import src.asyncio as asyncio
import src.pytest as pytest
from src.datetime import datetime
from src.pathlib import Path
import src.sys as sys
import src.json as json

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.memory import KeyValueMemory
from src.memory_types import (
    TaskContext, QueryNode, NodeStatus, TaskStatus,
    CombineStrategy, CombineStrategyType, QueryMapping,
    TableMapping, ColumnMapping, JoinMapping,
    TableSchema, ColumnInfo
)
from src.task_context_manager import TaskContextManager
from src.query_tree_manager import QueryTreeManager
from src.database_schema_manager import DatabaseSchemaManager
from src.node_history_manager import NodeHistoryManager


class TestQueryAnalyzerBIRD:
    """Test cases for query analysis using BIRD dataset"""
    
    async def setup_california_schools_schema(self, schema_manager: DatabaseSchemaManager):
        """Setup California Schools database schema"""
        
        # frpm table (Free and Reduced Price Meals)
        frpm_schema = TableSchema(
            name="frpm",
            columns={
                "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "County Name": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "District Name": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "School Name": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Educational Option Type": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Charter School (Y/N)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Charter Funding Type": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Free Meal Count (K-12)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Enrollment (K-12)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Free Meal Count (Ages 5-17)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Enrollment (Ages 5-17)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "FRPM Count (K-12)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "FRPM Count (Ages 5-17)": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "School Code": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(frpm_schema)
        
        # schools table
        schools_schema = TableSchema(
            name="schools",
            columns={
                "CDSCode": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=False),
                "NCESSchool": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "StatusType": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "County": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "District": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "School": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "MailStreet": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "MailCity": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "MailState": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "MailZip": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Phone": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "OpenDate": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "ClosedDate": ColumnInfo(dataType="DATE", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Virtual": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Magnet": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "CharterNum": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "Zip": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(schools_schema)
        
        # satscores table
        satscores_schema = TableSchema(
            name="satscores",
            columns={
                "cds": ColumnInfo(dataType="TEXT", nullable=False, isPrimaryKey=True, isForeignKey=True,
                                references={"table": "schools", "column": "CDSCode"}),
                "rtype": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "sname": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "dname": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "cname": ColumnInfo(dataType="TEXT", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "enroll12": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "NumTstTakr": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "AvgScrRead": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "AvgScrMath": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "AvgScrWrite": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False),
                "NumGE1500": ColumnInfo(dataType="INTEGER", nullable=True, isPrimaryKey=False, isForeignKey=False)
            }
        )
        await schema_manager.add_table(satscores_schema)
    
    @pytest.mark.asyncio
    async def test_simple_calculation_query(self):
        """Test simple query with calculation - BIRD Question 0"""
        memory = KeyValueMemory()
        
        # Initialize managers
        task_manager = TaskContextManager(memory)
        await task_manager.initialize(
            "bird_q0",
            "What is the highest eligible free rate for K-12 students in the schools in Alameda County?",
            "california_schools"
        )
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_california_schools_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(
            "What is the highest eligible free rate for K-12 students in the schools in Alameda County?"
        )
        
        # Simulate query analysis
        # This is a simple query that requires:
        # 1. Calculate eligible free rate = Free Meal Count (K-12) / Enrollment (K-12)
        # 2. Filter by County Name = 'Alameda'
        # 3. Find the highest rate
        
        mapping = QueryMapping(
            tables=[TableMapping(name="frpm", purpose="Calculate free rates and filter by county")],
            columns=[
                ColumnMapping(table="frpm", column="Free Meal Count (K-12)", usedFor="calculate"),
                ColumnMapping(table="frpm", column="Enrollment (K-12)", usedFor="calculate"),
                ColumnMapping(table="frpm", column="County Name", usedFor="filter")
            ]
        )
        
        await tree_manager.update_node(root_id, {
            "mapping": mapping.to_dict(),
            "sql": "SELECT `Free Meal Count (K-12)` / `Enrollment (K-12)` FROM frpm WHERE `County Name` = 'Alameda' ORDER BY (CAST(`Free Meal Count (K-12)` AS REAL) / `Enrollment (K-12)`) DESC LIMIT 1"
        })
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert node.intent == "What is the highest eligible free rate for K-12 students in the schools in Alameda County?"
        assert len(node.mapping.tables) == 1
        assert node.mapping.tables[0].name == "frpm"
        assert "Free Meal Count (K-12)" in [col.column for col in node.mapping.columns]
    
    @pytest.mark.asyncio
    async def test_simple_join_query(self):
        """Test simple JOIN query - BIRD Question 2"""
        memory = KeyValueMemory()
        
        # Initialize
        task_manager = TaskContextManager(memory)
        await task_manager.initialize(
            "bird_q2",
            "Please list the zip code of all the charter schools in Fresno County Office of Education.",
            "california_schools"
        )
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_california_schools_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(
            "Please list the zip code of all the charter schools in Fresno County Office of Education."
        )
        
        # This requires joining frpm and schools tables
        mapping = QueryMapping(
            tables=[
                TableMapping(name="frpm", alias="T1", purpose="Filter charter schools in district"),
                TableMapping(name="schools", alias="T2", purpose="Get zip codes")
            ],
            columns=[
                ColumnMapping(table="schools", column="Zip", usedFor="select"),
                ColumnMapping(table="frpm", column="District Name", usedFor="filter"),
                ColumnMapping(table="frpm", column="Charter School (Y/N)", usedFor="filter"),
                ColumnMapping(table="frpm", column="CDSCode", usedFor="join"),
                ColumnMapping(table="schools", column="CDSCode", usedFor="join")
            ],
            joins=[
                JoinMapping(
                    from_table="frpm",
                    to="schools",
                    on="T1.CDSCode = T2.CDSCode"
                )
            ]
        )
        
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert len(node.mapping.tables) == 2
        assert len(node.mapping.joins) == 1
        assert node.mapping.joins[0].from_table == "frpm"
        assert node.mapping.joins[0].to == "schools"
    
    @pytest.mark.asyncio
    async def test_subquery_pattern(self):
        """Test query with subquery pattern - BIRD Question 8"""
        memory = KeyValueMemory()
        
        # Initialize
        task_manager = TaskContextManager(memory)
        await task_manager.initialize(
            "bird_q8",
            "What is the number of SAT test takers of the schools with the highest FRPM count for K-12 students?",
            "california_schools"
        )
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_california_schools_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(
            "What is the number of SAT test takers of the schools with the highest FRPM count for K-12 students?"
        )
        
        # This can be decomposed into:
        # 1. Find school with highest FRPM count
        # 2. Get SAT test takers for that school
        
        # Create child node for finding school with highest FRPM
        child1 = QueryNode(
            nodeId="find_highest_frpm",
            intent="Find school with highest FRPM count for K-12",
            mapping=QueryMapping(
                tables=[TableMapping(name="frpm")],
                columns=[
                    ColumnMapping(table="frpm", column="CDSCode", usedFor="select"),
                    ColumnMapping(table="frpm", column="FRPM Count (K-12)", usedFor="orderBy")
                ]
            )
        )
        await tree_manager.add_node(child1, root_id)
        
        # Root node uses child result
        root_mapping = QueryMapping(
            tables=[TableMapping(name="satscores")],
            columns=[
                ColumnMapping(table="satscores", column="NumTstTakr", usedFor="select"),
                ColumnMapping(table="satscores", column="cds", usedFor="filter")
            ]
        )
        
        strategy = CombineStrategy(
            type=CombineStrategyType.FILTER,
            template="Use child result to filter main query"
        )
        
        await tree_manager.update_node(root_id, {
            "mapping": root_mapping.to_dict(),
            "combineStrategy": strategy.to_dict()
        })
        
        # Verify decomposition
        children = await tree_manager.get_children(root_id)
        assert len(children) == 1
        assert children[0].intent == "Find school with highest FRPM count for K-12"
    
    @pytest.mark.asyncio
    async def test_complex_calculation_with_condition(self):
        """Test complex query with calculation and condition - BIRD Question 12"""
        memory = KeyValueMemory()
        
        # Initialize
        task_manager = TaskContextManager(memory)
        query = "Among the schools with an SAT excellence rate of over 0.3, what is the highest eligible free rate for students aged 5-17?"
        await task_manager.initialize("bird_q12", query, "california_schools")
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_california_schools_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(query)
        
        # This requires:
        # 1. Calculate SAT excellence rate = NumGE1500 / NumTstTakr
        # 2. Filter schools with excellence rate > 0.3
        # 3. Calculate eligible free rate = Free Meal Count (Ages 5-17) / Enrollment (Ages 5-17)
        # 4. Find maximum
        
        mapping = QueryMapping(
            tables=[
                TableMapping(name="frpm", alias="T1", purpose="Calculate free rates"),
                TableMapping(name="satscores", alias="T2", purpose="Filter by excellence rate")
            ],
            columns=[
                ColumnMapping(table="frpm", column="Free Meal Count (Ages 5-17)", usedFor="calculate"),
                ColumnMapping(table="frpm", column="Enrollment (Ages 5-17)", usedFor="calculate"),
                ColumnMapping(table="satscores", column="NumGE1500", usedFor="calculate"),
                ColumnMapping(table="satscores", column="NumTstTakr", usedFor="calculate"),
                ColumnMapping(table="frpm", column="CDSCode", usedFor="join"),
                ColumnMapping(table="satscores", column="cds", usedFor="join")
            ],
            joins=[
                JoinMapping(
                    from_table="frpm",
                    to="satscores",
                    on="T1.CDSCode = T2.cds"
                )
            ]
        )
        
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Verify complex mapping
        node = await tree_manager.get_node(root_id)
        assert len(node.mapping.tables) == 2
        assert any("NumGE1500" in col.column for col in node.mapping.columns)
        assert any("Free Meal Count (Ages 5-17)" in col.column for col in node.mapping.columns)
    
    @pytest.mark.asyncio
    async def test_top_n_with_join(self):
        """Test TOP N query with JOIN - BIRD Question 13"""
        memory = KeyValueMemory()
        
        # Initialize
        task_manager = TaskContextManager(memory)
        query = "Please list the phone numbers of the schools with the top 3 SAT excellence rate."
        await task_manager.initialize("bird_q13", query, "california_schools")
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_california_schools_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(query)
        
        # Requires:
        # 1. Calculate excellence rate = NumGE1500 / NumTstTakr
        # 2. Order by excellence rate DESC
        # 3. Take top 3
        # 4. Join with schools to get phone numbers
        
        mapping = QueryMapping(
            tables=[
                TableMapping(name="schools", alias="T1", purpose="Get phone numbers"),
                TableMapping(name="satscores", alias="T2", purpose="Calculate excellence rate")
            ],
            columns=[
                ColumnMapping(table="schools", column="Phone", usedFor="select"),
                ColumnMapping(table="satscores", column="NumGE1500", usedFor="calculate"),
                ColumnMapping(table="satscores", column="NumTstTakr", usedFor="calculate"),
                ColumnMapping(table="schools", column="CDSCode", usedFor="join"),
                ColumnMapping(table="satscores", column="cds", usedFor="join")
            ],
            joins=[
                JoinMapping(
                    from_table="schools",
                    to="satscores",
                    on="T1.CDSCode = T2.cds"
                )
            ]
        )
        
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert "Phone" in [col.column for col in node.mapping.columns]
        assert any("calculate" in col.usedFor for col in node.mapping.columns)
    
    @pytest.mark.asyncio
    async def test_aggregate_with_condition(self):
        """Test COUNT query with multiple conditions - BIRD Question 16"""
        memory = KeyValueMemory()
        
        # Initialize
        task_manager = TaskContextManager(memory)
        query = "How many schools in merged Alameda have number of test takers less than 100?"
        await task_manager.initialize("bird_q16", query, "california_schools")
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_california_schools_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(query)
        
        # Requires:
        # 1. Filter by StatusType = 'Merged'
        # 2. Filter by County = 'Alameda'
        # 3. Filter by NumTstTakr < 100
        # 4. Count schools
        
        mapping = QueryMapping(
            tables=[
                TableMapping(name="schools", alias="T1", purpose="Filter by status and county"),
                TableMapping(name="satscores", alias="T2", purpose="Filter by test takers")
            ],
            columns=[
                ColumnMapping(table="schools", column="CDSCode", usedFor="count"),
                ColumnMapping(table="schools", column="StatusType", usedFor="filter"),
                ColumnMapping(table="schools", column="County", usedFor="filter"),
                ColumnMapping(table="satscores", column="NumTstTakr", usedFor="filter")
            ],
            joins=[
                JoinMapping(
                    from_table="schools",
                    to="satscores",
                    on="T1.CDSCode = T2.cds"
                )
            ]
        )
        
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Verify aggregate query setup
        node = await tree_manager.get_node(root_id)
        assert any("count" in col.usedFor for col in node.mapping.columns)
        assert len([col for col in node.mapping.columns if col.usedFor == "filter"]) >= 3
    
    @pytest.mark.asyncio
    async def test_date_filter_query(self):
        """Test query with date filtering - BIRD Question 4"""
        memory = KeyValueMemory()
        
        # Initialize
        task_manager = TaskContextManager(memory)
        query = "Please list the phone numbers of the direct charter-funded schools that are opened after 2000/1/1."
        await task_manager.initialize("bird_q4", query, "california_schools")
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_california_schools_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(query)
        
        # Requires:
        # 1. Filter by Charter Funding Type = 'Directly funded'
        # 2. Filter by Charter School (Y/N) = 1
        # 3. Filter by OpenDate > '2000-01-01'
        # 4. Get phone numbers
        
        mapping = QueryMapping(
            tables=[
                TableMapping(name="frpm", alias="T1", purpose="Filter charter schools"),
                TableMapping(name="schools", alias="T2", purpose="Filter by date and get phone")
            ],
            columns=[
                ColumnMapping(table="schools", column="Phone", usedFor="select"),
                ColumnMapping(table="frpm", column="Charter Funding Type", usedFor="filter"),
                ColumnMapping(table="frpm", column="Charter School (Y/N)", usedFor="filter"),
                ColumnMapping(table="schools", column="OpenDate", usedFor="filter")
            ],
            joins=[
                JoinMapping(
                    from_table="frpm",
                    to="schools",
                    on="T1.CDSCode = T2.CDSCode"
                )
            ]
        )
        
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Verify date handling
        node = await tree_manager.get_node(root_id)
        assert any("OpenDate" in col.column for col in node.mapping.columns)
        assert any(col.column == "Phone" and col.usedFor == "select" for col in node.mapping.columns)
    
    @pytest.mark.asyncio
    async def test_null_handling_query(self):
        """Test query that needs to handle NULL values - BIRD Question 1"""
        memory = KeyValueMemory()
        
        # Initialize
        task_manager = TaskContextManager(memory)
        query = "Please list the lowest three eligible free rates for students aged 5-17 in continuation schools."
        await task_manager.initialize("bird_q1", query, "california_schools")
        
        schema_manager = DatabaseSchemaManager(memory)
        await schema_manager.initialize()
        await self.setup_california_schools_schema(schema_manager)
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(query)
        
        # Requires:
        # 1. Calculate eligible free rate = Free Meal Count (Ages 5-17) / Enrollment (Ages 5-17)
        # 2. Filter by Educational Option Type = 'Continuation School'
        # 3. Exclude NULL rates
        # 4. Order by rate ASC
        # 5. Take bottom 3
        
        mapping = QueryMapping(
            tables=[TableMapping(name="frpm", purpose="Calculate rates for continuation schools")],
            columns=[
                ColumnMapping(table="frpm", column="Free Meal Count (Ages 5-17)", usedFor="calculate"),
                ColumnMapping(table="frpm", column="Enrollment (Ages 5-17)", usedFor="calculate"),
                ColumnMapping(table="frpm", column="Educational Option Type", usedFor="filter")
            ]
        )
        
        await tree_manager.update_node(root_id, {"mapping": mapping.to_dict()})
        
        # Verify
        node = await tree_manager.get_node(root_id)
        assert "Educational Option Type" in [col.column for col in node.mapping.columns]
        # Note: NULL handling would be in the SQL generation phase


class TestQueryDecompositionBIRD:
    """Test cases for complex query decomposition using BIRD examples"""
    
    @pytest.mark.asyncio
    async def test_multi_step_analysis(self):
        """Test decomposition of multi-step analytical query"""
        memory = KeyValueMemory()
        
        # Complex query that could benefit from decomposition
        query = """
        For each district with more than 10 schools, show the district name,
        number of schools, average SAT math score, and percentage of charter schools
        """
        
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("complex_analysis", query, "california_schools")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(query)
        
        # Simulate decomposition into sub-queries
        
        # Sub-query 1: Count schools per district
        child1 = QueryNode(
            nodeId="count_schools",
            intent="Count number of schools per district",
            mapping=QueryMapping(
                tables=[TableMapping(name="schools")],
                columns=[
                    ColumnMapping(table="schools", column="District", usedFor="groupBy"),
                    ColumnMapping(table="schools", column="CDSCode", usedFor="count")
                ]
            )
        )
        await tree_manager.add_node(child1, root_id)
        
        # Sub-query 2: Calculate average SAT math score per district
        child2 = QueryNode(
            nodeId="avg_sat_math",
            intent="Calculate average SAT math score per district",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="schools", alias="s"),
                    TableMapping(name="satscores", alias="sat")
                ],
                columns=[
                    ColumnMapping(table="schools", column="District", usedFor="groupBy"),
                    ColumnMapping(table="satscores", column="AvgScrMath", usedFor="aggregate")
                ],
                joins=[JoinMapping(from_table="schools", to="satscores", on="s.CDSCode = sat.cds")]
            )
        )
        await tree_manager.add_node(child2, root_id)
        
        # Sub-query 3: Calculate percentage of charter schools per district
        child3 = QueryNode(
            nodeId="charter_percentage",
            intent="Calculate percentage of charter schools per district",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="schools", alias="s"),
                    TableMapping(name="frpm", alias="f")
                ],
                columns=[
                    ColumnMapping(table="schools", column="District", usedFor="groupBy"),
                    ColumnMapping(table="frpm", column="Charter School (Y/N)", usedFor="aggregate")
                ],
                joins=[JoinMapping(from_table="schools", to="frpm", on="s.CDSCode = f.CDSCode")]
            )
        )
        await tree_manager.add_node(child3, root_id)
        
        # Root combines all results
        strategy = CombineStrategy(
            type=CombineStrategyType.JOIN,
            template="Join all sub-query results on District, filter for districts with > 10 schools"
        )
        
        await tree_manager.update_node(root_id, {
            "combineStrategy": strategy.to_dict()
        })
        
        # Verify decomposition
        children = await tree_manager.get_children(root_id)
        assert len(children) == 3
        assert root_id in [child.parentId for child in children]
        
        root = await tree_manager.get_node(root_id)
        assert root.combineStrategy.type == CombineStrategyType.JOIN
    
    @pytest.mark.asyncio
    async def test_comparison_query_decomposition(self):
        """Test decomposition of comparison query"""
        memory = KeyValueMemory()
        
        # Query comparing two groups
        query = """
        Compare the average SAT scores between charter schools and non-charter schools
        in districts with more than 1000 total enrollment
        """
        
        task_manager = TaskContextManager(memory)
        await task_manager.initialize("comparison_query", query, "california_schools")
        
        tree_manager = QueryTreeManager(memory)
        root_id = await tree_manager.initialize(query)
        
        # Sub-query 1: Charter schools SAT scores
        child1 = QueryNode(
            nodeId="charter_sat",
            intent="Average SAT scores for charter schools in large districts",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="schools"),
                    TableMapping(name="satscores"),
                    TableMapping(name="frpm")
                ]
            )
        )
        await tree_manager.add_node(child1, root_id)
        
        # Sub-query 2: Non-charter schools SAT scores
        child2 = QueryNode(
            nodeId="non_charter_sat",
            intent="Average SAT scores for non-charter schools in large districts",
            mapping=QueryMapping(
                tables=[
                    TableMapping(name="schools"),
                    TableMapping(name="satscores"),
                    TableMapping(name="frpm")
                ]
            )
        )
        await tree_manager.add_node(child2, root_id)
        
        # Root combines for comparison
        strategy = CombineStrategy(
            type=CombineStrategyType.CUSTOM,
            template="Compare charter vs non-charter average scores side by side"
        )
        
        await tree_manager.update_node(root_id, {
            "combineStrategy": strategy.to_dict()
        })
        
        # Verify comparison structure
        children = await tree_manager.get_children(root_id)
        assert len(children) == 2
        root = await tree_manager.get_node(root_id)
        assert root.combineStrategy.type == CombineStrategyType.CUSTOM


if __name__ == "__main__":
    asyncio.run(pytest.main([__file__, "-v"]))