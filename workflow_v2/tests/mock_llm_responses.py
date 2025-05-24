"""
Mock LLM responses for deterministic testing of agent tools

Provides predefined XML responses for different test scenarios.
"""

from src.typing import Dict, Any


class MockLLMResponses:
    """Mock LLM responses for testing"""
    
    # Query Analyzer responses
    SIMPLE_SELECT_ANALYSIS = """
    <analysis>
        <intent>Retrieve employee names from the employees table</intent>
        <complexity>simple</complexity>
        <queryType>SELECT</queryType>
        <tables>employees</tables>
        <columns>name</columns>
        <filters>none</filters>
        <aggregation>none</aggregation>
        <decomposition>
            <required>false</required>
        </decomposition>
    </analysis>
    """
    
    COMPLEX_DECOMPOSITION = """
    <analysis>
        <intent>Find top departments by average salary with constraints</intent>
        <complexity>complex</complexity>
        <queryType>COMPLEX</queryType>
        <decomposition>
            <required>true</required>
            <subQueries>
                <query>
                    <id>1</id>
                    <purpose>Count employees per department</purpose>
                    <query>Count employees in each department</query>
                    <type>AGGREGATE</type>
                </query>
                <query>
                    <id>2</id>
                    <purpose>Find long-tenure employees</purpose>
                    <query>Find employees with over 5 years tenure</query>
                    <type>SELECT</type>
                </query>
                <query>
                    <id>3</id>
                    <purpose>Calculate average salaries</purpose>
                    <query>Calculate average salary by department</query>
                    <type>AGGREGATE</type>
                </query>
            </subQueries>
            <combineStrategy>
                <type>JOIN</type>
                <description>Join results to find qualifying departments then rank by average salary</description>
            </combineStrategy>
        </decomposition>
    </analysis>
    """
    
    JOIN_QUERY_ANALYSIS = """
    <analysis>
        <intent>Retrieve employees with department and manager information</intent>
        <complexity>moderate</complexity>
        <queryType>JOIN</queryType>
        <tables>employees,departments</tables>
        <columns>employees.name,departments.name,manager.name</columns>
        <joins>
            <join>employees.department_id = departments.id</join>
            <join>employees.manager_id = employees.id (self-join)</join>
        </joins>
        <decomposition>
            <required>false</required>
        </decomposition>
    </analysis>
    """
    
    # Schema Linking responses
    BASIC_SCHEMA_LINK = """
    <schemaMapping>
        <tables>
            <table>
                <name>employees</name>
                <purpose>Main table for employee data</purpose>
                <confidence>high</confidence>
            </table>
        </tables>
        <columns>
            <column>
                <table>employees</table>
                <name>name</name>
                <purpose>Employee name to display</purpose>
                <confidence>high</confidence>
            </column>
        </columns>
        <joins>none</joins>
    </schemaMapping>
    """
    
    COMPLEX_SCHEMA_LINK = """
    <schemaMapping>
        <tables>
            <table>
                <name>employees</name>
                <purpose>Employee information</purpose>
                <confidence>high</confidence>
            </table>
            <table>
                <name>departments</name>
                <purpose>Department information</purpose>
                <confidence>high</confidence>
            </table>
        </tables>
        <columns>
            <column>
                <table>employees</table>
                <name>id,name,department_id,salary,hire_date</name>
                <purpose>Employee details for analysis</purpose>
            </column>
            <column>
                <table>departments</table>
                <name>id,name</name>
                <purpose>Department identification</purpose>
            </column>
        </columns>
        <joins>
            <join>
                <type>INNER</type>
                <condition>employees.department_id = departments.id</condition>
                <purpose>Link employees to departments</purpose>
            </join>
        </joins>
    </schemaMapping>
    """
    
    # SQL Generator responses
    SIMPLE_SQL_GENERATION = """
    <sqlGeneration>
        <sql>SELECT name FROM employees</sql>
        <explanation>Simple SELECT to retrieve all employee names</explanation>
        <optimizations>Consider adding LIMIT if table is large</optimizations>
    </sqlGeneration>
    """
    
    COMPLEX_SQL_WITH_CHILDREN = """
    <sqlGeneration>
        <sql>
        WITH dept_counts AS ({child_1_result}),
             tenure_depts AS ({child_2_result}),
             avg_salaries AS ({child_3_result})
        SELECT 
            d.name AS department,
            a.avg_salary
        FROM avg_salaries a
        JOIN dept_counts c ON a.department_id = c.department_id
        JOIN tenure_depts t ON a.department_id = t.department_id
        WHERE c.employee_count > 10
        ORDER BY a.avg_salary DESC
        LIMIT 5
        </sql>
        <explanation>Combines child query results using CTEs to find top departments</explanation>
    </sqlGeneration>
    """
    
    JOIN_SQL_GENERATION = """
    <sqlGeneration>
        <sql>
        SELECT 
            e.name AS employee_name,
            d.name AS department_name,
            m.name AS manager_name
        FROM employees e
        LEFT JOIN departments d ON e.department_id = d.id
        LEFT JOIN employees m ON e.manager_id = m.id
        ORDER BY e.name
        </sql>
        <explanation>Joins employees with departments and self-join for managers</explanation>
    </sqlGeneration>
    """
    
    # SQL Executor responses
    EXECUTION_SUCCESS = """
    <execution>
        <status>success</status>
        <rowCount>25</rowCount>
        <executionTime>45</executionTime>
        <evaluation>
            <matchesIntent>true</matchesIntent>
            <quality>high</quality>
            <completeness>100</completeness>
            <issues>none</issues>
        </evaluation>
        <suggestions>
            <suggestion>
                <type>index</type>
                <description>Consider adding index on employees.name for better performance</description>
                <impact>Could reduce query time by 30%</impact>
            </suggestion>
        </suggestions>
    </execution>
    """
    
    EXECUTION_ERROR = """
    <execution>
        <status>error</status>
        <error>
            <type>syntax</type>
            <message>Column 'invalid_column' does not exist in table 'employees'</message>
            <line>3</line>
            <position>15</position>
        </error>
        <suggestions>
            <suggestion>
                <type>fix</type>
                <description>Available columns in employees: id, name, department_id, salary</description>
            </suggestion>
        </suggestions>
    </execution>
    """
    
    EXECUTION_TIMEOUT = """
    <execution>
        <status>timeout</status>
        <partialResults>true</partialResults>
        <rowCount>10000</rowCount>
        <executionTime>30000</executionTime>
        <evaluation>
            <matchesIntent>partial</matchesIntent>
            <quality>unknown</quality>
            <issues>Query timed out after 30 seconds</issues>
        </evaluation>
        <suggestions>
            <suggestion>
                <type>optimization</type>
                <description>Add LIMIT clause to reduce result set</description>
            </suggestion>
            <suggestion>
                <type>optimization</type>
                <description>Add WHERE clause to filter data earlier</description>
            </suggestion>
        </suggestions>
    </execution>
    """
    
    @classmethod
    def get_response(cls, agent_type: str, scenario: str) -> str:
        """Get mock response for specific agent and scenario"""
        responses = {
            "query_analyzer": {
                "simple_select": cls.SIMPLE_SELECT_ANALYSIS,
                "complex_decomposition": cls.COMPLEX_DECOMPOSITION,
                "join_query": cls.JOIN_QUERY_ANALYSIS,
            },
            "schema_linking": {
                "basic": cls.BASIC_SCHEMA_LINK,
                "complex": cls.COMPLEX_SCHEMA_LINK,
            },
            "sql_generator": {
                "simple": cls.SIMPLE_SQL_GENERATION,
                "complex_with_children": cls.COMPLEX_SQL_WITH_CHILDREN,
                "join": cls.JOIN_SQL_GENERATION,
            },
            "sql_executor": {
                "success": cls.EXECUTION_SUCCESS,
                "error": cls.EXECUTION_ERROR,
                "timeout": cls.EXECUTION_TIMEOUT,
            }
        }
        
        return responses.get(agent_type, {}).get(scenario, "")
    
    @classmethod
    def create_mock_llm(cls, agent_type: str, scenario: str):
        """Create a mock LLM function for testing"""
        def mock_llm(prompt: str) -> str:
            return cls.get_response(agent_type, scenario)
        return mock_llm