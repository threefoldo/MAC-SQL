import autogen
import xml.etree.ElementTree as ET # For parsing in Dispatcher
import json # Still useful for internal representation or simple data in prompts
from .config import llm_config

# Import the QueryUnderstandingAgent from its separate file
from .query_understanding_agent import query_understanding_agent

# --- SQLGenerationAgent (Creator & Executor) ---
sql_generation_agent = autogen.AssistantAgent(
    name="SQLGenerationAgent",
    llm_config=llm_config,
    system_message="""You are the SQLGenerationAgent. Your role is to generate and execute SQL queries.
1.  Receive a 'todo_item_description', 'relevant_schema_info' (as XML string), 'input_context_from_previous_steps' (as XML string if any),
    and optionally 'feedback_from_validator' and 'previous_sql_attempt'.
2.  Based on these inputs, generate an accurate SQL query for the 'todo_item_description'.
    If feedback is provided, use it to correct your previous attempt.
3.  Use the 'execute_sql_and_return_output' tool to run your generated SQL query.
    You will receive the tool output as a string (likely a stringified Python dictionary representing the result or error).
    You MUST format this execution information into the <executionResult> or <executionErrorMessage> XML structure described below.
4.  Output your response as a single well-formed XML string with the root tag <sqlGenerationResponse>.
    The XML structure MUST be:
    <sqlGenerationResponse>
        <generatedSql>YOUR SQL QUERY</generatedSql>
        <executionResult> <!-- Present if execution was successful, even if no rows returned -->
            <result>
                <rows>
                    <row>
                        <column name="col1_name">value1</column>
                        <column name="col2_name">value2</column>
                        <!-- more column tags per row -->
                    </row>
                    <!-- more row tags if multiple results -->
                </rows>
                <rowCount>number_of_rows_returned</rowCount>
            </result>
        </executionResult>
        <executionErrorMessage>Error message if execution failed. If no error, this tag can be empty or omitted.</executionErrorMessage>
        <statusSummary>Brief summary like 'SQL generated and executed successfully' or 'Execution resulted in an error'.</statusSummary>
    </sqlGenerationResponse>

If SQL generation itself fails, <generatedSql> can be empty, and <statusSummary> should explain why.
If execution fails, <executionResult> should be empty or omitted, and <executionErrorMessage> MUST contain the error.
If execution is successful but returns no rows, <executionResult><result><rows/></result><rowCount>0</rowCount></executionResult> is appropriate.
Ensure no extra text outside the XML tags.
""",
)

# --- SQLValidationAgent (Analyzer & Feedback Provider) ---
sql_validation_agent = autogen.AssistantAgent(
    name="SQLValidationAgent",
    llm_config=llm_config,
    system_message="""You are the SQLValidationAgent. Your role is to analyze a generated SQL query, its execution result (or error),
in the context of the original 'todo_item_description' and the 'overall_nl_query'.
Inputs ('generated_sql_for_item', 'item_execution_result_xml', 'item_execution_error_xml',
'todo_item_description', 'original_overall_nl_query', 'relevant_schema_info_xml') will be provided.

1.  Analyze the inputs:
    - If 'item_execution_error_xml' is present: Is it syntax? Runtime? Suggest a fix.
    - If 'item_execution_result_xml' is present: Does the SQL logically address the description? Is the result plausible?
2.  Output your response as a single well-formed XML string with the root tag <sqlValidationResponse>.
    The XML structure MUST be:
    <sqlValidationResponse>
        <isItemValidAndCorrect>true</isItemValidAndCorrect> <!-- or false -->
        <feedbackForGenerator>Specific suggestions for SQLGenerationAgent if not valid/correct. Empty if valid.</feedbackForGenerator>
        <validationSummary>Brief summary of your validation findings.</validationSummary>
    </sqlValidationResponse>
Ensure no extra text outside the XML tags. Your feedback should be constructive and actionable.
""",
)
