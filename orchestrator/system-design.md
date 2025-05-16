**Core Principles Applied:**

1.  **Single Responsibility:** Each agent focuses on a distinct phase of the text-to-SQL process.
2.  **Decoupling & Minimized Explicit Communication:** Agents will primarily interact via the Orchestrator passing structured data. However, agents themselves can use shared tools (`SQLExecution`, `ReadDatabaseSchema`) directly, reducing the need for the Orchestrator to mediate these specific low-level calls. The "context" is primarily what's passed as input to an agent for its current task, and its own internal state.
3.  **Internal State & Iteration:** Agents can maintain an XML-based internal state to manage their iterative processes. For example, `SQLGenerationAgent` can generate multiple SQLs, test them using the `SQLExecution` tool, and refine its choices before outputting the best one.
4.  **XML for State and Agent I/O:** All inputs, outputs, and complex internal states will be represented in XML.
5.  **Shared Tools:** Functions like `SQLExecution` and `ReadDatabaseSchema` will be available to agents that need them.

**Shared Tools (Python Functions Registered with AutoGen Agents):**

These functions should be designed to be easily callable by an LLM (e.g., through AutoGen's tool registration). Their inputs and outputs should be simple and clear.

  * **`read_database_schema(database_id: str) -> str:`**

      * **Input:** `database_id` (string).
      * **Action:** Connects to the specified database. Fetches table names, column names, column types, primary keys, and foreign key relationships.
      * **Output (XML String):**
        ```xml
        <Schema>
            <DatabaseID>...</DatabaseID>
            <Tables>
                <table>
                    <Name>...</Name>
                    <Columns>
                        <Column>
                            <Name>...</Name>
                            <Type>...</Type>
                            </Column>
                        ...
                    </Columns>
                    <PrimaryKey>...</PrimaryKey>
                </table>
                ...
            </Tables>
            <ForeignKeys>
                <ForeignKey>
                    <FromTable>...</FromTable>
                    <FromColumn>...</FromColumn>
                    <ToTable>...</ToTable>
                    <ToColumn>...</ToColumn>
                </ForeignKey>
                ...
            </ForeignKeys>
        </Schema>
        ```
        Or, if an error occurs:
        ```xml
        <Error>
            <FunctionCalled>read_database_schema</FunctionCalled>
            <Message>Database not found or connection error.</Message>
        </Error>
        ```

  * **`execute_sql(database_id: str, sql_query: str) -> str:`**

      * **Input:** `database_id` (string), `sql_query` (string).
      * **Action:** Connects to the database and executes the SQL query.
      * **Output (XML String):**
        ```xml
        <SQLExecutionResult>
            <DatabaseID>...</DatabaseID>
            <SQLQuery>...</SQLQuery>
            <Status>Success</Status> <Data> <Row>
                    <Column name="col1_name">value1</Column>
                    <Column name="col2_name">value2</Column>
                </Row>
                <Row>...</Row>
            </Data>
            <RowCount>2</RowCount> <ErrorMessage></ErrorMessage> <ErrorClass></ErrorClass> </SQLExecutionResult>
        ```

**Agent Designs:**

**1. Orchestrator Agent (`OrchestratorAgent`)**

  * **Responsibility:** Manages the overall workflow, user interaction (initial query, final result, clarifications), and high-level state. Decides which agent to call for major steps.
  * **Input (from User):**
      * Natural language query (text).
      * Database identifier (text).
  * **Output (to User):**
      * Final SQL query (XML with query and possibly execution preview).
      * Clarification questions (XML).
      * Error messages (XML).
  * **Internal State (XML):**
    ```xml
    <OrchestratorState>
        <UserQuery>...</UserQuery>
        <DatabaseID>...</DatabaseID>
        <CurrentOverallStep>ContextUnderstanding</CurrentOverallStep> <SchemaInfo>...</SchemaInfo> <StructuredQueryUnderstanding>...</StructuredQueryUnderstanding> <GeneratedSQL>...</GeneratedSQL> <RefinedSQL>...</RefinedSQL> <ExecutionOutcome>...</ExecutionOutcome> <ConversationHistory>
            <Turn>
                <Agent>...</Agent>
                <Input>...</Input>
                <Output>...</Output>
            </Turn>
        </ConversationHistory>
        <LastError>...</LastError>
    </OrchestratorState>
    ```
  * **Tool Use:** Primarily calls other agents. XML parser/generator.
  * **Workflow Logic (Simplified):**
    1.  Receive user input. Initialize state.
    2.  Call `ContextUnderstandingAgent`.
    3.  Process response:
          * If clarification needed, present to user and await response. Then re-call `ContextUnderstandingAgent` with additional info.
          * If successful, store schema and structured query. Call `SQLGenerationAgent`.
    4.  Process `SQLGenerationAgent` response:
          * If successful SQL, (optional) Orchestrator might call `execute_sql` one final time or present SQL.
          * If `SQLGenerationAgent` indicates an error it can't resolve or produces a low-confidence SQL, consider calling `SQLRefinementAgent`.
    5.  Process `SQLRefinementAgent` response.
    6.  Present final result or error to user.

**2. Context Understanding Agent (`ContextUnderstandingAgent`)** (Merged Schema & Decomposer)

  * **Responsibility:** Understand the user's query in the context of the relevant database schema. Produces a structured representation of the query's intent.
  * **Input (XML from Orchestrator):**
    ```xml
    <ContextUnderstandingRequest>
        <UserQuery>...</UserQuery>
        <DatabaseID>...</DatabaseID>
        <PreviousClarifications> <Interaction>
                <QuestionToUser>...</QuestionToUser>
                <UserResponse>...</UserResponse>
            </Interaction>
        </PreviousClarifications>
    </ContextUnderstandingRequest>
    ```
  * **Output (XML to Orchestrator):**
    ```xml
    <ContextUnderstandingResponse>
        <Schema>
            </Schema>
        <StructuredQuery>
            <IdentifiedEntities>
                <Entity type="table">Customers</Entity>
                <Entity type="column">OrderDate</Entity>
            </IdentifiedEntities>
            <Conditions>
                <Condition text="City is 'New York'">
                    <Column>Customers.City</Column>
                    <Operator>=</Operator>
                    <Value>'New York'</Value>
                </Condition>
            </Conditions>
            <RequestedData>
                <Field source_column="Customers.Name" as="CustomerName"/>
            </RequestedData>
            <Confidence>0.9</Confidence>
        </StructuredQuery>
        <Status>Success</Status> <ClarificationQuestionIfAny>Which date range for 'last year'?</ClarificationQuestionIfAny>
        <ErrorDetails>...</ErrorDetails>
    </ContextUnderstandingResponse>
    ```
  * **Internal State (XML, for iterative understanding if needed, though less common here than in SQL gen/refine):**
    ```xml
    <ContextUnderstandingInternalState>
        <Request>...</Request>
        <SchemaFetchAttemptCount>0</SchemaFetchAttemptCount>
        <CurrentInterpretation>...</CurrentInterpretation>
        <AmbiguityScore>0.1</AmbiguityScore> </ContextUnderstandingInternalState>
    ```
  * **Tool Use (Directly callable by LLM within this agent):**
      * `read_database_schema(database_id: str)`
      * LLM for query parsing, entity extraction, condition identification, ambiguity detection.
      * XML parser/generator.
  * **Internal Logic:**
    1.  LLM decides to call `read_database_schema` with `DatabaseID`.
    2.  LLM analyzes `UserQuery` against the returned schema.
    3.  LLM constructs the `<StructuredQuery>` part.
    4.  If ambiguities arise that the LLM cannot resolve, it formulates a `<ClarificationQuestionIfAny>`.
    5.  The agent might iterate internally if the first interpretation is poor, perhaps by re-prompting its internal LLM with different strategies before deciding it needs external clarification.

**3. SQL Generation Agent (`SQLGenerationAgent`)**

  * **Responsibility:** Generate one or more SQL queries based on the structured understanding and schema. Validate and select the best SQL query.
  * **Input (XML from Orchestrator):**
    ```xml
    <SQLGenerationRequest>
        <DatabaseID>...</DatabaseID>
        <Schema> ... </Schema>
        <StructuredQuery> ... </StructuredQuery>
    </SQLGenerationRequest>
    ```
  * **Output (XML to Orchestrator):**
    ```xml
    <SQLGenerationResponse>
        <BestSQLQuery>SELECT Name FROM Customers WHERE City = 'New York';</BestSQLQuery>
        <Confidence>0.95</Confidence>
        <ExecutionPreviewIfValid> </ExecutionPreviewIfValid>
        <AlternativeValidSQLs> <SQL>...</SQL>
        </AlternativeValidSQLs>
        <Status>Success</Status> <ErrorDetails>Unable to form a query linking sales to product categories directly.</ErrorDetails>
        <GenerationLog>
            <Attempt>
                <GeneratedSQL>...</GeneratedSQL>
                <ValidationResult> </ValidationResult>
            </Attempt>
        </GenerationLog>
    </SQLGenerationResponse>
    ```
  * **Internal State (XML):**
    ```xml
    <SQLGenerationInternalState>
        <Request>...</Request>
        <CandidateSQLs>
            <Candidate id="1">
                <Query>...</Query>
                <IsSyntacticallyValid>Unknown</IsSyntacticallyValid> <ExecutionResult> <Status>Pending</Status>
                </ExecutionResult>
                <Score>0</Score>
            </Candidate>
        </CandidateSQLs>
        <CurrentBestCandidateID></CurrentBestCandidateID>
        <Attempts>0</Attempts>
        <MaxAttempts>5</MaxAttempts>
    </SQLGenerationInternalState>
    ```
  * **Tool Use (Directly callable by LLM within this agent):**
      * `execute_sql(database_id: str, sql_query: str)`
      * (Optional) `check_sql_syntax(sql_query: str) -> bool`
      * LLM for SQL generation, ranking candidates.
      * XML parser/generator.
  * **Internal Logic (Iterative):**
    1.  LLM generates an initial SQL candidate based on input.
    2.  LLM decides to call `execute_sql` for the candidate.
    3.  Update internal state with execution results. Score the candidate.
    4.  If SQL is valid and results look plausible (LLM might assess this), it could be the `CurrentBestCandidateID`.
    5.  If SQL is invalid or results are empty/unexpected, and `Attempts < MaxAttempts`:
          * LLM analyzes the error/result.
          * LLM generates a new, revised SQL candidate (self-correction). Increment `Attempts`. Go to step 2.
    6.  After iterations, select the best valid SQL from `CandidateSQLs`.

**4. SQL Refinement Agent (`SQLRefinementAgent`)**

  * **Responsibility:** Takes a potentially faulty SQL query (e.g., from `SQLGenerationAgent` if it couldn't fully resolve an issue, or if an externally run SQL failed) and attempts to correct it using schema information and error messages.
  * **Input (XML from Orchestrator):**
    ```xml
    <SQLRefinementRequest>
        <DatabaseID>...</DatabaseID>
        <Schema>...</Schema>
        <OriginalUserQuery>...</OriginalUserQuery> <FaultySQL>...</FaultySQL>
        <ExecutionError> <ErrorMessage>...</ErrorMessage>
            <ErrorClass>...</ErrorClass>
        </ExecutionError>
    </SQLRefinementRequest>
    ```
  * **Output (XML to Orchestrator):**
    ```xml
    <SQLRefinementResponse>
        <RefinedSQL>...</RefinedSQL>
        <Confidence>0.8</Confidence>
        <RefinementLog>Changed column 'CName' to 'CustomerName' based on schema and error.</RefinementLog>
        <ExecutionPreviewIfValid> </ExecutionPreviewIfValid>
        <Status>Success</Status> <ErrorDetails>Could not resolve the ambiguity in table join.</ErrorDetails>
    </SQLRefinementResponse>
    ```
  * **Internal State (XML):**
    ```xml
    <SQLRefinementInternalState>
        <Request>...</Request>
        <RefinementAttempts>
            <Attempt>
                <ProposedSQL>...</ProposedSQL>
                <ExecutionResult>...</ExecutionResult> </Attempt>
        </RefinementAttempts>
        <CurrentBestRefinedSQL></CurrentBestRefinedSQL>
        <IterationCount>0</IterationCount>
        <MaxIterations>3</MaxIterations>
    </SQLRefinementInternalState>
    ```
  * **Tool Use (Directly callable by LLM within this agent):**
      * `execute_sql(database_id: str, sql_query: str)`
      * LLM for analyzing errors and proposing SQL modifications.
      * XML parser/generator.
  * **Internal Logic (Iterative):**
    1.  LLM analyzes `FaultySQL` and `ExecutionError` in context of `Schema` and `OriginalUserQuery`.
    2.  LLM generates a `ProposedSQL`.
    3.  LLM decides to call `execute_sql` for the `ProposedSQL`.
    4.  Update internal state.
    5.  If refined SQL is valid, it becomes `CurrentBestRefinedSQL`.
    6.  If still errors and `IterationCount < MaxIterations`, LLM tries a different refinement strategy. Increment `IterationCount`. Go to step 2.

**AutoGen Implementation:**

  * Each agent would be an `autogen.AssistantAgent` (or a custom class inheriting from it).
  * The shared Python functions (`read_database_schema`, `execute_sql`) would be registered with the agents that need them using `agent.register_function(tool_map={...})`.
  * The Orchestrator could be a `UserProxyAgent` if it primarily relays user input and final output, or another `AssistantAgent` if it has more complex logic.
  * The "internal state" XML would be managed by each agent within its message handling logic. When an agent "speaks" or "replies," its output XML (as defined above) is sent. If it needs to call itself or iterate, it can do so by managing its state and re-triggering its LLM calls with updated prompts reflecting the current state of its internal XML.

This revised design enhances agent autonomy and leverages AutoGen's tool-use capabilities effectively, adhering to the principles of single responsibility and decoupled operation while allowing for robust, iterative problem-solving within each agent.