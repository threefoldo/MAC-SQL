"""
Context Understanding Agent that analyzes user queries against database schemas.
Refactored to follow the FileSurfer pattern with separated prompts and data.
"""
import autogen
import xml.etree.ElementTree as ET
import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

try:
    from .config import ContextUnderstandingConfig
    from .prompts import SYSTEM_PROMPT, PROCESS_REQUEST_PROMPT, CLARIFICATION_PROMPTS, ERROR_TEMPLATES
    from .schemas import get_schema_xml
except ImportError:
    # Handle standalone usage
    from config import ContextUnderstandingConfig
    from prompts import SYSTEM_PROMPT, PROCESS_REQUEST_PROMPT, CLARIFICATION_PROMPTS, ERROR_TEMPLATES
    from schemas import get_schema_xml


class ContextUnderstandingAgent:
    """Context Understanding Agent that analyzes user queries against database schemas."""
    
    def __init__(self, config: Optional[ContextUnderstandingConfig] = None):
        """
        Initialize the Context Understanding Agent.
        
        Args:
            config: Configuration object, uses defaults if not provided
        """
        self.config = config or ContextUnderstandingConfig()
        
        # Set up logging
        logging.basicConfig(level=getattr(logging, self.config.log_level))
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Create the AutoGen assistant agent
        self.agent = autogen.AssistantAgent(
            name=self.config.name,
            llm_config=self.config.llm_config,
            system_message=SYSTEM_PROMPT
        )
        
        # Register the tool for the agent
        self._register_tools()
        
        # Track conversation state
        self.conversation_history: List[Dict] = []
        self.clarification_count: int = 0
    
    def _register_tools(self) -> None:
        """Register tools for the agent."""
        tool_config = self.config.tools_config.get("read_database_schema", {})
        
        # Register for LLM
        self.agent.register_for_llm(
            name="read_database_schema",
            description=tool_config.get("description", "Read database schema information")
        )(self.read_database_schema)
        
        # Register for execution
        self.agent.register_for_execution(
            name="read_database_schema"
        )(self.read_database_schema)
    
    def read_database_schema(self, database_id: str) -> str:
        """
        Read database schema information.
        
        Args:
            database_id: Database identifier
            
        Returns:
            XML string with schema information
        """
        try:
            schema_xml = get_schema_xml(database_id)
            self.logger.info(f"Retrieved schema for database: {database_id}")
            return schema_xml
        except Exception as e:
            self.logger.error(f"Failed to retrieve schema for {database_id}: {e}")
            return f"<Error>Schema not found for database: {database_id}</Error>"
    
    def process_request(self, request_xml: str) -> str:
        """
        Process a context understanding request.
        
        Args:
            request_xml: XML string with the request
            
        Returns:
            XML string with the response
        """
        try:
            # Parse the input XML
            root = ET.fromstring(request_xml)
            user_query = root.find('UserQuery').text
            database_id = root.find('DatabaseID').text
            
            if not user_query or not database_id:
                return self._create_error_response(
                    ERROR_TEMPLATES["missing_field"].format(
                        field="UserQuery or DatabaseID"
                    )
                )
            
            # Get previous clarifications if any
            previous_clarifications = self._parse_clarifications(root)
            
            # Create message for the agent
            message = PROCESS_REQUEST_PROMPT.format(
                user_query=user_query,
                database_id=database_id,
                previous_clarifications=previous_clarifications
            )
            
            # Log the request
            self.logger.info(f"Processing query: {user_query} for database: {database_id}")
            
            # Get response from agent
            response = self._simulate_agent_response(user_query, database_id, previous_clarifications)
            
            # Validate XML if configured
            if self.config.validate_xml_output:
                try:
                    ET.fromstring(response)
                except ET.ParseError as e:
                    self.logger.error(f"Invalid XML response: {e}")
                    return self._create_error_response(
                        ERROR_TEMPLATES["invalid_xml"].format(error=str(e))
                    )
            
            # Update conversation history
            self.conversation_history.append({
                "request": request_xml,
                "response": response
            })
            
            return response
            
        except ET.ParseError as e:
            return self._create_error_response(
                ERROR_TEMPLATES["invalid_xml"].format(error=str(e))
            )
        except Exception as e:
            self.logger.error(f"Error processing request: {e}")
            return self._create_error_response(str(e))
    
    def _parse_clarifications(self, root: ET.Element) -> List[Tuple[str, str]]:
        """Parse previous clarifications from the request."""
        previous_clarifications = []
        clarifications_elem = root.find('PreviousClarifications')
        
        if clarifications_elem is not None:
            for interaction in clarifications_elem.findall('Interaction'):
                question = interaction.find('QuestionToUser')
                response = interaction.find('UserResponse')
                if question is not None and response is not None:
                    previous_clarifications.append(
                        (question.text, response.text)
                    )
        
        return previous_clarifications
    
    def _create_error_response(self, error_message: str) -> str:
        """Create a standard error response."""
        return f"""<ContextUnderstandingResponse>
    <Status>Error</Status>
    <ErrorDetails>{error_message}</ErrorDetails>
</ContextUnderstandingResponse>"""
    
    def _simulate_agent_response(
        self, 
        user_query: str, 
        database_id: str, 
        previous_clarifications: List[Tuple[str, str]]
    ) -> str:
        """
        Simulate agent response for demonstration.
        In actual implementation, this would use AutoGen's conversation mechanism.
        """
        # First, call read_database_schema
        schema = self.read_database_schema(database_id)
        
        # Analyze the query
        confidence = self._calculate_confidence(user_query, database_id)
        clarification = self._generate_clarification(user_query, database_id, confidence)
        
        # Generate structured response based on query analysis
        structured_query = self._analyze_query(user_query, database_id)
        
        # Build response
        response_parts = ['<ContextUnderstandingResponse>']
        
        if self.config.include_schema_in_response:
            response_parts.append(f'    {schema}')
        
        response_parts.extend([
            '    <StructuredQuery>',
            structured_query,
            f'        <Confidence>{confidence}</Confidence>',
            '    </StructuredQuery>',
            '    <Status>Success</Status>'
        ])
        
        if clarification:
            response_parts.append(
                f'    <ClarificationQuestionIfAny>{clarification}</ClarificationQuestionIfAny>'
            )
        
        response_parts.append('</ContextUnderstandingResponse>')
        
        return '\n'.join(response_parts)
    
    def _calculate_confidence(self, user_query: str, database_id: str) -> float:
        """Calculate confidence score for query understanding."""
        # Simple heuristic: more specific queries get higher confidence
        query_lower = user_query.lower()
        
        # Check for specific tables/columns mentioned
        specific_terms = 0
        
        if database_id == "california_schools":
            if any(term in query_lower for term in ["charter", "frpm", "sat", "scores"]):
                specific_terms += 1
            if any(term in query_lower for term in ["alameda", "county", "district"]):
                specific_terms += 1
        elif database_id == "financial":
            if any(term in query_lower for term in ["transaction", "account", "amount"]):
                specific_terms += 1
            if any(term in query_lower for term in ["500", "greater", "over"]):
                specific_terms += 1
        
        # Base confidence
        confidence = 0.7
        
        # Increase confidence for specific terms
        confidence += specific_terms * 0.1
        
        # Cap at 0.95
        return min(confidence, 0.95)
    
    def _generate_clarification(
        self, 
        user_query: str, 
        database_id: str, 
        confidence: float
    ) -> Optional[str]:
        """Generate clarification question if needed."""
        if confidence >= self.config.confidence_threshold:
            return None
        
        if self.clarification_count >= self.config.max_clarifications:
            return None
        
        # Simple clarification logic
        query_lower = user_query.lower()
        
        if "schools" in query_lower and "what" not in query_lower and "which" not in query_lower:
            return CLARIFICATION_PROMPTS["unclear_aggregation"]
        
        if database_id == "california_schools" and "charter" in query_lower:
            return "What specific information about charter schools would you like to see?"
        
        return CLARIFICATION_PROMPTS["missing_condition"].format(entity="criteria")
    
    def _analyze_query(self, user_query: str, database_id: str) -> str:
        """Analyze query and generate structured representation."""
        # This is a simplified analysis - in production, the LLM would do this
        query_lower = user_query.lower()
        
        entities = []
        conditions = []
        requested_data = []
        
        if database_id == "california_schools":
            if "eligible free rate" in query_lower:
                entities.extend([
                    '<Entity type="table">frpm</Entity>',
                    '<Entity type="column">Free Meal Count (K-12)</Entity>',
                    '<Entity type="column">Enrollment (K-12)</Entity>'
                ])
                requested_data.append(
                    '<Field source_column="frpm.Free Meal Count (K-12) / frpm.Enrollment (K-12)" as="eligible_free_rate"/>'
                )
            
            if "alameda" in query_lower:
                conditions.append('''<Condition text="County is Alameda">
                <Column>frpm.County Name</Column>
                <Operator>=</Operator>
                <Value>\'Alameda\'</Value>
            </Condition>''')
        
        elif database_id == "financial":
            if "transaction" in query_lower:
                entities.append('<Entity type="table">trans</Entity>')
                
                if "500" in query_lower or "over" in query_lower:
                    entities.append('<Entity type="column">amount</Entity>')
                    conditions.append('''<Condition text="Amount greater than 500">
                <Column>trans.amount</Column>
                <Operator>></Operator>
                <Value>500</Value>
            </Condition>''')
                
                requested_data.extend([
                    '<Field source_column="trans.trans_id" as="TransactionID"/>',
                    '<Field source_column="trans.amount" as="Amount"/>',
                    '<Field source_column="trans.type" as="Type"/>',
                    '<Field source_column="trans.date" as="Date"/>'
                ])
        
        # Build structured query XML
        parts = ['        <IdentifiedEntities>']
        parts.extend(f'            {entity}' for entity in entities)
        parts.append('        </IdentifiedEntities>')
        
        parts.append('        <Conditions>')
        parts.extend(f'            {condition}' for condition in conditions)
        parts.append('        </Conditions>')
        
        parts.append('        <RequestedData>')
        parts.extend(f'            {field}' for field in requested_data)
        parts.append('        </RequestedData>')
        
        return '\n'.join(parts)


# Example usage
if __name__ == "__main__":
    # Create agent with default config
    agent = ContextUnderstandingAgent()
    
    # Example request
    request = """<ContextUnderstandingRequest>
    <UserQuery>What is the eligible free rate for all schools in Alameda county?</UserQuery>
    <DatabaseID>california_schools</DatabaseID>
    <PreviousClarifications/>
</ContextUnderstandingRequest>"""
    
    # Process request
    response = agent.process_request(request)
    print(response)