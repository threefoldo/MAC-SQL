"""
DSPy Models for Text-to-SQL
This module contains the custom language model implementation for Google Gemini
and base DSPy modules for the Text-to-SQL system.
"""

import os
import logging
from google import genai
from google.genai import types
import dspy

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configure Gemini API key
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")


class GeminiProLM(dspy.LM):
    """Language model implementation for Google Gemini Pro"""

    def __init__(self, model="gemini-2.0-flash", temperature=0.1):
        """Initialize the Gemini Pro LM with specified model and temperature"""
        self.model = model
        self.temperature = temperature

        # Store kwargs to make it compatible with DSPy's expectations
        self.kwargs = {
            "temperature": temperature,
            "model": model,
            "n": 1,  # Default number of generations
            "max_tokens": 8192  # Default max tokens
        }

        # Initialize client
        if not GOOGLE_API_KEY:
            logger.warning("GOOGLE_API_KEY not found in environment variables")
            self.client = None
        else:
            self.client = genai.Client(api_key=GOOGLE_API_KEY)
    
    def basic_request(self, prompt, **kwargs):
        """Make a basic request to the Gemini API using the new client-based approach"""
        if not self.client:
            return "Error: API key not configured"
            
        try:
            temp = kwargs.get("temperature", self.temperature)
            
            config = types.GenerateContentConfig(
                temperature=temp,
                max_output_tokens=kwargs.get("max_tokens", 8192),
                top_p=kwargs.get("top_p", 0.95),
                top_k=kwargs.get("top_k", 0)
            )

            response = self.client.models.generate_content(
                model=self.model,
                config=config,
                contents=prompt
            )
            
            return response.text
        except Exception as e:
            logger.error(f"Error calling Gemini API: {e}")
            return "Error generating response"
    
    def __call__(self, messages, **kwargs):
        """Call the LM with the given messages"""
        if not self.client:
            return "Error: API key not configured"

        try:
            if isinstance(messages, list):
                # Handle chat format (list of messages)
                contents = []
                system_instruction = None

                for msg in messages:
                    role = msg["role"]
                    content = msg["content"]

                    # Handle system role as system instruction
                    if role == "system":
                        system_instruction = content
                        continue

                    # Map DSPy roles to Gemini roles
                    gemini_role = "user"
                    if role == "assistant":
                        gemini_role = "model"

                    # For roles other than user/assistant/system, add role prefix
                    text_content = content
                    if role not in ["user", "assistant", "system"]:
                        text_content = f"[{role}]: {content}"

                    contents.append(types.Content(role=gemini_role, parts=[types.Part(text=text_content)]))

                # Create config with or without system instruction
                if system_instruction:
                    config = types.GenerateContentConfig(
                        temperature=kwargs.get("temperature", self.temperature),
                        max_output_tokens=kwargs.get("max_tokens", 8192),
                        top_p=kwargs.get("top_p", 0.95),
                        top_k=kwargs.get("top_k", 0),
                        system_instruction=system_instruction
                    )
                else:
                    config = types.GenerateContentConfig(
                        temperature=kwargs.get("temperature", self.temperature),
                        max_output_tokens=kwargs.get("max_tokens", 8192),
                        top_p=kwargs.get("top_p", 0.95),
                        top_k=kwargs.get("top_k", 0)
                    )

                # Add response format configuration for JSON
                response = self.client.models.generate_content(
                    model=self.model,
                    config=config,
                    contents=contents,
                    generation_config={"response_mime_type": "application/json"}
                )

                # Return the text for DSPy to parse as JSON
                return response.text
            else:
                # Handle single string prompt
                return self.basic_request(messages, **kwargs)
        except Exception as e:
            logger.error(f"Error in GeminiProLM.__call__: {e}")
            return "Error generating response"


# Define signatures for the modules

# Schema Extractor Signature
schema_extractor_signature = dspy.Signature(
    "db_id, query, db_schema, foreign_keys, evidence -> extracted_schema"
)

# SQL Decomposer Signature
sql_decomposer_signature = dspy.Signature(
    "query, schema_info, foreign_keys, evidence -> sub_questions, sql"
)

# SQL Validator Signature
sql_validator_signature = dspy.Signature(
    "query, sql, schema_info, foreign_keys, error_info, evidence -> refined_sql, explanation"
)


def create_gemini_lm():
    """Create and return a GeminiProLM instance"""
    return GeminiProLM()