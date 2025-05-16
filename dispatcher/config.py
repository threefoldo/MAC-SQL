import os
import autogen

# Get API key from environment variable
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("No OpenAI API key found. Please set OPENAI_API_KEY environment variable.")

# Create config list with the API key
config_list = [
    {
        "model": "gpt-4o",
        "api_key": api_key
    },
]

llm_config = {
    "config_list": config_list,
    "cache_seed": 42,  # For reproducibility
    "temperature": 0,
}