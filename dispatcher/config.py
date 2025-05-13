import autogen

config_list = autogen.config_list_from_json(
    "OAI_CONFIG_LIST", # Your OAI_CONFIG_LIST.json or environment variable
    filter_dict={"model": ["gpt-4o"]}, # Or your preferred models
)

llm_config = {
    "config_list": config_list,
    "cache_seed": 42, # For reproducibility
    "temperature": 0,
}