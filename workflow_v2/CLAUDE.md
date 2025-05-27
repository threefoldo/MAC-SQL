# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Structure (workflow_v2)
- **src/**: Contains all the latest production code. **ALWAYS search and modify files in this directory when generating or updating code.**
- **docs/**: Contains optional documentation, legacy code, and reference materials. Only use when explicitly requested by users.
- **nbs/**: Contains Jupyter notebooks for exploratory programming and idea testing. Not for production code.
- **tests/**: Contains all test files and test-related code.

## LLM Integration Guidelines
- **Always use XML instead of JSON**: XML is more robust for partial outputs. LLMs may not always generate complete responses, and partial XML is easier to parse than partial JSON
- **Prefer LLM instructions over code**: Operations should be performed through LLM instructions whenever possible. Code should primarily:
  - Prepare context for the LLM
  - Parse LLM output
  - Handle infrastructure (database connections, file I/O, etc.)
  - No operational logic should be hard-coded unless absolutely necessary
- **XML parsing pattern**: Use regex to extract XML blocks first, then parse them:
  ```python
  xml_match = re.search(r'<root>.*?</root>', content, re.DOTALL)
  if xml_match:
      xml_content = xml_match.group()
      root = ET.fromstring(xml_content)
  ```
- **Prompt Design Philosophy**: Put rules and logics in prompts, let the code process the structured data and deterministic states

## Development Environment
- Get API key by adding this command "source ../.env && export OPENAI_API_KEY"

## Runtime Assumptions
- Assume the Jupyter notebook server is always running

## Version Control and Workflow
- Always skip .ipynb_checkpoints directory