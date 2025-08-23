# Bronto MCP Server

This project contains an MCP server for Bronto. 

It currently provides tools for models to access details on Bronto datasets, top-keys as well as event and statistical 
searches. When these tools are provided to an AI agent, they make it possible to answer questions such as

_Can you please provide some log events from datasets in the ingestion collection, except for the ones related to garbage collection? The data should be between "2025-05-10 16:05:45" and "2025-05-10 16:25:05"._


## Installation

Check out this project, then create a virtual environment at its root. Finally install the requirements, e.g.

```shell
python3 -m venv env
env/bin/pip install -r requirements.txt
```

## Configuration

This MCP server is configured using environment variables:

- `BRONTO_API_KEY`: a Bronto API key
- `BRONTO_API_ENDPOINT`: a Bronto API endpoint, e.g. https://api.eu.staging.bronto.io


## Usage

To run the mcp server from the root of this project, you can use the following command:
```shell
BRONTO_API_KEY=<API KEY HERE> \
BRONTO_API_ENDPOINT=https://api.eu.bronto.io \
PYTHONPATH=src/main/ \
env/bin/python src/main/brmcpserver/main.py
```

This MCP server should work with any agent that supports MCP. However, it has only been tested with Claude Code.

Finally, in order to configure Claude Code so that it uses the Bronto MCP server, simply run 
```json
claude mcp add --transport http bronto http://localhost:8000
```
Note: this will make the Bronto MCP server available to your current project/folder. If you wish to make the server 
available globally, simply add `--scope user` to the command above. Details on how to manage MCP servers with 
Claude Code can be found at https://docs.anthropic.com/en/docs/claude-code/mcp

Then launch Claude Code and run the `/mcp` command to check that the Bronto MCP server is available and that Claude Code 
was able to connect to it. 
