# Bronto MCP Server

This project contains a P.O.C MCP server for Bronto. 

It currently provides 3 tools 

- `get_logs`: retrieves log data based on a timerange and a list of log IDs
- `get_datasets`: retrieves datasets details for the configured account
- `get_timestamp_as_unix_epoch`: converts dates such as `2025-05-10 16:25:05` to a unix timestamp in milliseconds

When these tools are provided to an AI agent, they make it possible to answer questions such as

_Can you please provide some log events from datasets in the ingestion collection, except for the ones related to garbage collection? The data should be between "2025-05-10 16:05:45" and "2025-05-10 16:25:05"._


## Installation

Create a virtual environment at the root of the project and install the requirements, e.g.

```shell
python3 -m venv env
env/bin/pip install -r requirements.txt
```

## Configuration

This MCP server can be configured using environment variables:

- `BRONTO_API_KEY`: a Bronto API key
- `BRONTO_API_ENDPOINT`: a Bronto API endpoint, e.g. https://api.eu.staging.bronto.io


## Usage

This MCP server should work with any agent that supports MCP. However, it has only been tested with the Amazon Bedrock Client: https://github.com/aws-samples/amazon-bedrock-client-for-mac

Note: if `~/.aws/config` is present on your system and `~/.aws/credentials` is not, AWS Bedrock client will show an error at startup and will not be able to connect to AWS.
One way to overcome the issue is to create `~/.aws/credentials` and to add a profile to it. The profile definition can be obtained from IAM Identity Centre (Option 2 when selecting `Access Keys`, i.e. `Option 2: Add a profile to your AWS credentials file`)
After restarting the Bedrock client,, you might need to select the profile to use in the Bedrock Client settings.

Finally, in order to configure the client so that it uses the Bronto MCP server, first make sure that `Enable MCP` is selected, in the `Developer` section of the settings. Then select `Open Config File` and use a configuration similar to the following one:
```json
{ "mcpServers": {
    "bronto": {
        "command": "/PATH/TO/bronto-mcp-server/env/bin/python",
        "args": [
            "/PATH/TO/bronto-mcp-server/src/main/brmcpserver/main.py"
        ],
        "env": {
            "PYTHONPATH": "/PATH/TO/bronto-mcp-server/src/main/brmcpserver/",
            "BRONTO_API_KEY": "*******",
            "BRONTO_API_ENDPOINT": "https://api.eu.staging.bronto.io"
          }
    }
    }
 }
```

Restarting the client will probably be needed at this point. After it restarts, the tools exposed by the MCP server 
should be listed when clicking on the `+` sign at the bottom of the client window.

You can test that the Bronto MCP server can be used by the client by asking a question such as:

```
Can you please provide some log events from datasets in the ingestion collection, except for the ones related to garbage collection? The data should be between "2025-05-10 16:05:45" and "2025-05-10 16:25:05".
```

The output should make it clear that the client is retrieving data from Bronto.
