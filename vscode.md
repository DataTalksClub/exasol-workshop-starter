# Connecting from VS Code

## Install the Exasol extension

```bash
wget https://github.com/exasol-labs/exasol-vscode/releases/download/v1.1.2/exasol-vscode-1.1.2.vsix
code --install-extension exasol-vscode-1.1.2.vsix
```

The latest version is available at https://github.com/exasol-labs/exasol-vscode/releases/.

## Get connection details

If you're not using the Codespace (where `code/` is already set up), download the script:

```bash
mkdir -p code/utils && cd code
uv init
touch utils/__init__.py
PREFIX=https://raw.githubusercontent.com/alexeygrigorev/exasol-workshop-starter/main/reference
wget ${PREFIX}/utils/connection_info.py -O utils/connection_info.py
```

Run it:

```bash
uv run python utils/connection_info.py
```

This prints everything you need: host, port, username, password, and the TLS certificate fingerprint.

## Add the connection

Use the connection details from `connection_info.py` to add a connection in the Exasol extension. For TLS validation, select "Fingerprint (pin certificate)" and paste the fingerprint from the output.
