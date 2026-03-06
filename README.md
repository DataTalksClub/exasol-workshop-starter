# Exasol Workshop Starter

Open this repo in GitHub Codespaces to get started.

## AWS Access Setup

Run this in your Codespace terminal (your instructor will provide the URL and token):

```bash
source setup.sh '<endpoint_url>' '<token>'
```

Then verify:

```bash
aws sts get-caller-identity
```

Credentials refresh automatically — no further action needed.
