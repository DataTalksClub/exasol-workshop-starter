# Exasol Workshop Starter

Open this repo in GitHub Codespaces to get started with AWS access.

## Getting Started

1. Open this repo in a Codespace (click **Code → Codespaces → Create codespace on main**)
2. When the terminal opens, you'll be prompted for a passphrase
3. Enter the passphrase shared by your instructor
4. AWS access is now configured — verify with:

```bash
aws sts get-caller-identity
```

Credentials refresh automatically in the background. No keys to manage.

## If the prompt didn't appear

Run manually:

```bash
bash .devcontainer/setup-aws.sh
```

## Instructor Setup

### Prerequisites

- AWS account with a deployed credential vending stack (see `infra/` in the main repo)
- The Lambda Function URL and workshop token from the stack output

### 1. Encrypt the token

The passphrase is stored in `.env` as `SECRET`. Use it to encrypt the workshop token:

```bash
source .env
./encrypt-token.sh '<workshop-token-from-deploy>' "$SECRET"
```

### 2. Update devcontainer.json

Edit `.devcontainer/devcontainer.json` and replace the placeholders in `containerEnv`:

- `WORKSHOP_CRED_URL` → the Lambda Function URL
- `WORKSHOP_TOKEN_ENC` → the encrypted token from step 1

Commit and push. The encrypted token is safe to commit — it's useless without the passphrase.

### 3. Share the passphrase

Tell participants the passphrase during the workshop. Without it, the encrypted token is useless.
