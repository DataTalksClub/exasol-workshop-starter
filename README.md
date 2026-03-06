# Exasol Workshop Starter

Open this repo in GitHub Codespaces to get started with AWS access.

## For Participants

### 1. Set the passphrase

Your instructor will share the passphrase during the workshop.

Via the web UI:

1. Go to [github.com/settings/codespaces](https://github.com/settings/codespaces) → New secret
2. Name: `WORKSHOP_PASSPHRASE`
3. Value: the passphrase from your instructor
4. Repository access: select `alexeygrigorev/exasol-workshop-starter`

Or via the CLI:

```bash
gh secret set WORKSHOP_PASSPHRASE --user --repos alexeygrigorev/exasol-workshop-starter --app codespaces
# paste the passphrase when prompted
```

### 2. Create a Codespace

Via the web UI: go to the repo page → Code → Codespaces → Create codespace on main

Or via the CLI:

```bash
gh codespace create --repo alexeygrigorev/exasol-workshop-starter --branch main --machine basicLinux32gb
gh codespace ssh  # or open in VS Code
```

AWS access is configured automatically. Verify:

```bash
aws sts get-caller-identity
```

### If it didn't work

If you created the Codespace before setting the secret, either rebuild it
(`Cmd/Ctrl+Shift+P` → "Rebuild Container") or run manually:

```bash
bash .devcontainer/setup-aws.sh
```

Enter the passphrase when prompted. Then open a new terminal or run `source ~/.bashrc`.

### Troubleshooting

- **Codespace created before setting the secret?** Rebuild it: `Cmd/Ctrl+Shift+P` → "Rebuild Container"
- **"Wrong passphrase"?** Double-check with your instructor
- **Permission errors on AWS?** Ask your instructor — the role may need updated permissions

Credentials refresh automatically in the background. No keys to manage.
