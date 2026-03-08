# Codespaces Setup

## 1. Fork this repo

Click the "Fork" button at the top right of this page to create your own copy.

## 2. Set the passphrase

Your instructor will share the passphrase during the workshop.

Via the web UI:

1. Go to [github.com/settings/codespaces](https://github.com/settings/codespaces) → New secret
2. Name: `WORKSHOP_PASSPHRASE`
3. Value: the passphrase from your instructor
4. Repository access: select your fork (`<your-username>/exasol-workshop-starter`)

Or via the CLI:

```bash
gh secret set WORKSHOP_PASSPHRASE --user --repos <your-username>/exasol-workshop-starter --app codespaces
# paste the passphrase when prompted
```

## 3. Create a Codespace

Via the web UI: go to your fork → Code → Codespaces → Create codespace on main

Or via the CLI:

```bash
gh codespace create --repo <your-username>/exasol-workshop-starter --branch main --machine basicLinux32gb
gh codespace ssh  # or open in VS Code
```

AWS access is configured automatically. Verify:

```bash
aws sts get-caller-identity
```

## 4. Follow the workshop

See [workshop.md](../workshop.md) for the full workshop guide.
