# Local Setup (without Codespaces)

If you're not using GitHub Codespaces, you need to install the tools and configure AWS credentials using your own AWS account.

## Install AWS CLI

```bash
# Linux (x86_64)
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -o awscliv2.zip
sudo ./aws/install

# macOS
brew install awscli
```

Verify: `aws --version`

## Install the Exasol CLI

```bash
mkdir -p ~/bin
curl https://downloads.exasol.com/exasol-personal/installer.sh | bash
mv exasol ~/bin/
```

Or download it from the [Exasol Personal Edition page](https://downloads.exasol.com/exasol-personal) and place it in `~/bin/` (or any other folder on the `PATH`).

## Configure AWS Credentials

You'll need your own AWS account. Configure the CLI with your credentials:

```bash
aws configure
```

Set the region:

```bash
export AWS_DEFAULT_REGION=eu-central-1
```

Verify:

```bash
aws sts get-caller-identity
```

## Follow the workshop

Once the tools are installed and AWS is configured, follow [workshop.md](../workshop.md).
