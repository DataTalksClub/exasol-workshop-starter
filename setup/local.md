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

## Required AWS Permissions

We tested this workshop with the following IAM policy. The permissions are broad; you may be able to make them more restrictive for your setup.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "ec2:*", "Resource": "*" },
    { "Effect": "Allow", "Action": "s3:*", "Resource": "*" },
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole", "iam:PassRole", "iam:DeleteRole",
        "iam:GetRole", "iam:GetRolePolicy", "iam:TagRole",
        "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
        "iam:PutRolePolicy", "iam:DeleteRolePolicy",
        "iam:CreateInstanceProfile", "iam:TagInstanceProfile",
        "iam:AddRoleToInstanceProfile", "iam:DeleteInstanceProfile",
        "iam:RemoveRoleFromInstanceProfile", "iam:GetInstanceProfile",
        "iam:ListInstanceProfiles", "iam:ListInstanceProfilesForRole",
        "iam:ListRoles"
      ],
      "Resource": "*"
    },
    { "Effect": "Allow", "Action": "ssm:*", "Resource": "*" },
    { "Effect": "Allow", "Action": "logs:*", "Resource": "*" }
  ]
}
```

## Follow the workshop

Once the tools are installed and AWS is configured, follow [workshop.md](../workshop.md).
