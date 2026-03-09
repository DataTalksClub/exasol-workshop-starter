FROM mcr.microsoft.com/devcontainers/base:ubuntu

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip python3 python3-pip file \
    && rm -rf /var/lib/apt/lists/*

# AWS CLI
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscli.zip \
    && unzip -q /tmp/awscli.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscli.zip

# uv (install to /usr/local/bin so it's available for all users)
RUN curl -LsSf https://astral.sh/uv/install.sh | env INSTALLER_NO_MODIFY_PATH=1 sh -s -- --no-modify-path \
    && mv /root/.local/bin/uv /usr/local/bin/ \
    && mv /root/.local/bin/uvx /usr/local/bin/

# ~/bin for user-installed tools
RUN mkdir -p /home/vscode/bin && chown vscode:vscode /home/vscode/bin
ENV PATH="/home/vscode/bin:${PATH}"

# Exasol Personal Edition CLI
RUN curl -fSL "https://x-up.s3.eu-west-1.amazonaws.com/releases/exasol-personal/linux/$(uname -m)/latest/exasol" \
    -o /home/vscode/bin/exasol \
    && chmod +x /home/vscode/bin/exasol \
    && chown vscode:vscode /home/vscode/bin/exasol
