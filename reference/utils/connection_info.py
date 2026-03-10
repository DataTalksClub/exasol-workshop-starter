"""
Print Exasol connection details for use in VS Code or other tools.

Reads deployment and secrets files, connects to extract the TLS
certificate fingerprint, and prints everything needed to set up
a connection.
"""

import json
import ssl
import socket
import hashlib
from pathlib import Path


def find_deployment_files(deployment_dir: Path | None = None) -> tuple[Path, Path]:
    if deployment_dir is None:
        deployment_dir = Path(__file__).parent.parent.parent / "deployment"
    else:
        deployment_dir = Path(deployment_dir)

    dep_file = deployment_dir / "deployment.json"
    sec_file = deployment_dir / "secrets.json"
    if dep_file.exists() and sec_file.exists():
        return dep_file, sec_file
    raise FileNotFoundError(
        f"Expected deployment files not found in {deployment_dir}: "
        "deployment.json and secrets.json"
    )


def get_config(deployment_dir: Path | None = None) -> dict:
    dep_file, sec_file = find_deployment_files(deployment_dir)

    with open(dep_file) as f:
        deploy = json.load(f)
    with open(sec_file) as f:
        secrets = json.load(f)

    node = next(iter(deploy["nodes"].values()))

    return {
        "host": node["dnsName"],
        "port": int(node["database"]["dbPort"]),
        "user": secrets["dbUsername"],
        "password": secrets["dbPassword"],
        "deployment_id": deploy["deploymentId"],
    }


def get_fingerprint(host: str, port: int = 8563) -> str:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    with socket.create_connection((host, port), timeout=10) as sock:
        with context.wrap_socket(sock, server_hostname=host) as ssock:
            cert_der = ssock.getpeercert(binary_form=True)
            return hashlib.sha256(cert_der).hexdigest().upper()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Print Exasol connection details")
    parser.add_argument(
        "-d", "--deployment-dir",
        default=None,
        help="Path to deployment directory (default: ~/deployment)",
    )
    args = parser.parse_args()

    config = get_config(args.deployment_dir)

    print("Exasol Connection Details")
    print("=" * 50)
    print(f"Host:        {config['host']}")
    print(f"Port:        {config['port']}")
    print(f"Username:    {config['user']}")
    print(f"Password:    {config['password']}")

    print()
    print("Fetching TLS certificate fingerprint...")
    try:
        fingerprint = get_fingerprint(config["host"], config["port"])
        print(f"Fingerprint: {fingerprint}")
    except Exception as e:
        print(f"Could not fetch fingerprint: {e}")
        fingerprint = None


if __name__ == "__main__":
    main()
