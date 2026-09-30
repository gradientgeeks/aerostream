# Security Policy

The AeroStream team takes the security and integrity of distributed data pipelines seriously. We appreciate your efforts to responsibly disclose vulnerabilities to us.

---

## Supported Versions

Security updates and patches are actively maintained for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| `0.1.x` | :white_check_mark: |
| `< 0.1` | :x:                |

---

## Reporting a Vulnerability

**Please DO NOT report security vulnerabilities through public GitHub issues, discussions, or pull requests.**

If you believe you have discovered a vulnerability in AeroStream, please follow this disclosure process:

### 1. Preferred Method: GitHub Private Vulnerability Reporting
Submit a private report through GitHub:
- Navigate to the repository's **Security** tab.
- Click **Report a vulnerability** under *Advisories*.
- Provide full reproduction details.

### 2. Alternative Method: Direct Security Email
If GitHub Private Reporting is unavailable, send an encrypted or direct email to:
* **`security@gradientgeeks.com`**
* CC: **`uttam-mahata-cs@outlook.com`**

### What to Include in Your Report
To help us triage and resolve the issue quickly, please include:
* Description of the vulnerability (e.g., unauthorized partition access, wire protocol buffer overflow, consensus split-brain flaw).
* Affected components (`rust-broker`, `go-controller`, `ui`, `sdks`).
* Step-by-step reproduction instructions or a minimal proof-of-concept (PoC).
* Potential impact and attack vectors.

---

## Response Timeline & SLAs

* **Initial Acknowledgment**: Within **48 hours** of receiving your report.
* **Triage & Assessment**: Within **5 business days** with confirmation of validity and severity rating (CVSS).
* **Patch Development**: We aim to deliver a fix within **30 days** depending on vulnerability complexity.
* **Public Disclosure**: Coordinated disclosure after a patched version is published and users have reasonable time to update.

---

## Security Best Practices for Operators

When deploying AeroStream in production:
* Enable TLS on Kafka wire port (`9092`) and native port (`9091`).
* Enforce authentication (SASL/SCRAM or mTLS) and configure RBAC / ACL policies.
* Isolate Raft consensus (port `7001`) and gRPC control channels (port `8001`) on a private internal VPC/network.
* Run broker and controller containers with unprivileged cgroup user IDs.
