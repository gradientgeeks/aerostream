# Contributing to AeroStream

First off, thank you for considering contributing to **AeroStream**! It is people like you that make AeroStream such a resilient, high-performance distributed streaming engine.

Please take a moment to review these guidelines before submitting code or opening issues.

---

## 1. Contributor License Agreement (CLA)

Before we can merge your pull requests, you must accept our **[Contributor License Agreement (CLA)](CLA.md)**.

* When you open a Pull Request, our automated **CLA Assistant** bot will post a comment with a link to digitally sign the agreement.
* The CLA ensures that all contributions can be freely redistributed under Apache 2.0 and protects the project maintainers and downstream users.

---

## 2. Code of Conduct

AeroStream follows the **[Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md)**. By participating in this project, you agree to abide by its terms.

---

## 3. Reporting Issues and Requesting Features

* **Bug Reports**: Open an issue describing the problem, reproduction steps, expected behavior, system architecture (CPU, OS, memory), and log outputs.
* **Feature Requests**: Open a feature discussion or issue detailing the use case, why existing Kafka API/features don't suffice, and proposed architecture.
* **Security Issues**: **Do NOT** report security vulnerabilities via public issues. Follow our **[Security Policy](SECURITY.md)** instead.

---

## 4. Development Setup

AeroStream combines a **Go 1.26 Control Plane** and a **Rust 1.98.1 Data Plane**.

### Prerequisites
* Go 1.24+ (Recommended: Go 1.26+)
* Rust 1.75+ (Edition 2021/2024, `rustup toolchain install stable`)
* Node.js 22+ & npm (for Web Console and docs)
* Docker (for multi-node integration tests)

### Building from Source

```bash
# 1. Build Go Controller
cd go-controller
go build -o bin/controller ./cmd/controller

# 2. Build Rust Broker
cd ../rust-broker
cargo build --release

# 3. Build Web Console
cd ../ui
npm install && npm run build
```

---

## 5. Coding Standards

* **Rust Data Plane**:
  - Format using `cargo fmt --check`.
  - Lint using `cargo clippy --all-targets -- -D warnings`.
  - Maximize zero-copy paths: favor `Bytes`, `Span<u8>`, and direct `sendfile(2)` DMA over unnecessary heap allocations.
* **Go Control Plane**:
  - Format using `gofmt -s` and `goimports`.
  - Lint using `golangci-lint run`.
  - Ensure lock contention is minimized; use sync primitives judiciously.
* **Documentation & Comments**:
  - Keep inline comments concise and purposeful (maximum 3 lines per comment block). Avoid storytelling narratives.

---

## 6. Testing

All pull requests must pass automated test suites:

```bash
# Run Rust unit and integration tests
cd rust-broker && cargo test

# Run Go unit and consensus tests
cd go-controller && go test -race -v ./...

# Run End-to-End client suites
python3 client/test_scale_eos.py
python3 client/test_persistent_group_state.py
```

---

## 7. Submitting a Pull Request

1. Fork the repository and create a descriptive branch:
   ```bash
   git checkout -b feat/tiered-storage-gcs-retry
   ```
2. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat(...)`: A new feature
   - `fix(...)`: A bug fix
   - `perf(...)`: Performance improvement
   - `docs(...)`: Documentation changes
   - `test(...)`: Adding or updating tests
3. Push to your fork and submit a Pull Request to `main`.
4. Ensure the **CLA Bot** check and all CI tests pass.
5. Address reviewer feedback constructively.
