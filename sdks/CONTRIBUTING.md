# Contributing to AeroStream Client SDKs

Thank you for your interest in contributing to the **AeroStream Official Client SDKs**!

This repository (`gradientgeeks/aerostream-sdk`) houses the official language bindings for AeroStream's ultra-low-latency native binary protocol (`0xAE 0x01` on TCP port `9091`).

---

## 1. Contributor License Agreement (CLA)

Before we can merge any pull requests, you must accept our **[Contributor License Agreement (CLA)](CLA.md)**.
* When you open a Pull Request, the **CLA Assistant** bot will ask you to review and sign the agreement.
* The CLA ensures that these SDKs remain 100% permissively licensed under Apache 2.0 for all developers and companies worldwide.

---

## 2. Code of Conduct

All contributors are expected to follow the **[Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md)**.

---

## 3. Supported SDK Languages & Directory Structure

```
sdks/
├── go/       # Go Client (github.com/gradientgeeks/aerostream-sdk/go)
├── rust/     # Rust Crate (aerostream-client)
├── java/     # Java Client (org.gradientgeeks.aerostream:aerostream-client)
├── dotnet/   # .NET C# Client (GradientGeeks.AeroStream.Client)
└── nodejs/   # Node.js TypeScript Client (@gradientgeeks/aerostream-client)
```

---

## 4. Local Testing & Verification

Before submitting a Pull Request, ensure that the tests for your target language pass:

### Go SDK
```bash
cd go
go test -v ./...
```

### Rust SDK
```bash
cd rust
cargo test
cargo clippy -- -D warnings
```

### Java SDK
```bash
cd java
mvn clean test
```

### .NET SDK
```bash
cd dotnet
dotnet test
```

### Node.js SDK
```bash
cd nodejs
npm install
npm test
npm run build
```

---

## 5. Native Binary Protocol Contract (`0xAE 0x01`)

All SDK implementations must conform strictly to the native protocol framing:

* **Header (7 bytes)**:
  `[magic: 0xAE, 0x01 (2B)][cmd: u8 (1B)][body_len: u32 BE (4B)]`
* **Command 0 (Auth)**:
  Request body: `[token: UTF-8]`. Response: `[0xAE, 0x01, status: u8]`.
* **Command 1 (Produce)**:
  Request: `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][payload_len: u32 BE][payload: raw bytes]`.
  Response: `[0xAE, 0x01, status: u8][offset: u64 BE]`.
* **Command 2 (Fetch Single/Bounded)**:
  Request: `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][start_offset: u64 BE][max_bytes: u32 BE]`.
* **Command 4 (Multi-Entry Long Poll)**:
  Request: `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][start_offset: u64 BE][max_bytes: u32 BE][max_wait_ms: u32 BE]`.

---

## 6. Pull Request Guidelines

1. Follow Conventional Commits: `feat(...)`, `fix(...)`, `docs(...)`, `test(...)`.
2. Add or update unit tests for any new features or protocol edge cases.
3. Keep public APIs idiomatic to the target programming language.
