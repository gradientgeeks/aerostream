# AeroStream Developer & AI Agent Guide (`AGENT.md`)

Welcome to the **AeroStream** codebase. This guide serves as the definitive reference for AI agents and human contributors working across the repository.

---

## 1. System Architecture & Repository Ecosystem

AeroStream is a next-generation distributed streaming platform featuring a **dual-engine architecture** that decouples consensus governance from high-throughput log storage:

```
                          ┌────────────────────────┐
                          │   Kafka Clients        │
                          │   (:9092 / :9093)      │
                          └───────────┬────────────┘
                                      │ Kafka Wire Protocol
                                      ▼
┌─────────────────────────┐       ┌────────────────────────┐
│  Go Control Plane       │       │  Rust Data Plane       │
│  (go-controller/)       │◄─────►│  (rust-broker/)        │
│  - HashiCorp Raft FSM   │ gRPC  │  - Shard-per-Core      │
│  - Schema Registry      │:8001  │  - Zero-Copy Storage   │
│  - Kafka Connect 100%   │       │  - Paced Page Cache    │
│  - Web Console UI:9001  │       │  - Native Port :9091   │
└─────────────────────────┘       └────────────────────────┘
```

### 1.1 Core Engines
- **Go Control Plane (`go-controller/`)**:
  - Implements HashiCorp Raft consensus (`ClusterState` FSM) on port `7001`.
  - Dynamic REST API on port `9001` serving metrics, topics, partitions, consumer lag, and ACLs.
  - Confluent-compatible Schema Registry (AVRO, JSON Schema, Protobuf).
  - Kafka Connect 100% REST Engine.
  - Serves the compiled Angular Web Console UI at `/aerostream/console/` (or `/`).

- **Rust Data Plane (`rust-broker/`)**:
  - Shard-per-Core engine pinning worker threads to dedicated CPU cores (`libc::sched_setaffinity`).
  - Zero-copy commit log and in-place base offset patching.
  - Paced page-cache writeback (`sync_file_range` + `posix_fadvise`) eliminating kernel dirty page flusher stalls.
  - Wire compatibility supporting 35+ Kafka API keys on port `9092` / `9093`.
  - Ultra-high throughput binary protocol on port `9091` (`0xAE 0x01`).

- **Showcase & Benchmark Portal (`site/`)**:
  - Angular 21 responsive landing page, interactive benchmarks, and documentation host.
  - Hosted and continuously deployed on **Firebase Hosting** (`aerostream-gg`).

---

## 2. Git Submodules & Multi-Repo Ecosystem

The AeroStream ecosystem is organized into modular, independently versioned standalone GitHub repositories under the `gradientgeeks` organization. The primary `aerostream` repository embeds components as Git submodules:

| Submodule Path | Standalone Repository | Description | Visibility |
| :--- | :--- | :--- | :--- |
| `sdks/` | [`gradientgeeks/aerostream-sdk`](https://github.com/gradientgeeks/aerostream-sdk) | Official Native Protocol client SDKs (Go, Rust, Java, .NET, Node.js) | Public |
| `ui/` | [`gradientgeeks/aerostream-ui`](https://github.com/gradientgeeks/aerostream-ui) | Standalone Angular 21 Web Management Console | Public |
| `docs/` | [`gradientgeeks/aerostream-docs`](https://github.com/gradientgeeks/aerostream-docs) | MkDocs Material documentation portal source & diagrams | Public |

### 2.1 Submodule Usage & Workflows

#### Cloning the Repository with Submodules
To clone the main repository along with all nested submodules:
```bash
git clone --recurse-submodules https://github.com/gradientgeeks/aerostream.git
```

#### Initializing Submodules in an Existing Clone
If the repository was cloned without `--recurse-submodules`:
```bash
git submodule update --init --recursive
```

#### Pulling the Latest Upstream Changes for All Submodules
To fast-forward all submodules to the latest commits on their tracking branches:
```bash
git submodule update --remote --merge
```

#### Making & Committing Changes in a Submodule
When contributing to a submodule (e.g. `ui/`, `sdks/`, or `docs/`):
```bash
cd ui # or sdks / docs
git checkout main
# Edit files, run tests, and verify
git add .
git commit --author="Uttam-Mahata <uttam-mahata-cs@outlook.com>" -m "feat(ui): add feature description"
git push origin main

# Update parent pointer in the primary aerostream repo
cd ..
git add ui
git commit --author="Uttam-Mahata <uttam-mahata-cs@outlook.com>" -m "chore(submodule): bump ui to latest commit"
git push origin main && git push github main
```

#### Checking Submodule Status
```bash
git submodule status
git submodule foreach 'git status -s'
```

---

## 3. Contributor License Agreement (CLA) & Governance

All public repositories under `gradientgeeks` require a signed Contributor License Agreement (CLA) before pull requests can be merged.

### 3.1 CLA Metadata & Legal Contact
- **CLA Document**: [`CLA.md`](CLA.md) (hosted at `https://aerostream.gradientgeeks.com/docs/cla/`)
- **License**: Apache License 2.0
- **Primary Contact Email**: `contact@gradientgeeks.com`
- **Maintainer CC**: `uttam-mahata-cs@outlook.com`

### 3.2 Configuring CLA Assistant (cla-assistant.io)

When setting up or managing CLA Assistant on [cla-assistant.io](https://cla-assistant.io):

1. **Step ① — Choose a Repository or Organization**:
   - **Recommended (Organization-wide)**: Click **"(want to link an org?)"** at the top right of the prompt. Select `gradientgeeks`. This links all current and future repositories (`aerostream`, `aerostream-sdk`, `aerostream-ui`, `aerostream-docs`) in a single step!
   - **Per-Repository**: Alternatively, select `gradientgeeks/aerostream` from the dropdown, then repeat for `aerostream-sdk`, `aerostream-ui`, and `aerostream-docs`.

2. **Step ② — Choose a CLA (Gist)**:
   - CLA Assistant requires a public GitHub Gist containing the CLA markdown text.
   - Create a public Gist at [gist.github.com](https://gist.github.com) titled `CLA.md` containing the text from `CLA.md`.
   - Paste the Gist URL (e.g., `https://gist.github.com/Uttam-Mahata/<gist-id>`) into Step ②.
   - Leave *"Share the Gist"* unchecked.

3. **Step ③ — CLA Requirement Settings (Optional)**:
   - **Minimum File Number Changes**: Leave blank.
   - **Minimum Line Number Changes**: Leave blank.
   - *Rationale*: Leaving these blank ensures that **any** contribution (even a single-line fix) requires a signed CLA, maintaining strict legal compliance.

4. **Click "LINK"**:
   - CLA Assistant adds a webhook to the repository and immediately monitors all PRs.
   - Contributors sign the CLA with a single click directly inside the GitHub PR discussion via GitHub OAuth.

---

## 4. Semantic Versioning Specification (SemVer 2.0.0)

AeroStream strictly adheres to [Semantic Versioning 2.0.0](https://semver.org/):

$$\text{Format: } \mathbf{vMAJOR.MINOR.PATCH} \quad (\text{Current: } \mathbf{v0.1.0})$$

1. **MAJOR (`vX.0.0`)**: Incompatible API or wire protocol changes; breaking configuration schema modifications.
2. **MINOR (`v0.X.0`)**: Backwards-compatible new features, new Kafka API keys, or REST endpoints.
3. **PATCH (`v0.1.X`)**: Backwards-compatible bug fixes, performance optimizations, and security patches.

> [!IMPORTANT]
> When bumping versions, ensure consistency across:
> - `site/package.json`
> - `ui/package.json`
> - `.github/workflows/firebase-hosting-*.yml`
> - UI brand version pills (`v0.1.0`)

---

## 5. Modern Web Guidance & Frontend Standards

All UI development in `ui/` and `site/` must comply with modern web development standards:

### 5.1 Button Interactivity & Touch Targets (WCAG 2.5.8 AA)
- **Focus Rings**: Always use `:focus-visible` with `outline: 2px solid var(--app-primary); outline-offset: 2px;`. Never use bare `outline: none`.
- **Touch Targets**: Minimum target size of **44×44px** on coarse pointers (`@media (pointer: coarse)`) and mobile viewports (`< 640px`).
- **Tactile Feedback**: Use `:active:not([disabled]) { transform: scale(0.98); }` for physical press feedback.
- **Forced Colors Mode**: Support Windows High Contrast Mode (`@media (forced-colors: active)`) using system keywords `ButtonText` and `Highlight`.
- **Reduced Motion**: Respect `prefers-reduced-motion: reduce` by disabling non-essential transitions and transform animations.

### 5.2 Responsive Design Principles
- **Fluid Layouts**: Use `clamp()`, `min()`, `max()`, and container queries instead of rigid media query breakpoints.
- **Mobile Stack**: Multi-button action rows must stack cleanly on mobile (`< 640px`) with 100% button width.
- **Horizontal Scrolling**: Horizontal tab bars and command snippets must support smooth touch scrolling (`-webkit-overflow-scrolling: touch; overflow-x: auto;`) without breaking layout constraints.
- **Visual Clarity**: Strictly avoid blurry glowing drop-shadows or high-bloom effects. Use crisp 1px borders with subtle alpha transparencies (`rgba(0, 229, 255, 0.2)`).

---

## 6. Build & Run Procedures

### 6.1 Local Cluster Management
```bash
# Start 3 Go Controllers and 2 Rust Brokers
make start

# View running cluster status
curl -s http://127.0.0.1:9001/api/cluster | jq .

# Stop cluster
make stop
```

### 6.2 Web Console UI (`ui/`)
```bash
cd ui
npm install
npm run build    # Output: ui/dist/ui/browser
```

### 6.3 Documentation & Landing Site (`site/` & `docs/`)
```bash
# Build Angular showcase website
cd site
npm install
npm run build    # Output: site/dist/site/browser

# Build MkDocs documentation into website dist
cd ..
mkdocs build -f docs/mkdocs.yml -d ../site/dist/site/browser/docs
```

### 6.4 Multi-Stage Docker Container
```bash
# Build multi-stage image (UI + Go Controller + Rust Broker)
docker build -t quay.io/gradientgeeks/aerostream:latest -t quay.io/uttammahata/aerostream:latest .

# Run container locally
docker run -d --name aerostream \
  -p 9001:9001 \
  -p 9091:9091 \
  -p 9092:9092 \
  -p 8001:8001 \
  quay.io/gradientgeeks/aerostream:latest
```

---

## 7. CI/CD & Firebase Hosting Workflow

The documentation and showcase site is automatically validated and deployed to Firebase Hosting via GitHub Actions:

- **Workflows**:
  - `.github/workflows/firebase-hosting-merge.yml`: Triggers on push to `main`, checks out repository with `submodules: recursive`, verifies SemVer `v0.1.0`, builds Angular `site/`, builds `docs/` using MkDocs into `site/dist/site/browser/docs`, ensures site provisioning (`firebase hosting:sites:create`), and deploys to the **live channel** on project `aerostream-gg`.
  - `.github/workflows/firebase-hosting-pull-request.yml`: Triggers on pull requests, checks out with submodules, builds `site/` and `docs/`, and deploys an ephemeral **preview channel**.
- **On-Demand Site Provisioning**:
  - The workflow executes `firebase hosting:sites:create aerostream-gg --project=aerostream-gg` before deployment to prevent `404 Site Not Found` errors.
  - Explicit site binding is declared in `site/firebase.json` via `"site": "aerostream-gg"`.
- **Secrets**: `FIREBASE_SERVICE_ACCOUNT_AEROSTREAM_GG` and `GITHUB_TOKEN`.

---

## 8. Git Commit & Synchronization Rules

AeroStream strictly enforces commit authorship and remote synchronization:

### 8.1 Commit Authorship
All commits must be authored with:
```bash
git commit --author="Uttam-Mahata <uttam-mahata-cs@outlook.com>" -m "..."
```

### 8.2 Remotes Synchronization
AeroStream maintains two upstream git remotes that **must always remain synchronized**:

1. **Azure DevOps (`origin`)**: `git@ssh.dev.azure.com:v3/gradient-geeks/gradientgeeks/AeroMQ`
2. **GitHub (`github`)**: `https://github.com/gradientgeeks/aerostream.git`

Always push changes to both remotes:
```bash
git push origin main && git push github main
```
