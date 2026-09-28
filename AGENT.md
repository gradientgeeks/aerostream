# AeroStream Developer & AI Agent Guide (`AGENT.md`)

Welcome to the **AeroStream** codebase. This guide serves as the definitive reference for AI agents and human contributors working across the repository.

---

## 1. System Architecture

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

- **Go Control Plane (`go-controller/`)**:
  - Implements HashiCorp Raft consensus (`ClusterState` FSM).
  - Dynamic REST API on port `9001` serving metrics, topics, partitions, consumer lag, and ACLs.
  - Confluent Schema Registry (AVRO, JSON Schema, Protobuf).
  - Kafka Connect 100% REST Engine.
  - Serves the compiled Angular Web Console UI at `/aerostream/console/` (or `/`).

- **Rust Data Plane (`rust-broker/`)**:
  - Shard-per-Core engine pinning worker threads to dedicated CPU cores (`libc::sched_setaffinity`).
  - Zero-copy commit log and in-place base offset patching.
  - Paced page-cache writeback (`sync_file_range` + `posix_fadvise`) eliminating kernel dirty page flusher stalls.
  - Wire compatibility supporting 35+ Kafka API keys on port `9092` / `9093`.
  - Ultra-high throughput binary protocol on port `9091` (`0xAE 0x01`).

- **Web Console UI (`ui/`)**:
  - Angular 21 application built with Angular Material and Tailwind CSS.
  - Embedded in the Go Controller HTTP server and Docker container.

- **Documentation & Showcase Site (`site/`)**:
  - Angular 21 responsive landing page, interactive benchmarks, and comprehensive documentation.
  - Hosted on **Firebase Hosting** (`aerostream-gg`).

---

## 2. Semantic Versioning Specification (SemVer 2.0.0)

AeroStream strictly adheres to [Semantic Versioning 2.0.0](https://semver.org/):

$$\text{Format: } \mathbf{vMAJOR.MINOR.PATCH} \quad (\text{Current: } \mathbf{v0.1.0})$$

1. **MAJOR (`vX.0.0`)**:
   - Incompatible API or wire protocol changes.
   - Breaking configuration schema modifications.
2. **MINOR (`v0.X.0`)**:
   - Backwards-compatible new features.
   - New Kafka API keys or protocol version additions.
   - New REST endpoints, connector plugins, or Schema Registry serializers.
3. **PATCH (`v0.1.X`)**:
   - Backwards-compatible bug fixes and security patches.
   - Performance optimizations, memory reductions, and refactors.

> [!IMPORTANT]
> When bumping versions, ensure consistency across:
> - `site/package.json`
> - `ui/package.json`
> - `.github/workflows/firebase-hosting-*.yml`
> - UI brand version pills (`v0.1.0`)

---

## 3. Modern Web Guidance & Frontend Standards

All UI development in `ui/` and `site/` must comply with modern web development standards:

### 3.1 Button Interactivity & Touch Targets (WCAG 2.5.8 AA)
- **Focus Rings**: Always use `:focus-visible` with `outline: 2px solid var(--app-primary); outline-offset: 2px;`. Never use bare `outline: none`.
- **Touch Targets**: Minimum target size of **44×44px** on coarse pointers (`@media (pointer: coarse)`) and mobile viewports (`< 640px`).
- **Tactile Feedback**: Use `:active:not([disabled]) { transform: scale(0.98); }` for physical press feedback.
- **Forced Colors Mode**: Support Windows High Contrast Mode (`@media (forced-colors: active)`) using system keywords `ButtonText` and `Highlight`.
- **Reduced Motion**: Respect `prefers-reduced-motion: reduce` by disabling non-essential transitions and transform animations.

### 3.2 Responsive Design Principles
- **Fluid Layouts**: Use `clamp()`, `min()`, `max()`, and container queries instead of rigid media query breakpoints.
- **Mobile Stack**: Multi-button action rows (`.hero-actions`, `.footer-cta-buttons`, `.article-nav-buttons`) must stack cleanly on mobile (`< 640px`) with 100% button width.
- **Horizontal Scrolling**: Horizontal tab bars and command snippets must support smooth touch scrolling (`-webkit-overflow-scrolling: touch; overflow-x: auto;`) without breaking layout constraints.
- **Visual Clarity**: Strictly avoid blurry glowing drop-shadows or high-bloom effects. Use crisp 1px borders with subtle alpha transparencies (`rgba(0, 229, 255, 0.2)`).

---

## 4. Build & Run Procedures

### 4.1 Local Cluster Management
```bash
# Start 3 Go Controllers and 2 Rust Brokers
make start

# View running cluster status
curl -s http://127.0.0.1:9001/api/cluster | jq .

# Stop cluster
make stop
```

### 4.2 Web Console UI (`ui/`)
```bash
cd ui
npm install
npm run build    # Output: ui/dist/ui/browser
```

### 4.3 Documentation & Landing Site (`site/`)
```bash
cd site
npm install
npm run build    # Output: site/dist/site/browser
npm start        # Local development server on http://localhost:4200
```

### 4.4 Multi-Stage Docker Container
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

## 5. CI/CD & Firebase Hosting Workflow

The documentation and showcase site is automatically validated and deployed to Firebase Hosting via GitHub Actions:

- **Workflows**:
  - `.github/workflows/firebase-hosting-merge.yml`: Triggers on push to `main`, verifies SemVer `v0.1.0`, builds Angular `site/`, ensures site provisioning (`firebase hosting:sites:create`), and deploys to the **live channel** on project `aerostream-gg`.
  - `.github/workflows/firebase-hosting-pull-request.yml`: Triggers on pull requests, verifies SemVer `v0.1.0`, builds Angular `site/`, ensures site provisioning, and deploys an ephemeral **preview channel**.
- **On-Demand Site Provisioning (October 2026 Compliant)**:
  - Starting October 15, 2026, Firebase Hosting requires explicit on-demand site provisioning for newly created or automated projects.
  - The workflow executes `firebase hosting:sites:create aerostream-gg --project=aerostream-gg` before deployment to prevent `404 Site Not Found` errors.
  - Explicit site binding is declared in `site/firebase.json` via `"site": "aerostream-gg"`.
- **Service Account Secret**: `FIREBASE_SERVICE_ACCOUNT_AEROSTREAM_GG`
- **GitHub Token**: `GITHUB_TOKEN`

---

## 6. Git Synchronization Rules

AeroStream maintains two upstream git remotes that **must always remain synchronized**:

1. **Azure DevOps (origin)**: `git@ssh.dev.azure.com:v3/gradient-geeks/gradientgeeks/AeroMQ`
2. **GitHub (github)**: `https://github.com/gradientgeeks/aerostream.git`

Always push changes to both remotes:
```bash
git push origin main && git push github main
```
