# AeroStream Showcase & Documentation Portal (`aerostream.gradientgeeks.com`)

Welcome to the official web showcase and documentation portal for **AeroStream** (by [Gradient Geeks](https://gradientgeeks.com)), deployed at [`aerostream.gradientgeeks.com`](https://aerostream.gradientgeeks.com).

---

## 🎯 Overview

This is the public-facing showcase and technical documentation portal for **AeroStream**, designed for deployment to CDN / static hosting (e.g. Cloudflare Pages, Firebase Hosting, AWS S3/CloudFront) at [`aerostream.gradientgeeks.com`](https://aerostream.gradientgeeks.com). It showcases benchmark results, architecture deep dives, Kafka wire compatibility, and interactive documentation.

---

## 🚀 Key Features of the Showcase App

1. **Dual-Engine Showcase Hero**:
   - Interactive live performance statistics (1,064 MB/s native throughput, 60 µs p50 median delivery latency, 1.29 MiB idle memory, 3 OS worker threads).
   - High-contrast visual branding with zero blurry glows, featuring the animated SVG Turbine Ring Buffer logo.
2. **Interactive Benchmark Explorer**:
   - 4-way comparative metrics dashboard across payload sizes from 100 Bytes to 50 MB:
     - **Apache Kafka** (Official Apache JVM Broker)
     - **Redpanda** (C++ Seastar Broker)
     - **AeroStream (Kafka Port :9092)** (Drop-in compatibility mode)
     - **AeroStream (Native Port :9091)** (High-speed zero-copy kernel)
   - Dynamic switching across Wire Throughput, Median Latency (p50), and Memory Footprint.
3. **Dual-Engine Architecture Deep-Dive**:
   - Interactive architecture inspector detailing Go Raft Quorum FSM, Rust Zero-Copy Commit Log Storage, Kafka Wire Protocol listener with in-place base offset patching, and Multi-Cloud Tiered Storage.
4. **Comprehensive Technical Documentation Portal (`/docs`)**:
   - Chapter 1: Platform Overview & 30-Second Cluster Deployment
   - Chapter 2: Dual-Engine Architecture Deep-Dive
   - Chapter 3: Kafka Wire Protocol & Drop-In Migration
   - Chapter 4: Built-in Confluent-Compatible Schema Registry
   - Chapter 5: In-Broker Stream Transforms & Automated PII Masking
   - Chapter 6: Enterprise Role-Based Access Control (RBAC) & Cooperative Sticky Rebalance (KIP-848)
   - Chapter 7: Multi-Cloud Tiered Storage (AWS S3, MinIO, GCS, Azure Blob)
   - Chapter 8: Production Operator Hardware Sizing & Graceful Partition Draining
   - Sticky scroll-spy Table of Contents, instant search filtering, and one-click code snippet copying.
5. **Theme Harmonization**:
   - Signal-based Dark and Light theme switcher with local storage persistence and full Angular Material MDC + Tailwind CSS synchronization.
6. **Modern Web Standards**:
   - Built on Angular 21 with OnPush change detection, Signals, standalone components, semantic HTML5, container queries, and sub-100 kB initial transfer bundle size.

---

## 🛠️ Local Development

### Prerequisites
- Node.js `v20.x` or `v24.x` (verified with `v24.17.0`)
- npm `v10.x` or `v11.x`

### Quickstart

```bash
# Navigate to site directory
cd site

# Install dependencies (if not already installed)
npm install

# Start local dev server (default port 4200)
npm start
# or: ng serve
```

Open your browser at `http://localhost:4200`.

---

## 📦 Production Build

```bash
cd site
npm run build
```

The production output is placed into `site/dist/site/browser` and is ready for static deployment on any modern web host or CDN:

```bash
# Preview production build locally with any static HTTP server:
npx serve dist/site/browser -p 8080
```

---

## 🌐 Deploying to `aerostream.gradientgeeks.com`

### Option 1: Cloudflare Pages / Vercel
1. Set Root Directory to `site`.
2. Build Command: `npm run build`.
3. Output Directory: `dist/site/browser`.

### Option 2: Nginx / Kubernetes Ingress
```nginx
server {
    listen 80;
    server_name aerostream.gradientgeeks.com;
    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```
