import { Component, signal, computed, ChangeDetectionStrategy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatTooltipModule } from '@angular/material/tooltip';

export interface DocSection {
  id: string;
  category: string;
  title: string;
  badge?: string;
  readTime: string;
  summary: string;
  anchors: { id: string; label: string }[];
}

@Component({
  selector: 'app-docs',
  standalone: true,
  imports: [
    CommonModule,
    RouterLink,
    FormsModule,
    MatIconModule,
    MatButtonModule,
    MatTooltipModule,
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './docs.component.html',
  styleUrl: './docs.component.scss'
})
export class DocsComponent {
  readonly searchQuery = signal<string>('');
  readonly activeSectionId = signal<string>('overview');
  readonly copiedCodeBlock = signal<string | null>(null);
  readonly isSidebarMobileOpen = signal<boolean>(false);

  readonly sections: DocSection[] = [
    {
      id: 'overview',
      category: 'Getting Started',
      title: 'Platform Overview & Quickstart',
      badge: 'Core',
      readTime: '4 min',
      summary: 'Introduction to AeroStream, Dual-Engine architecture rationale, and rapid 30-second cluster deployment.',
      anchors: [
        { id: 'what-is-aerostream', label: 'What is AeroStream?' },
        { id: 'dual-engine-rationale', label: 'Dual-Engine Rationale' },
        { id: 'quickstart-cluster', label: 'Deploying a 30s Cluster' },
        { id: 'connecting-clients', label: 'Connecting Clients' },
      ]
    },
    {
      id: 'architecture',
      category: 'Architecture',
      title: 'Dual-Engine Architecture Deep-Dive',
      badge: 'Deep Dive',
      readTime: '9 min',
      summary: 'In-depth analysis of the Go Raft control plane and Rust zero-copy storage kernel.',
      anchors: [
        { id: 'control-plane-kernel', label: 'Control Plane (Go 1.26)' },
        { id: 'storage-data-plane', label: 'Data Plane (Rust 1.98.1)' },
        { id: 'zero-copy-pipeline', label: 'Zero-Copy sendfile(2) Pipeline' },
        { id: 'hardware-crc', label: 'Hardware CRC32C Acceleration' },
      ]
    },
    {
      id: 'kafka-protocol',
      category: 'Wire Protocol',
      title: 'Apache Kafka Compatibility (Port 9092)',
      badge: 'Drop-In',
      readTime: '6 min',
      summary: 'Complete guide to Kafka wire protocol compatibility, supported ApiKeys, and migration steps.',
      anchors: [
        { id: 'port-9092-compatibility', label: 'Port 9092 Compatibility' },
        { id: 'supported-api-keys', label: 'Supported API Keys & Versions' },
        { id: 'in-place-patching', label: 'In-Place Base-Offset Patching' },
        { id: 'migrating-from-kafka', label: 'Migrating from Kafka & Redpanda' },
      ]
    },
    {
      id: 'schema-registry',
      category: 'Data Governance',
      title: 'Built-in Schema Registry',
      badge: 'Confluent API',
      readTime: '5 min',
      summary: 'Managing Avro, Protobuf, and JSON schemas with Confluent-compatible REST endpoints.',
      anchors: [
        { id: 'schema-registry-endpoints', label: 'REST API Endpoints' },
        { id: 'schema-compatibility', label: 'Compatibility Enforcement' },
        { id: 'avro-json-protobuf', label: 'Avro, JSON, & Protobuf Examples' },
      ]
    },
    {
      id: 'transforms',
      category: 'Stream Processing',
      title: 'In-Broker Stream Transforms & WASM',
      badge: 'WASM',
      readTime: '5 min',
      summary: 'Executing inline stream filtering, JSON transformation, and automated PII data masking.',
      anchors: [
        { id: 'transform-types', label: 'Supported Transform Types' },
        { id: 'pii-masking', label: 'Automated PII Masking' },
        { id: 'wasm-sandboxing', label: 'WASM Sandbox Execution' },
        { id: 'transforms-api', label: 'Transforms REST API' },
      ]
    },
    {
      id: 'security-rbac',
      category: 'Security & Ops',
      title: 'Enterprise Security, RBAC & ACLs',
      badge: 'RBAC',
      readTime: '6 min',
      summary: 'Role-based access control, granular wildcard ACL policies, and cooperative sticky rebalancing.',
      anchors: [
        { id: 'rbac-roles', label: 'Principal Roles & Permissions' },
        { id: 'acl-rules', label: 'Granular ACL Rules & Wildcards' },
        { id: 'cooperative-sticky', label: 'Cooperative Sticky Rebalance (KIP-848)' },
      ]
    },
    {
      id: 'tiered-storage',
      category: 'Storage',
      title: 'Multi-Cloud Tiered Storage Pipeline',
      badge: 'S3 / GCS',
      readTime: '6 min',
      summary: 'Automating cold segment rolling and asynchronous offloading to AWS S3, MinIO, GCS, and Azure Blob.',
      anchors: [
        { id: 'tiered-storage-concept', label: 'Hot vs. Cold Storage Tiers' },
        { id: 'configuring-providers', label: 'Configuring S3, GCS, and Azure' },
        { id: 'transparent-fetch', label: 'Transparent Historical Fetch' },
      ]
    },
    {
      id: 'operator-guide',
      category: 'Operations',
      title: 'Production Operations & Kubernetes',
      badge: 'Production',
      readTime: '8 min',
      summary: 'Hardware sizing, Kubernetes StatefulSets, automated scale-down, and Prometheus telemetry.',
      anchors: [
        { id: 'hardware-sizing', label: 'Hardware Sizing & CPU Affinity' },
        { id: 'kubernetes-deployment', label: 'Kubernetes StatefulSets & PreStop' },
        { id: 'broker-draining', label: 'Graceful Broker Partition Draining' },
        { id: 'observability', label: 'Prometheus Metrics & Health Checks' },
      ]
    },
    {
      id: 'performance',
      category: 'Benchmarks',
      title: 'Performance Measurement: AeroStream vs Apache Kafka',
      badge: 'OMB',
      readTime: '5 min',
      summary: 'OpenMessaging Benchmark results on the Kafka wire protocol, with CPU, memory, and temperature details of the test machine.',
      anchors: [
        { id: 'perf-results', label: 'Results at a Glance' },
        { id: 'perf-throughput', label: 'Throughput Over Time' },
        { id: 'perf-cpu-thermal', label: 'CPU, Memory & Temperature' },
        { id: 'perf-testbed', label: 'Test Machine & Methodology' },
        { id: 'perf-caveats', label: 'Caveats' },
      ]
    }
  ];

  readonly filteredSections = computed(() => {
    const q = this.searchQuery().toLowerCase().trim();
    if (!q) return this.sections;
    return this.sections.filter(s =>
      s.title.toLowerCase().includes(q) ||
      s.summary.toLowerCase().includes(q) ||
      s.category.toLowerCase().includes(q) ||
      s.anchors.some(a => a.label.toLowerCase().includes(q))
    );
  });

  readonly activeSection = computed(() =>
    this.sections.find(s => s.id === this.activeSectionId()) || this.sections[0]
  );

  readonly composeSnippet = `version: '3.8'

services:
  controller:
    image: gradientgeeks/aerostream-controller:latest
    container_name: aerostream-controller
    ports:
      - "9001:9001"   # HTTP REST, Admin & Schema Registry
      - "8001:8001"   # Internal gRPC & HashiCorp Raft Quorum
    environment:
      - AERO_NODE_ID=node1
      - AERO_RAFT_BOOTSTRAP=true
      - AERO_DATA_DIR=/data/controller
    volumes:
      - controller-data:/data

  broker:
    image: gradientgeeks/aerostream-broker:latest
    container_name: aerostream-broker-1
    ports:
      - "9091:9091"   # Ultra High-Speed Native TCP Protocol
      - "9092:9092"   # Apache Kafka Wire Protocol
    environment:
      - BROKER_ID=1
      - CONTROLLER_ADDR=http://controller:8001
      - STORAGE_DIR=/data/broker
      - LOG_SEGMENT_BYTES=134217728  # 128 MB rolling threshold
    depends_on:
      - controller
    volumes:
      - broker-data:/data

volumes:
  controller-data:
  broker-data:`;

  readonly bash1Snippet = `# 1. Create a 3-partition topic via Controller REST API
curl -X POST http://localhost:9001/api/topics \\
  -H "Content-Type: application/json" \\
  -d '{"name": "orders", "partitions": 3, "replication_factor": 1}'

# 2. Produce records via standard Kafka CLI tools
echo "order-101: {\\"amount\\": 89.50}" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders`;

  readonly migrationSnippet = `# Before: Apache Kafka or Redpanda
# spring.kafka.bootstrap-servers=kafka-cluster.internal:9092

# After: AeroStream (Drop-in, no code change needed)
spring.kafka.bootstrap-servers=aerostream.internal:9092`;

  readonly transformSnippet = `// Input on topic 'orders'
{"order_id": "ORD-1", "credit_card": "4111-2222-3333-4444", "total": 99.00}

// Output on topic 'orders-sanitized' (automatically transformed!)
{"order_id": "ORD-1", "credit_card": "***", "total": 99.00}`;

  readonly drainSnippet = `# Drains broker 10, electing alternative replicas as partition leaders gracefully
curl -X POST http://localhost:9001/api/brokers/10/drain`;

  readonly perfReproSnippet = `# Each command starts one broker container (--cpus=2.0 --memory=2g), runs the OMB workload, samples
# docker stats throughout, and removes the broker afterwards (benchmarks/openmessaging-benchmark/omb-run.sh)
./omb-run.sh quay.io/gradientgeeks/aerostream:latest workloads/aerostream-16p-1kb.yaml aerostream-kafkawire
./omb-run.sh kafka workloads/aerostream-16p-1kb.yaml apache-kafka-kafkawire`;

  selectSection(id: string): void {
    this.activeSectionId.set(id);
    this.isSidebarMobileOpen.set(false);
    if (typeof window !== 'undefined') {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }
  }

  toggleMobileSidebar(): void {
    this.isSidebarMobileOpen.update(v => !v);
  }

  scrollToAnchor(anchorId: string): void {
    if (typeof document !== 'undefined') {
      const el = document.getElementById(anchorId);
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }
  }

  copyCode(code: string, blockId: string): void {
    navigator.clipboard.writeText(code).then(() => {
      this.copiedCodeBlock.set(blockId);
      setTimeout(() => this.copiedCodeBlock.set(null), 2000);
    });
  }
}
