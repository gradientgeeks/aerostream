import { Component, signal, computed, ChangeDetectionStrategy, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink, ActivatedRoute } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatTooltipModule } from '@angular/material/tooltip';
import { TurbineLogoComponent } from '../logo/turbine-logo.component';

export interface ArchitectureNode {
  id: string;
  title: string;
  engine: 'Go Control Plane' | 'Rust Data Plane';
  icon: string;
  color: string;
  summary: string;
  details: string[];
}

@Component({
  selector: 'app-showcase',
  standalone: true,
  imports: [
    CommonModule,
    RouterLink,
    MatIconModule,
    MatButtonModule,
    MatTooltipModule,
    TurbineLogoComponent,
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './showcase.component.html',
  styleUrl: './showcase.component.scss'
})
export class ShowcaseComponent implements OnInit {
  private readonly route = inject(ActivatedRoute);
  readonly copiedCommand = signal<boolean>(false);
  readonly selectedNodeId = signal<string>('rust-storage');
  readonly selectedQuickstartTab = signal<'docker' | 'python' | 'go' | 'java' | 'rest'>('docker');
  readonly copiedSnippet = signal<string | null>(null);

  ngOnInit(): void {
    this.route.fragment.subscribe((fragment) => {
      if (fragment) {
        setTimeout(() => {
          const el = document.getElementById(fragment);
          if (el) {
            el.scrollIntoView({ behavior: 'smooth', block: 'start' });
          }
        }, 100);
      }
    });
  }

  readonly dockerSnippet = `services:
  controller:
    image: gradientgeeks/aerostream-controller:latest
    ports:
      - "9001:9001"   # HTTP REST & Schema Registry
      - "8001:8001"   # Internal gRPC & Raft Quorum
    command: ["-bootstrap", "-id", "node1", "-data-dir", "/data"]

  broker:
    image: gradientgeeks/aerostream-broker:latest
    ports:
      - "9091:9091"   # High-Speed Native TCP
      - "9092:9092"   # Kafka Wire Protocol
    command: ["--id", "1", "--controller", "http://controller:8001", "--storage-dir", "/data"]
    depends_on:
      - controller`;

  readonly pythonSnippet = `from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

future = producer.send('orders', {'order_id': 'ORD-9821', 'amount': 149.50})
record_metadata = future.get(timeout=10)
print(f"Delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")`;

  readonly goSnippet = `package main

import (
    "fmt"
    "github.com/gradientgeeks/aerostream/client"
)

func main() {
    c, err := client.NewClient("127.0.0.1:9091")
    if err != nil { panic(err) }
    defer c.Close()

    offset, err := c.Produce("orders", 0, []byte("order_payload_bytes"))
    fmt.Printf("Produced at offset %d with microsecond latency!\\n", offset)
}`;

  readonly javaSnippet = `spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: 1
    consumer:
      group-id: analytics-service
      auto-offset-reset: earliest`;

  readonly restSnippet = `# 1. Ingest record via HTTP
curl -X POST http://localhost:9001/api/produce \\
  -H "Content-Type: application/json" \\
  -d '{"topic": "orders", "partition": 0, "payload": "eyJrZXkiOiAidmFsdWUifQ=="}'

# 2. Register Schema via Confluent-compatible Schema Registry
curl -X POST http://localhost:9001/subjects/orders-value/versions \\
  -H "Content-Type: application/json" \\
  -d '{"schema": "{\\"type\\":\\"record\\",\\"name\\":\\"Order\\",\\"fields\\":[{\\"name\\":\\"id\\",\\"type\\":\\"string\\"}]}"}'`;

  readonly architectureNodes: ArchitectureNode[] = [
    {
      id: 'rust-storage',
      title: 'Zero-Copy Commit Log Storage',
      engine: 'Rust Data Plane',
      icon: 'sd_storage',
      color: '#ff5722',
      summary: 'High-throughput, crash-consistent append-only commit log with microsecond NVMe storage.',
      details: [
        'Single-syscall writes using FileExt::write_all_at without file-pointer mutex lock contention.',
        'Active-segment in-memory length tracking eliminates statx and lseek syscall overhead.',
        'Binary search in memory-mapped .idx files provides O(log N) offset-to-position translation.',
        'Linux sendfile(2) integration streams disk pages directly to client network sockets.'
      ]
    },
    {
      id: 'rust-kafka',
      title: 'Kafka Wire Protocol Listener',
      engine: 'Rust Data Plane',
      icon: 'sync_alt',
      color: '#00e5ff',
      summary: 'Drop-in Apache Kafka replacement on TCP port 9092 supporting standard client libraries.',
      details: [
        'Implements Kafka protocol API Keys: 0 (Produce), 1 (Fetch), 3 (Metadata), 18 (ApiVersions), 22 (InitProducerId).',
        'In-place base offset patching directly at disk boundary eliminates multi-megabyte heap cloning.',
        'Hardware-accelerated CRC32C (SSE4.2 / ARMv8) verifies batches at 10.9 GB/s.',
        'Connection-level reusable frame buffers avoid memory reallocation and page-fault stalls.'
      ]
    },
    {
      id: 'go-raft',
      title: 'Raft Quorum Consensus FSM',
      engine: 'Go Control Plane',
      icon: 'hub',
      color: '#00e5ff',
      summary: 'High-availability Raft consensus state machine governing cluster topology and partitions.',
      details: [
        'HashiCorp Raft implementation with quorum voting and automated leader failover.',
        'In-memory partition state machine with atomic BoltDB snapshotting and log compaction.',
        'Graceful Kubernetes scale-down with preStop hooks and automated partition draining.',
        'Green Tea Garbage Collector (Go 1.26) ensures <1 ms p99.9 GC pause times.'
      ]
    },
    {
      id: 'go-governance',
      title: 'Schema Registry & RBAC Engine',
      engine: 'Go Control Plane',
      icon: 'security',
      color: '#8b5cf6',
      summary: 'Confluent-compatible Schema Registry and enterprise zero-trust access control.',
      details: [
        'Built-in Schema Registry supporting Avro, Protobuf, and JSON schemas with BACKWARD/FULL compatibility checks.',
        'Enterprise RBAC with granular Topic/Group/Cluster ACLs and wildcard matching (e.g. orders-*).',
        'Cooperative Sticky Rebalance Protocol (KIP-848) eliminates stop-the-world rebalance storms.',
        'Native Stream Transforms engine executes sandboxed WASM filters and automated PII masking.'
      ]
    },
    {
      id: 'rust-tiered',
      title: 'Multi-Cloud Tiered Storage',
      engine: 'Rust Data Plane',
      icon: 'cloud_upload',
      color: '#ffb300',
      summary: 'Automated asynchronous offloading of cold segments to object storage for infinite retention.',
      details: [
        'Automated segment rolling based on 128 MB threshold or configurable retention policies.',
        'Pluggable multi-cloud object storage providers: AWS S3, MinIO, Google Cloud Storage (GCS), Azure Blob.',
        'Non-blocking background worker thread offloads closed segments without stalling hot active partitions.',
        'Transparent historical fetch reads seamless segments from cloud object tiers.'
      ]
    }
  ];

  readonly selectedNode = computed(() =>
    this.architectureNodes.find(n => n.id === this.selectedNodeId()) || this.architectureNodes[0]
  );

  copyDockerCommand(): void {
    const cmd = 'docker run -d -p 9092:9092 -p 9001:9001 gradientgeeks/aerostream:latest';
    navigator.clipboard.writeText(cmd).then(() => {
      this.copiedCommand.set(true);
      setTimeout(() => this.copiedCommand.set(false), 2000);
    });
  }

  copySnippet(code: string, id: string): void {
    navigator.clipboard.writeText(code).then(() => {
      this.copiedSnippet.set(id);
      setTimeout(() => this.copiedSnippet.set(null), 2000);
    });
  }

  selectNode(id: string): void {
    this.selectedNodeId.set(id);
  }

  selectTab(tab: 'docker' | 'python' | 'go' | 'java' | 'rest'): void {
    this.selectedQuickstartTab.set(tab);
  }
}
