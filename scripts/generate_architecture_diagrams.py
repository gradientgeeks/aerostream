#!/usr/bin/env python3
"""
AeroStream Architecture Diagram Architect
Generates Excalidraw v2 JSON, SVG Vector, and High-Resolution PNG Assets for:
1. dual_engine_architecture
2. shard_per_core_architecture
3. produce_fetch_pipeline
4. controller_broker_orchestration
5. cluster_topology_scale_down
6. tiered_storage_pipeline
7. native_and_kafka_dual_protocol
"""

import os
import sys
import json
import uuid
import subprocess
from typing import Tuple, List, Dict, Any

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOCS_DIAGRAMS = os.path.join(BASE_DIR, "docs", "diagrams")
CORE_DOCS_DIAGRAMS = os.path.join(BASE_DIR, "core", "docs", "diagrams")
DOCS_IMAGES = os.path.join(BASE_DIR, "docs", "images")
CORE_DOCS_IMAGES = os.path.join(BASE_DIR, "core", "docs", "images")

os.makedirs(DOCS_DIAGRAMS, exist_ok=True)
os.makedirs(CORE_DOCS_DIAGRAMS, exist_ok=True)
os.makedirs(DOCS_IMAGES, exist_ok=True)

WIDTH = 3200
HEIGHT = 2160

def xml_escape(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;"))

class DiagramArchitect:
    def __init__(self, title: str, subtitle: str, badges: list = None):
        self.title = title
        self.subtitle = subtitle
        self.badges = badges or []
        self._seed = 2000

        # Excalidraw structures
        self.exc_elements = []

        # SVG structures
        self.svg_defs = []
        self.svg_content = []
        self._init_svg_defs()

        # Add Header to Excalidraw
        self.add_exc_rect(60, 40, WIDTH - 120, 140, stroke_color="#334155", bg_color="#0f172a", stroke_width=2, roundness=3)
        self.add_exc_text(96, 75, self.title, font_size=28, color="#f8fafc", font_family=1)
        self.add_exc_text(96, 125, self.subtitle, font_size=16, color="#94a3b8", font_family=1)

    def _next_seed(self):
        self._seed += 1
        return self._seed

    def _init_svg_defs(self):
        self.svg_defs.append('''
    <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">
      <path d="M 40 0 L 0 0 0 40" fill="none" stroke="#1e293b" stroke-width="0.7" stroke-opacity="0.6"/>
      <circle cx="40" cy="40" r="1.2" fill="#334155" opacity="0.4"/>
    </pattern>
    <filter id="shadow" x="-5%" y="-5%" width="110%" height="115%" filterUnits="userSpaceOnUse">
      <feDropShadow dx="0" dy="12" stdDeviation="16" flood-color="#000000" flood-opacity="0.55"/>
    </filter>
    <marker id="arrow-cyan" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#38bdf8"/>
    </marker>
    <marker id="arrow-emerald" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#34d399"/>
    </marker>
    <marker id="arrow-amber" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#fbbf24"/>
    </marker>
    <marker id="arrow-purple" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#a855f7"/>
    </marker>
    <marker id="arrow-rose" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#fb7185"/>
    </marker>
    <linearGradient id="grad-card" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#1e293b" stop-opacity="0.9"/>
      <stop offset="100%" stop-color="#0f172a" stop-opacity="0.95"/>
    </linearGradient>
    <linearGradient id="grad-cyan-card" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="#0c4a6e" stop-opacity="0.4"/>
      <stop offset="100%" stop-color="#0f172a" stop-opacity="0.9"/>
    </linearGradient>
    <linearGradient id="grad-emerald-card" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="#064e3b" stop-opacity="0.4"/>
      <stop offset="100%" stop-color="#0f172a" stop-opacity="0.9"/>
    </linearGradient>
        ''')

    # --- EXCALIDRAW PRIMITIVES ---
    def add_exc_rect(self, x, y, w, h, stroke_color="#334155", bg_color="#1e293b",
                     stroke_width=2, stroke_style="solid", fill_style="solid", roundness=3):
        el_id = str(uuid.uuid4())[:8]
        el = {
            "id": el_id,
            "type": "rectangle",
            "x": x,
            "y": y,
            "width": w,
            "height": h,
            "angle": 0,
            "strokeColor": stroke_color,
            "backgroundColor": bg_color,
            "fillStyle": fill_style,
            "strokeWidth": stroke_width,
            "strokeStyle": stroke_style,
            "roughness": 0,
            "opacity": 100,
            "groupIds": [],
            "frameId": None,
            "roundness": {"type": roundness} if roundness else None,
            "seed": self._next_seed(),
            "version": 1,
            "versionNonce": 1,
            "isDeleted": False,
            "boundElements": None,
            "updated": 1,
            "link": None,
            "locked": False
        }
        self.exc_elements.append(el)
        return el_id

    def add_exc_text(self, x, y, text, font_size=16, color="#f8fafc", align="left", font_family=1):
        el_id = str(uuid.uuid4())[:8]
        lines = str(text).split("\n")
        max_line_len = max(len(l) for l in lines) if lines else 1
        w = max(40, int(max_line_len * (font_size * 0.6)))
        h = max(20, int(len(lines) * (font_size * 1.35)))
        el = {
            "id": el_id,
            "type": "text",
            "x": x,
            "y": y,
            "width": w,
            "height": h,
            "angle": 0,
            "strokeColor": color,
            "backgroundColor": "transparent",
            "fillStyle": "solid",
            "strokeWidth": 1,
            "strokeStyle": "solid",
            "roughness": 0,
            "opacity": 100,
            "groupIds": [],
            "frameId": None,
            "roundness": None,
            "seed": self._next_seed(),
            "version": 1,
            "versionNonce": 1,
            "isDeleted": False,
            "boundElements": None,
            "updated": 1,
            "link": None,
            "locked": False,
            "text": text,
            "fontSize": font_size,
            "fontFamily": font_family,
            "textAlign": align,
            "verticalAlign": "top",
            "baseline": int(font_size * 0.85),
            "containerId": None,
            "originalText": text,
            "lineHeight": 1.25
        }
        self.exc_elements.append(el)
        return el_id

    def add_exc_arrow(self, x1, y1, x2, y2, color="#38bdf8", stroke_width=2, stroke_style="solid"):
        el_id = str(uuid.uuid4())[:8]
        dx = x2 - x1
        dy = y2 - y1
        el = {
            "id": el_id,
            "type": "arrow",
            "x": x1,
            "y": y1,
            "width": abs(dx),
            "height": abs(dy),
            "angle": 0,
            "strokeColor": color,
            "backgroundColor": "transparent",
            "fillStyle": "solid",
            "strokeWidth": stroke_width,
            "strokeStyle": stroke_style,
            "roughness": 0,
            "opacity": 100,
            "groupIds": [],
            "frameId": None,
            "roundness": {"type": 2},
            "seed": self._next_seed(),
            "version": 1,
            "versionNonce": 1,
            "isDeleted": False,
            "boundElements": None,
            "updated": 1,
            "link": None,
            "locked": False,
            "points": [[0, 0], [dx, dy]],
            "lastCommittedPoint": None,
            "startBinding": None,
            "endBinding": None,
            "startArrowhead": None,
            "endArrowhead": "arrow"
        }
        self.exc_elements.append(el)
        return el_id

    # --- HIGH-LEVEL COMPONENT BUILDERS (BUILD BOTH SVG & EXCALIDRAW) ---
    def add_container(self, x, y, w, h, title, subtitle="",
                      accent_color="#38bdf8", bg_fill="url(#grad-card)",
                      border_color="#0284c7", badge=""):
        # SVG
        filt_attr = 'filter="url(#shadow)"'
        self.svg_content.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{bg_fill}" stroke="{border_color}" stroke-width="3" {filt_attr}/>'
        )
        self.svg_content.append(
            f'<text x="{x+36}" y="{y+54}" fill="{accent_color}" font-size="28" font-family="sans-serif" font-weight="800">{xml_escape(title)}</text>'
        )
        if subtitle:
            self.svg_content.append(
                f'<text x="{x+36}" y="{y+86}" fill="#94a3b8" font-size="16" font-family="sans-serif" font-weight="500">{xml_escape(subtitle)}</text>'
            )
        if badge:
            bw = len(badge) * 8 + 24
            bx = x + w - bw - 36
            self.svg_content.append(
                f'<rect x="{bx}" y="{y+32}" width="{bw}" height="32" rx="8" fill="#0f172a" stroke="{accent_color}" stroke-width="1.5"/>'
            )
            self.svg_content.append(
                f'<text x="{bx + bw/2}" y="{y+53}" fill="{accent_color}" font-size="14" font-family="sans-serif" font-weight="700" text-anchor="middle">{xml_escape(badge)}</text>'
            )

        # Excalidraw
        self.add_exc_rect(x, y, w, h, stroke_color=border_color, bg_color="#0b1329", stroke_width=3, roundness=3)
        self.add_exc_text(x + 36, y + 36, title, font_size=24, color=accent_color)
        if subtitle:
            self.add_exc_text(x + 36, y + 70, subtitle, font_size=15, color="#94a3b8")

    def add_card(self, x, y, w, h, title, items=None, subtitle="",
                 accent_color="#38bdf8", bg_fill="url(#grad-card)",
                 border_color="#334155", badge="", badge_color="#0369a1",
                 code_block=""):
        # SVG
        self.svg_content.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{bg_fill}" stroke="{border_color}" stroke-width="2" filter="url(#shadow)"/>'
        )
        self.svg_content.append(
            f'<path d="M {x+12} {y} L {x+w-12} {y}" stroke="{accent_color}" stroke-width="3" stroke-linecap="round"/>'
        )
        self.svg_content.append(
            f'<text x="{x+24}" y="{y+36}" fill="{accent_color}" font-size="20" font-family="sans-serif" font-weight="700">{xml_escape(title)}</text>'
        )

        if badge:
            bw = len(badge) * 8 + 20
            self.svg_content.append(
                f'<rect x="{x+w-bw-20}" y="{y+18}" width="{bw}" height="26" rx="6" fill="{badge_color}"/>'
            )
            self.svg_content.append(
                f'<text x="{x+w-bw/2-20}" y="{y+36}" fill="#f8fafc" font-size="13" font-family="sans-serif" font-weight="600" text-anchor="middle">{xml_escape(badge)}</text>'
            )

        ty = y + 66
        if subtitle:
            self.svg_content.append(
                f'<text x="{x+24}" y="{ty}" fill="#94a3b8" font-size="14" font-family="sans-serif" font-weight="500">{xml_escape(subtitle)}</text>'
            )
            ty += 28

        if items:
            for item in items:
                self.svg_content.append(f'<circle cx="{x+26}" cy="{ty-5}" r="3" fill="{accent_color}"/>')
                self.svg_content.append(
                    f'<text x="{x+38}" y="{ty}" fill="#cbd5e1" font-size="15" font-family="sans-serif">{xml_escape(item)}</text>'
                )
                ty += 28

        if code_block:
            code_lines = code_block.strip().split("\n")
            cb_h = len(code_lines) * 22 + 18
            self.svg_content.append(
                f'<rect x="{x+20}" y="{ty+6}" width="{w-40}" height="{cb_h}" rx="8" fill="#090d16" stroke="#1e293b" stroke-width="1"/>'
            )
            cy = ty + 28
            for line in code_lines:
                self.svg_content.append(
                    f'<text x="{x+34}" y="{cy}" fill="#38bdf8" font-size="13" font-family="monospace">{xml_escape(line)}</text>'
                )
                cy += 22

        # Excalidraw
        self.add_exc_rect(x, y, w, h, stroke_color=border_color, bg_color="#131d31", stroke_width=2, roundness=3)
        self.add_exc_text(x + 20, y + 16, title, font_size=18, color=accent_color)
        ey = y + 44
        if subtitle:
            self.add_exc_text(x + 20, ey, subtitle, font_size=13, color="#94a3b8")
            ey += 24
        if items:
            for item in items:
                self.add_exc_text(x + 20, ey, "• " + item, font_size=14, color="#cbd5e1")
                ey += 22
        if code_block:
            self.add_exc_text(x + 20, ey + 6, code_block, font_size=12, color="#38bdf8", font_family=3)

    def add_arrow(self, x1, y1, x2, y2, color="#38bdf8", stroke_width=3.0,
                  dashed=False, marker="arrow-cyan", label=""):
        # SVG
        dash_attr = 'stroke-dasharray="6,6"' if dashed else ''
        marker_attr = f'marker-end="url(#{marker})"' if marker else ''
        self.svg_content.append(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{stroke_width}" {dash_attr} {marker_attr}/>'
        )
        if label:
            mx = (x1 + x2) / 2
            my = (y1 + y2) / 2 - 10
            lw = len(label) * 8 + 20
            self.svg_content.append(
                f'<rect x="{mx - lw/2}" y="{my - 16}" width="{lw}" height="24" rx="6" fill="#0b0f19" stroke="{color}" stroke-width="1.5"/>'
            )
            self.svg_content.append(
                f'<text x="{mx}" y="{my + 1}" fill="#f8fafc" font-size="13" font-family="sans-serif" font-weight="700" text-anchor="middle">{xml_escape(label)}</text>'
            )

        # Excalidraw
        self.add_exc_arrow(x1, y1, x2, y2, color=color, stroke_width=int(stroke_width))
        if label:
            mx = (x1 + x2) / 2
            my = (y1 + y2) / 2 - 20
            self.add_exc_text(mx - len(label)*3, my, label, font_size=12, color=color)

    def export_svg(self) -> str:
        svg_parts = []
        svg_parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">')
        svg_parts.append('<defs>')
        svg_parts.extend(self.svg_defs)
        svg_parts.append('</defs>')
        svg_parts.append(f'<rect width="{WIDTH}" height="{HEIGHT}" fill="#0b0f19"/>')
        svg_parts.append(f'<rect width="{WIDTH}" height="{HEIGHT}" fill="url(#grid)"/>')

        # Header box
        svg_parts.append(f'<rect x="60" y="40" width="{WIDTH-120}" height="140" rx="14" fill="#0f172a" stroke="#334155" stroke-width="2" filter="url(#shadow)"/>')
        svg_parts.append(f'<text x="96" y="98" fill="#f8fafc" font-size="34" font-family="sans-serif" font-weight="800">{xml_escape(self.title)}</text>')
        svg_parts.append(f'<text x="96" y="142" fill="#94a3b8" font-size="18" font-family="sans-serif" font-weight="500">{xml_escape(self.subtitle)}</text>')

        # Badges
        cur_bx = WIDTH - 100
        for badge_text, bg_col, text_col, border_col in reversed(self.badges):
            font_size = 14
            pad_x = 16
            w = int(len(badge_text) * 8.5 + pad_x * 2)
            h = 36
            cur_bx -= w + 16
            svg_parts.append(f'<rect x="{cur_bx}" y="92" width="{w}" height="{h}" rx="8" fill="{bg_col}" stroke="{border_col}" stroke-width="1.5"/>')
            svg_parts.append(f'<text x="{cur_bx + w/2}" y="115" fill="{text_col}" font-size="{font_size}" font-family="sans-serif" font-weight="700" text-anchor="middle">{xml_escape(badge_text)}</text>')

        # Main content
        svg_parts.extend(self.svg_content)

        # Footer
        svg_parts.append(f'<line x1="60" y1="{HEIGHT-50}" x2="{WIDTH-60}" y2="{HEIGHT-50}" stroke="#1e293b" stroke-width="1"/>')
        svg_parts.append(f'<text x="96" y="{HEIGHT-25}" fill="#64748b" font-size="14" font-family="sans-serif">AeroStream Next-Gen Distributed Streaming Platform • Excalidraw v2 Verified Architecture Spec</text>')
        svg_parts.append(f'<text x="{WIDTH-96}" y="{HEIGHT-25}" fill="#64748b" font-size="14" font-family="sans-serif" text-anchor="end">Production Engineered • High-Resolution 3200×2160</text>')

        svg_parts.append('</svg>')
        return "\n".join(svg_parts)

    def export_excalidraw(self) -> dict:
        return {
            "type": "excalidraw",
            "version": 2,
            "source": "https://excalidraw.com",
            "elements": self.exc_elements,
            "appState": {
                "gridSize": None,
                "viewBackgroundColor": "#0b0f19"
            },
            "files": {}
        }


# ==============================================================================
# DIAGRAM GENERATORS
# ==============================================================================

def build_dual_engine() -> Tuple[str, dict]:
    title = "AeroStream Dual-Engine Architecture"
    subtitle = "Go 1.26 Control Plane (Consensus, Registry, RBAC) & Rust 1.98.1 Data Plane (Zero-Copy Storage Kernel)"
    badges = [
        ("Dual-Engine Architecture", "#0369a1", "#e0f2fe", "#38bdf8"),
        ("Go 1.26 Control", "#0284c7", "#ffffff", "#0ea5e9"),
        ("Rust 1.98.1 Kernel", "#059669", "#ffffff", "#10b981"),
        ("gRPC Stream 8001", "#4f46e5", "#ffffff", "#6366f1"),
        ("Kafka Wire 9092", "#d97706", "#ffffff", "#f59e0b")
    ]
    diag = DiagramArchitect(title, subtitle, badges)

    # 1. Left Container: Go 1.26 Control Plane
    c_x, c_y, c_w, c_h = 60, 220, 1480, 1850
    diag.add_container(c_x, c_y, c_w, c_h, "GO 1.26 CONTROL PLANE",
                       "Cluster Metadata State Machine, Raft Quorum Consensus, Security & Administration",
                       accent_color="#38bdf8", bg_fill="url(#grad-cyan-card)", border_color="#0284c7", badge="Port 7001 / 8001 / 9001")

    diag.add_card(c_x + 40, c_y + 120, 680, 250, "Web Console & Management API",
                  items=[
                      "REST API endpoints (POST /api/topics, GET /api/cluster)",
                      "Angular 18 Enterprise Admin Web Dashboard",
                      "Prometheus metrics scraping endpoint (/metrics)",
                      "Broker scale-down drain triggers (/api/brokers/{id}/drain)"
                  ],
                  subtitle="Port 9001 • HTTP/1.1 & HTTP/2 REST Gateway",
                  accent_color="#38bdf8", badge="REST / UI", badge_color="#0369a1")

    diag.add_card(c_x + 760, c_y + 120, 680, 250, "Confluent-Compatible Schema Registry",
                  items=[
                      "Full Schema Registry v1 REST API compatibility",
                      "Avro, Protocol Buffers, and JSON Schema validators",
                      "Schema evolution rules: BACKWARD, FORWARD, FULL",
                      "Fast schema ID cache with Swiss Table lookups"
                  ],
                  subtitle="Port 9001 • Confluent Wire Format Compatible",
                  accent_color="#38bdf8", badge="Registry", badge_color="#0369a1")

    diag.add_card(c_x + 40, c_y + 400, 680, 360, "Raft Consensus Engine",
                  items=[
                      "Port 7001 • 3-Node / 5-Node Raft Consensus Quorum",
                      "HashiCorp Raft algorithm implementation in Go",
                      "Leader Election, Term Management, Heartbeats",
                      "Replicated commit log with log compaction & snapshots",
                      "Zero-downtime failover with sub-second re-election",
                      "Linearizable state reads via ReadIndex verification"
                  ],
                  subtitle="Port 7001 • Distributed Log Replication",
                  accent_color="#818cf8", badge="Raft Consensus", badge_color="#4338ca")

    diag.add_card(c_x + 760, c_y + 400, 680, 360, "Metadata State Machine (FSM) & BoltDB",
                  items=[
                      "ACID embedded transactional storage: cluster_state.db",
                      "Persistent topic definitions, partition counts, replicas",
                      "In-Sync Replicas (ISR) set tracking and High Watermark",
                      "Dynamic broker discovery and rack-aware layouts",
                      "B+ Tree indexed key-value engine with zero external deps",
                      "Snapshot recovery and fast bootstrap replay on restart"
                  ],
                  subtitle="Embedded BoltDB • Local B+ Tree ACID Engine",
                  accent_color="#818cf8", badge="BoltDB FSM", badge_color="#4338ca")

    diag.add_card(c_x + 40, c_y + 790, 680, 360, "RBAC & Granular ACL Swiss Tables",
                  items=[
                      "Swiss Table hash map for O(1) concurrent authorization",
                      "Principal authentication: mTLS, SASL/PLAIN, SASL/SCRAM",
                      "Role-Based Access Control: Superuser, Admin, Producer, Consumer",
                      "Topic-level & Consumer Group-level fine-grained ACLs",
                      "Dynamic ACL propagation to Rust brokers via gRPC"
                  ],
                  subtitle="High-Performance Hashbrown In-Memory Tables",
                  accent_color="#38bdf8", badge="Security", badge_color="#0369a1")

    diag.add_card(c_x + 760, c_y + 790, 680, 360, "Cooperative Sticky Rebalance Engine",
                  items=[
                      "Incremental cooperative consumer group partition assignments",
                      "Sticky partition migration minimizing client rebalance latency",
                      "Broker drain orchestrator with automatic ISR re-election",
                      "Client quota enforcement (produce/fetch byte rate limits)",
                      "Automated partition leader balance across cluster racks"
                  ],
                  subtitle="Cooperative Sticky Protocol • KIP-429 Compliant",
                  accent_color="#38bdf8", badge="Coordinator", badge_color="#0369a1")

    diag.add_card(c_x + 40, c_y + 1180, 1400, 240, "gRPC Controller Server (ControlService)",
                  items=[
                      "Port 8001 • Bidirectional HTTP/2 Multiplexed gRPC Channel to Data Plane Brokers",
                      "RegisterBroker RPC: broker registration, host advertisement, rack allocation, storage config",
                      "Periodic Heartbeat RPC (2s ticker): collects LEO, lag telemetry, disk capacity, and broker liveness",
                      "HeartbeatResponse piggybacking: sends updated partition leadership, quota tables, topic configs",
                      "Failure detection sweeper (every 3s): detects 8s lease expiry and triggers automated leader election"
                  ],
                  subtitle="Port 8001 • Bidirectional Control Plane Highway",
                  accent_color="#38bdf8", badge="gRPC Port 8001", badge_color="#0284c7")

    # 2. Right Container: Rust 1.98.1 Data Plane
    d_x, d_y, d_w, d_h = 1660, 220, 1480, 1850
    diag.add_container(d_x, d_y, d_w, d_h, "RUST 1.98.1 DATA PLANE",
                       "Zero-Copy Append-Only Storage Kernel, Shard-per-Core Engine, Dual Protocol",
                       accent_color="#34d399", bg_fill="url(#grad-emerald-card)", border_color="#059669", badge="Port 9091 / 9092")

    diag.add_card(d_x + 40, d_y + 120, 680, 250, "Kafka Wire Protocol Server",
                  items=[
                      "Port 9092 • Drop-in Apache Kafka Binary Protocol",
                      "ApiKeys 0 through 36 (Produce, Fetch, Metadata, Txn)",
                      "Zero translation proxy or sidecar overhead",
                      "SASL/PLAIN, SASL/SCRAM, and SSL/TLS support",
                      "Fetch v0-v11 with KIP-392 rack-aware follower fetch"
                  ],
                  subtitle="Port 9092 • Full Native Compatibility",
                  accent_color="#fbbf24", badge="Kafka 9092", badge_color="#b45309")

    diag.add_card(d_x + 760, d_y + 120, 680, 250, "AeroStream Native High-Speed Protocol",
                  items=[
                      "Port 9091 • Ultra-compact 7-byte binary header",
                      "Framing: [0xAE, 0x01][cmd: u8][body_len: u32 BE]",
                      "Sub-millisecond p99.9 latency, zero header bloat",
                      "Commands: Produce(1), Fetch(2), ReplicaFetch(3), Multi(4)",
                      "Official client SDKs: Go, Rust, Java, .NET, Node.js"
                  ],
                  subtitle="Port 9091 • 0xAE 0x01 Binary Framing",
                  accent_color="#34d399", badge="Native 9091", badge_color="#047857")

    diag.add_card(d_x + 40, d_y + 400, 680, 360, "Shard-per-Core Execution Engine",
                  items=[
                      "Dedicated OS threads pinned via libc::sched_setaffinity",
                      "Deterministic routing: S = (Hash64(topic) ^ partition) % N_shards",
                      "Lock-free flume bounded MPMC actor channels",
                      "Zero mutexes, zero RwLocks, zero atomic spinlocks",
                      "Exclusive HashMap<PartitionKey, PartitionLog> per core",
                      "Guaranteed L1/L2 data & instruction cache locality"
                  ],
                  subtitle="Thread-per-Core Architecture • Zero Contention",
                  accent_color="#34d399", badge="Shard-per-Core", badge_color="#047857")

    diag.add_card(d_x + 760, d_y + 400, 360, 360, "Append-Only Log Kernel",
                  items=[
                      "Active segment mmap append log",
                      "Sparse index (.idx) binary search",
                      "Timestamp index (.timeindex)",
                      "Single-syscall write_all_at",
                      "CRC32C hardware acceleration (SSE4.2)",
                      "In-place base offset patching"
                  ],
                  subtitle="Physical PartitionLog Storage",
                  accent_color="#34d399", badge="Storage Kernel", badge_color="#047857")

    diag.add_card(d_x + 1140, d_y + 400, 300, 360, "I/O Kernel Directives",
                  items=[
                      "Linux sendfile(2) DMA",
                      "Zero userspace copying",
                      "Page cache passthrough",
                      "sync_file_range(2)",
                      "Paced writeback scheduler",
                      "No dirty page flush storms"
                  ],
                  subtitle="Linux Kernel Optimized",
                  accent_color="#34d399", badge="Zero-Copy DMA", badge_color="#047857")

    diag.add_card(d_x + 40, d_y + 790, 680, 360, "In-Broker Transforms & Compaction",
                  items=[
                      "Lightweight stream transformations without external flink/spark",
                      "Wasm runtime & native transform execution pipelines",
                      "Background log compaction thread with clean/dirty ratio checks",
                      "Key-deduplication retaining latest record per message key",
                      "Zero allocation byte-slice filtering on ingress records"
                  ],
                  subtitle="In-Line Stream Enrichment & Deduplication",
                  accent_color="#34d399", badge="Transforms", badge_color="#047857")

    diag.add_card(d_x + 760, d_y + 790, 680, 360, "Multi-Cloud Tiered Storage Offload",
                  items=[
                      "Seamless local NVMe rollover to cloud object storage",
                      "Async OffloadTask queue powered by worker thread pools",
                      "Supported providers: AWS S3, MinIO, Google Cloud GCS, Azure Blob",
                      "Transparent cold segment retrieval on historical fetch requests",
                      "Local disk space preservation with LRU cache eviction"
                  ],
                  subtitle="Hybrid Hot NVMe & Cold Cloud Storage",
                  accent_color="#fbbf24", badge="Tiered Storage", badge_color="#b45309")

    diag.add_card(d_x + 40, d_y + 1180, 1400, 240, "Broker Orchestration & Replication Client",
                  items=[
                      "Continuous gRPC background client connected to Go Controller on Port 8001",
                      "Reports Log End Offsets (LEO), partition lag, and storage health every 2 seconds",
                      "Receives dynamic configuration updates (quotas, topic compression codecs)",
                      "Automated partition leader/follower transitions directed by Controller",
                      "Follower replica replication loop: issues Native Cmd 3 ReplicaFetch to leader broker"
                  ],
                  subtitle="HTTP/2 gRPC Channel • Automated Broker Lifecycle",
                  accent_color="#34d399", badge="gRPC Client", badge_color="#047857")

    # Center Bridge Connectors
    diag.add_arrow(1540, 1480, 1660, 1480, color="#38bdf8", stroke_width=4, marker="arrow-cyan", label="Heartbeat & Telemetry (2s)")
    diag.add_arrow(1660, 1540, 1540, 1540, color="#34d399", stroke_width=4, marker="arrow-emerald", label="Configs & Assignments Push")

    return diag.export_svg(), diag.export_excalidraw()


def build_shard_per_core() -> Tuple[str, dict]:
    title = "AeroStream Shard-per-Core (Thread-per-Core) Storage Architecture"
    subtitle = "Physical Core Pinning, Lock-Free Actor Model, Deterministic Hash Routing, and Zero Contention"
    badges = [
        ("Shard-per-Core Engine", "#047857", "#ffffff", "#34d399"),
        ("libc::sched_setaffinity", "#0284c7", "#ffffff", "#38bdf8"),
        ("flume Bounded Channels", "#7c3aed", "#ffffff", "#a78bfa"),
        ("Deterministic Routing", "#b45309", "#ffffff", "#fbbf24"),
        ("Zero Mutex Contention", "#065f46", "#ffffff", "#10b981")
    ]
    diag = DiagramArchitect(title, subtitle, badges)

    top_y = 220
    diag.add_container(60, top_y, 3080, 270, "1. MULTI-THREADED TOKIO NETWORK INGRESS LAYER",
                       "Accepts client TCP connections concurrently on Ports 9091 (Native) and 9092 (Kafka Wire Protocol)",
                       accent_color="#38bdf8", border_color="#334155", badge="Network Ingress")

    diag.add_card(100, top_y + 100, 700, 150, "Client Connection Pool",
                  items=["Thousands of concurrent TCP sockets", "TLS termination (tokio-rustls)", "Non-blocking frame reading"],
                  accent_color="#38bdf8", badge="Network I/O", badge_color="#0369a1")

    diag.add_card(840, top_y + 100, 1100, 150, "Deterministic Shard Router",
                  items=[
                      "Deterministic Hash: S = (Hash64(topic) ^ partition) % N_shards",
                      "Zero Cross-Shard Shuffling: 100% of operations for (topic, partition) hit the EXACT same core",
                      "O(1) Routing Table: zero lock contention during routing calculation"
                  ],
                  accent_color="#fbbf24", badge="ShardRouter", badge_color="#b45309")

    diag.add_card(1980, top_y + 100, 1120, 150, "Lock-Free Actor Mailbox (flume channels)",
                  items=[
                      "Request dispatch: flume::bounded(1024) cross-thread channels",
                      "Payload: ShardRequest (Append, ReadFromOffset, EnsurePartition)",
                      "Async completion: tokio::sync::oneshot::Sender<Result<T, io::Error>>"
                  ],
                  accent_color="#a855f7", badge="flume MPMC", badge_color="#6b21a8")

    mid_y = 520
    diag.add_container(60, mid_y, 3080, 1260, "2. HARDWARE CORE PINNING & THREAD-PER-CORE EXECUTION UNITS",
                       "Each OS thread is permanently bound to a dedicated physical CPU core via libc::sched_setaffinity (zero thread migration, 100% L1/L2 cache locality)",
                       accent_color="#34d399", border_color="#059669", badge="NUMA Pinned Cores")

    shard_w = 720
    shard_gap = 40
    start_x = 100

    cores_info = [
        ("Core 0 • NUMA Node 0", "shard-0", "#38bdf8", "#0284c7", ["orders-0", "orders-4", "payments-0"]),
        ("Core 1 • NUMA Node 0", "shard-1", "#34d399", "#059669", ["orders-1", "orders-5", "inventory-0"]),
        ("Core 2 • NUMA Node 1", "shard-2", "#fbbf24", "#d97706", ["orders-2", "payments-1", "telemetry-0"]),
        ("Core 3 • NUMA Node 1", "shard-3", "#a855f7", "#7c3aed", ["orders-3", "inventory-1", "telemetry-1"])
    ]

    for i, (core_title, thread_name, color, dark_col, partitions) in enumerate(cores_info):
        sx = start_x + i * (shard_w + shard_gap)
        sy = mid_y + 120

        diag.add_card(sx, sy, shard_w, 1100, core_title, subtitle="Thread-per-Core Isolated Execution Unit",
                      accent_color=color, border_color=color, badge=thread_name, badge_color=dark_col)

        diag.add_card(sx + 20, sy + 70, shard_w - 40, 140, "Hardware Core Pinning",
                      items=[
                          "libc::CPU_SET(core_id, &mut cpuset)",
                          "libc::sched_setaffinity(0, len, &cpuset)",
                          "Thread-isolated L1d / L1i / L2 cache",
                          "Zero OS scheduler context switching"
                      ],
                      accent_color=color, badge="Pinning", badge_color=dark_col)

        diag.add_card(sx + 20, sy + 230, shard_w - 40, 120, "Lock-Free Mailbox",
                      items=[
                          "flume::Receiver<ShardRequest>",
                          "Non-blocking batch drain",
                          "Zero cross-core memory bus locking"
                      ],
                      accent_color=color, badge="Mailbox", badge_color=dark_col)

        diag.add_card(sx + 20, sy + 370, shard_w - 40, 240, "Owned PartitionLog Map",
                      items=[
                          "HashMap<PartitionKey, PartitionLog>",
                          "ZERO Mutex / ZERO RwLock / ZERO Atomics",
                          f"Assigned Partitions: {', '.join(partitions)}",
                          "Sequential, deterministically ordered mutation",
                          "Independent High Watermark & LEO tracking"
                      ],
                      accent_color=color, badge="Zero Locks", badge_color=dark_col)

        diag.add_card(sx + 20, sy + 630, shard_w - 40, 430, "Single-Thread Execution Kernel",
                      items=[
                          "1. Offset Verification & Next Offset Allocation",
                          "2. In-Place Base Offset Patching directly in RAM",
                          "3. Hardware CRC32C (SSE4.2 hardware intrinsics)",
                          "4. mmap Append into active 000...00.log",
                          "5. Paced sync_file_range(2) dirty page flush",
                          "6. Sparse .idx Index binary search / append",
                          "7. Dispatch result via oneshot sender channel"
                      ],
                      subtitle="Deterministic Zero-Contention Pipeline",
                      accent_color=color, badge="Storage Kernel", badge_color=dark_col)

        # Ingress arrow to core
        diag.add_arrow(1390, top_y + 250, sx + shard_w // 2, sy, color=color, stroke_width=2.5, dashed=True, marker="arrow-cyan")

    bot_y = 1820
    diag.add_container(60, bot_y, 3080, 270, "3. STORAGE HARDWARE & OS PAGE CACHE LAYER",
                       "Direct NVMe Storage I/O with Paced sync_file_range Writeback and sendfile DMA Zero-Copy Kernel Transfer",
                       accent_color="#34d399", border_color="#334155", badge="NVMe & Kernel")

    diag.add_card(100, bot_y + 100, 950, 140, "Active Memory-Mapped Log (.log)",
                  items=["Direct mmap memory writes", "Sequential append without seek penalties", "Configurable max_segment_size (1GB rolls)"],
                  accent_color="#34d399", badge="NVMe Append", badge_color="#065f46")

    diag.add_card(1100, bot_y + 100, 950, 140, "Paced Writeback Scheduler",
                  items=["sync_file_range(SYNC_FILE_RANGE_WRITE)", "Prevents Linux pdflush dirty page writeback storms", "Flat sub-millisecond p99.9 latency curves"],
                  accent_color="#fbbf24", badge="Paced Writeback", badge_color="#b45309")

    diag.add_card(2100, bot_y + 100, 1000, 140, "Sparse Index Files (.idx & .timeindex)",
                  items=["12-byte fixed entries: 8-byte rel offset + 4-byte pos", "O(log N) binary search for instant seek resolution", "Zero memory overhead index caching"],
                  accent_color="#38bdf8", badge="Sparse Index", badge_color="#0369a1")

    return diag.export_svg(), diag.export_excalidraw()


def build_produce_fetch_pipeline() -> Tuple[str, dict]:
    title = "AeroStream Zero-Copy Produce & Fetch Pipeline"
    subtitle = "Hardware-Accelerated Ingestion (CRC32C SSE4.2) & Kernel DMA Zero-Copy Read Path (sendfile)"
    badges = [
        ("Zero-Copy Pipeline", "#059669", "#ffffff", "#34d399"),
        ("CRC32C SSE4.2", "#0284c7", "#ffffff", "#38bdf8"),
        ("In-Place Patching", "#d97706", "#ffffff", "#fbbf24"),
        ("Linux sendfile(2)", "#e11d48", "#ffffff", "#fb7185"),
        ("Direct DMA Transfer", "#7c3aed", "#ffffff", "#a78bfa")
    ]
    diag = DiagramArchitect(title, subtitle, badges)

    wy = 220
    diag.add_container(60, wy, 3080, 880, "WRITE PATH: HIGH-THROUGHPUT PRODUCE PIPELINE",
                       "Hardware-accelerated validation, in-place batch header patching, mmap append, and paced background writeback",
                       accent_color="#38bdf8", border_color="#0284c7", badge="Write Path (Produce)")

    step_w = 470
    step_gap = 36
    px = 100

    produce_steps = [
        ("Step 1: Ingestion", "Client Ingress", "#38bdf8", [
            "Kafka Client (Port 9092) or Native Client (Port 9091)",
            "Tokio non-blocking socket read",
            "Batch header framing validation",
            "Magic prefix verification (0xAE 0x01)"
        ]),
        ("Step 2: Hardware CRC", "SSE4.2 Acceleration", "#38bdf8", [
            "Hardware-accelerated CRC32C",
            "Intel SSE4.2 / ARM CRC32 instructions",
            "Validates record checksum in nanoseconds",
            "Zero CPU memory bandwidth stalls"
        ]),
        ("Step 3: State Tracker", "Producer State Check", "#fbbf24", [
            "Idempotent producer ID allocation",
            "Sequence number check: seq == expected",
            "Deduplicates in-flight network retries",
            "Rejects out-of-order batches (Error 45)"
        ]),
        ("Step 4: Offset Patching", "In-Place Batch Header", "#34d399", [
            "Shard allocates monotonic base offset",
            "Patches base offset directly into memory buffer",
            "Single-pass write without reallocations",
            "Eliminates buffer copy / serialize overhead"
        ]),
        ("Step 5: Storage Kernel", "mmap Log Append", "#34d399", [
            "Appends batch to active segment .log",
            "Single-syscall write_all_at / mmap slice",
            "Updates 12-byte sparse index (.idx)",
            "Paced writeback via sync_file_range(2)"
        ]),
        ("Step 6: Completion", "High Watermark & ACK", "#a855f7", [
            "Log End Offset (LEO) incremented",
            "High Watermark (HW) advanced on replication",
            "Produce ACK returned to producer client",
            "Sub-millisecond p99.9 produce latency"
        ])
    ]

    for i, (stitle, ssub, scolor, sitems) in enumerate(produce_steps):
        cx = px + i * (step_w + step_gap)
        diag.add_card(cx, wy + 130, step_w, 480, stitle, items=sitems, subtitle=ssub, accent_color=scolor, badge=f"0{i+1}", badge_color="#0f172a")
        if i < len(produce_steps) - 1:
            diag.add_arrow(cx + step_w, wy + 370, cx + step_w + step_gap, wy + 370, color="#38bdf8", stroke_width=3, marker="arrow-cyan")

    diag.add_card(100, wy + 640, 1440, 220, "In-Place Base Offset Patching & Zero Re-allocation",
                  items=[
                      "Traditional brokers deserialize record batches, update offsets, and re-serialize into a new buffer.",
                      "AeroStream performs direct binary patching: offsets are written directly into known byte offsets in-place.",
                      "Guarantees exactly ONE copy of message bytes from network socket directly to kernel page cache."
                  ],
                  accent_color="#34d399", badge="In-Place Patching", badge_color="#059669")

    diag.add_card(1580, wy + 640, 1480, 220, "Paced sync_file_range Dirty Page Writeback",
                  items=[
                      "Calls sync_file_range(fd, pos, len, SYNC_FILE_RANGE_WRITE) to asynchronously initiate writeback.",
                      "Prevents Linux kernel pdflush dirty-page background writeback spikes from blocking user space.",
                      "Smooths SSD/NVMe wear leveling and ensures deterministic real-time tail latencies."
                  ],
                  accent_color="#fbbf24", badge="Kernel I/O", badge_color="#b45309")

    ry = 1140
    diag.add_container(60, ry, 3080, 930, "READ PATH: LINUX SENDFILE(2) ZERO-COPY FETCH PIPELINE",
                       "Direct Linux Page Cache to Network Socket DMA Transfer without touching user space memory",
                       accent_color="#fb7185", border_color="#e11d48", badge="Read Path (Fetch)")

    fetch_steps = [
        ("Step 1: Fetch Request", "Client Fetch Ingress", "#fb7185", [
            "Consumer FetchRequest (Topic, Partition, Offset, MaxBytes)",
            "Long-polling support (max_wait_ms)",
            "Immediate return if records available",
            "Parked via async notify if waiting for HW"
        ]),
        ("Step 2: HW Validation", "High Watermark Guard", "#fb7185", [
            "Verifies StartOffset < High Watermark",
            "Prevents reading uncommitted / un-replicated records",
            "Returns empty response if offset at LEO",
            "Prevents dirty reads across cluster"
        ]),
        ("Step 3: Index Lookup", "Sparse Index Binary Search", "#fbbf24", [
            "Binary search on sparse .idx file",
            "Locates exact physical byte offset & length",
            "Resolves active segment or sealed historical file",
            "O(log N) lookup time in under 200ns"
        ]),
        ("Step 4: Header Framing", "Protocol Header Out", "#38bdf8", [
            "Writes Kafka or Native protocol response header",
            "Magic header + status + byte length",
            "Flushed immediately to TCP send buffer",
            "Sets socket to non-blocking DMA mode"
        ]),
        ("Step 5: sendfile(2)", "Kernel DMA Direct Transfer", "#34d399", [
            "libc::sendfile(socket_fd, file_fd, offset, bytes)",
            "Linux Page Cache -> Socket buffer -> NIC DMA",
            "ZERO copies to user space memory!",
            "Zero CPU cycles spent moving data bytes"
        ]),
        ("Step 6: Delivery", "Client Stream Ingestion", "#a855f7", [
            "Consumer receives raw bytes directly from NIC",
            "Multi-gigabit/s line rate saturation",
            "Zero CPU cache pollution on broker",
            "Sub-100µs end-to-end read latency"
        ])
    ]

    for i, (stitle, ssub, scolor, sitems) in enumerate(fetch_steps):
        cx = px + i * (step_w + step_gap)
        diag.add_card(cx, ry + 130, step_w, 480, stitle, items=sitems, subtitle=ssub, accent_color=scolor, badge=f"0{i+1}", badge_color="#0f172a")
        if i < len(fetch_steps) - 1:
            diag.add_arrow(cx + step_w, ry + 370, cx + step_w + step_gap, ry + 370, color="#fb7185", stroke_width=3, marker="arrow-rose")

    diag.add_card(100, ry + 640, 1440, 250, "Traditional Broker (4 Memory Copies & 4 Context Switches)",
                  items=[
                      "1. Kernel reads NVMe disk blocks into OS Page Cache",
                      "2. read() syscall copies bytes from Page Cache into User-Space JVM/Go buffer (Context Switch 1 & 2)",
                      "3. write() syscall copies bytes from User-Space into Socket Buffer (Context Switch 3 & 4)",
                      "4. NIC controller copies bytes from Socket Buffer via DMA to network cable",
                      "Result: Saturated memory bus, heavy CPU cache pollution, high latency spikes"
                  ],
                  accent_color="#fb7185", badge="Traditional", badge_color="#9f1239")

    diag.add_card(1580, ry + 640, 1480, 250, "AeroStream Linux sendfile(2) Zero-Copy Architecture",
                  items=[
                      "1. libc::sendfile(socket_fd, file_fd, offset, bytes) issued directly from Rust storage kernel",
                      "2. Linux kernel performs Direct Memory Access (DMA): Page Cache -> NIC Network Buffer directly",
                      "3. ZERO memory copies into user space memory, ZERO CPU cache line pollution",
                      "4. CPU utilization stays flat under multi-gigabyte continuous read traffic",
                      "Result: Maximum network bandwidth saturation with microsecond tail latency"
                  ],
                  accent_color="#34d399", badge="AeroStream Zero-Copy", badge_color="#059669")

    return diag.export_svg(), diag.export_excalidraw()


def build_controller_broker_orchestration() -> Tuple[str, dict]:
    title = "AeroStream Controller-Broker Control Plane Orchestration"
    subtitle = "Bidirectional gRPC Streaming (Port 8001), 2s Heartbeat Telemetry, Dynamic Configs, and Automated Failover"
    badges = [
        ("gRPC Stream (Port 8001)", "#0284c7", "#ffffff", "#38bdf8"),
        ("2s Heartbeat Ticker", "#059669", "#ffffff", "#34d399"),
        ("8s Lease Expiry", "#b45309", "#ffffff", "#fbbf24"),
        ("Dynamic Quotas & Codecs", "#7c3aed", "#ffffff", "#a78bfa"),
        ("Automated ISR Updates", "#e11d48", "#ffffff", "#fb7185")
    ]
    diag = DiagramArchitect(title, subtitle, badges)

    gx, gy, gw, gh = 60, 220, 1380, 1400
    diag.add_container(gx, gy, gw, gh, "GO CONTROLLER (RAFT LEADER)",
                       "Cluster Authority: Consensus Leader, Topology Manager, Lease Sweeper, Quota Enforcer",
                       accent_color="#38bdf8", bg_fill="url(#grad-cyan-card)", border_color="#0284c7", badge="Port 8001 Server")

    diag.add_card(gx + 40, gy + 130, 620, 360, "Raft Consensus FSM State",
                  items=[
                      "Committed ClusterState in BoltDB",
                      "Active Brokers registry (id, host, ports, rack)",
                      "Topic partition layouts & leader epochs",
                      "In-Sync Replicas (ISR) set membership",
                      "Quorum consensus on all topology mutations"
                  ],
                  subtitle="Port 7001 • Raft Log FSM",
                  accent_color="#38bdf8", badge="FSM State", badge_color="#0369a1")

    diag.add_card(gx + 700, gy + 130, 640, 360, "Failure Detector & Lease Sweeper",
                  items=[
                      "Sweep Ticker fires every 3 seconds",
                      "Inspects last_heartbeat timestamp per broker",
                      "Lease Expiry: time.Since(last_hb) > 8s",
                      "Automatically triggers CmdCleanInactive",
                      "Elects new leader from surviving ISR set"
                  ],
                  subtitle="8-Second Lease Expiry Watchdog",
                  accent_color="#fbbf24", badge="Lease Sweeper", badge_color="#b45309")

    diag.add_card(gx + 40, gy + 530, 1300, 380, "gRPC ControlService Handler (Server)",
                  items=[
                      "1. RegisterBroker(RegisterBrokerRequest) -> RegisterBrokerResponse",
                      "   • Validates broker_id, host, data_port (9091), kafka_port (9092), rack ID",
                      "   • Proposes CmdRegisterBroker to Raft log; responds with cluster ID and assigned partitions",
                      "2. Heartbeat(HeartbeatRequest) -> HeartbeatResponse",
                      "   • Receives disk_usage_bytes and per-partition Log End Offsets (LEO)",
                      "   • Computes High Watermark = min(LEO of all in-sync replicas)",
                      "   • Piggybacks updated partition leader assignments (assigned_leaders, assigned_followers)",
                      "   • Piggybacks dynamic client quotas (byte rate limits) and topic compression rules (zstd, lz4)"
                  ],
                  subtitle="proto/control.proto • Multiplexed HTTP/2 Service",
                  accent_color="#38bdf8", badge="gRPC Server", badge_color="#0284c7")

    diag.add_card(gx + 40, gy + 950, 1300, 420, "Dynamic Cluster Management Policies",
                  items=[
                      "Client Quotas Subsystem: per-user and per-client-id produce/fetch rate throttles",
                      "Topic Compression Codec Overrides: dynamically enforces zstd, lz4, or snappy without broker restarts",
                      "Automated High Watermark Advancement: notifies brokers when replicas catch up",
                      "Graceful Drain Orchestrator: intercepts scale-down requests and gracefully migrates partitions"
                  ],
                  subtitle="Live Push to All Connected Storage Brokers",
                  accent_color="#818cf8", badge="Dynamic Config", badge_color="#4338ca")

    rx, ry, rw, rh = 1760, 220, 1380, 1400
    diag.add_container(rx, ry, rw, rh, "RUST STORAGE BROKER (DATA PLANE)",
                       "High-Speed Storage Node: Background Orchestrator, Heartbeat Ticker, Partition Log Engine",
                       accent_color="#34d399", bg_fill="url(#grad-emerald-card)", border_color="#059669", badge="Port 8001 Client")

    diag.add_card(rx + 40, ry + 130, 620, 360, "Broker Startup Lifecycle",
                  items=[
                      "Spawns background orchestration task",
                      "Connects persistent gRPC channel to Controller",
                      "Sends RegisterBrokerRequest with capabilities",
                      "Mounts assigned topic partition directories",
                      "Initializes Shard-per-Core engine threads"
                  ],
                  subtitle="Startup & Cluster Join Flow",
                  accent_color="#34d399", badge="Join Flow", badge_color="#047857")

    diag.add_card(rx + 700, ry + 130, 640, 360, "2s Heartbeat Ticker Loop",
                  items=[
                      "tokio::time::interval(Duration::from_secs(2))",
                      "Collects current disk usage across volumes",
                      "Gathers Log End Offset (LEO) for all partitions",
                      "Calculates consumer replica lag metrics",
                      "Transmits HeartbeatRequest over gRPC stream"
                  ],
                  subtitle="Continuous Telemetry Ticker",
                  accent_color="#34d399", badge="Heartbeat Loop", badge_color="#047857")

    diag.add_card(rx + 40, ry + 530, 1300, 380, "Piggybacked Response Handler",
                  items=[
                      "Applies leadership changes: transitions partitions between Leader and Follower roles",
                      "Updates Client Quotas table in shared memory Swiss Tables",
                      "Updates Topic Compression codecs (Zstd, Lz4, Snappy)",
                      "Follower partitions: starts Native Cmd 3 ReplicaFetch stream from assigned leader broker",
                      "Leader partitions: updates High Watermark based on controller min-LEO response"
                  ],
                  subtitle="Zero-Restart In-Flight Configuration Updates",
                  accent_color="#34d399", badge="State Sync", badge_color="#047857")

    diag.add_card(rx + 40, ry + 950, 1300, 420, "Replica Fetching & Cluster Replication (Cmd 3)",
                  items=[
                      "Follower brokers issue Native Protocol Cmd 3 ReplicaFetch to leader broker on Port 9091",
                      "Leader streams raw log batches directly via Linux sendfile(2) zero-copy DMA",
                      "Follower appends to local PartitionLog and updates local LEO",
                      "Follower reports new LEO on next 2s heartbeat; Controller advances cluster High Watermark"
                  ],
                  subtitle="Native Protocol Cmd 3 High-Performance Replication",
                  accent_color="#fbbf24", badge="Replication", badge_color="#b45309")

    # Middle gRPC Arrows
    diag.add_arrow(1440, 560, 1760, 560, color="#38bdf8", stroke_width=4, marker="arrow-cyan", label="1. RegisterBroker(id, host, ports, rack)")
    diag.add_arrow(1760, 640, 1440, 640, color="#34d399", stroke_width=4, marker="arrow-emerald", label="RegisterBrokerResponse(cluster_id, partitions)")

    diag.add_arrow(1760, 800, 1440, 800, color="#34d399", stroke_width=4, marker="arrow-emerald", label="2. HeartbeatRequest(disk_usage, per-partition LEOs)")
    diag.add_arrow(1440, 880, 1760, 880, color="#38bdf8", stroke_width=4, marker="arrow-cyan", label="3. HeartbeatResponse(assigned_leaders, quotas, codecs)")

    # Bottom Timeline
    ty = 1660
    diag.add_container(60, ty, 3080, 450, "AUTOMATED FAILURE DETECTION & LEADER RE-ELECTION LIFECYCLE",
                       "Sequence of events when a broker experiences a hardware failure or network partition",
                       accent_color="#fb7185", border_color="#e11d48", badge="Failover Timeline")

    timeline_steps = [
        ("T = 0.0s", "Broker Hardware Stall", "#fb7185", [
            "Broker 1 network disconnects or process crashes.",
            "In-flight requests fail or pause.",
            "Last successful heartbeat recorded."
        ]),
        ("T = 2.0s", "Heartbeat Missed", "#fbbf24", [
            "Controller expects periodic 2s heartbeat.",
            "No packet received from Broker 1.",
            "Controller increments missed beat counter."
        ]),
        ("T = 8.0s", "8s Lease Expiry", "#fbbf24", [
            "Failure detection sweeper ticker fires.",
            "time.Since(last_hb) exceeds 8.0s timeout.",
            "Broker 1 marked as DEAD in active lease table."
        ]),
        ("T = 8.1s", "Raft Proposal", "#38bdf8", [
            "Controller proposes CmdCleanInactive to Raft.",
            "Follower controllers commit log entry.",
            "Quorum majority (2/3) reached in <10ms."
        ]),
        ("T = 8.2s", "Leader Re-Election", "#34d399", [
            "FSM evicts Broker 1 from active topology.",
            "Surviving in-sync replica (Broker 2) elected Leader.",
            "New leader epoch incremented in BoltDB."
        ]),
        ("T = 8.4s", "Topology Push", "#a855f7", [
            "Surviving brokers receive assignment in HeartbeatResp.",
            "Kafka clients receive updated MetadataResponse.",
            "Producers resume writes to Broker 2 seamlessly."
        ])
    ]

    t_w = 470
    t_gap = 36
    for i, (time_lbl, step_name, st_col, st_items) in enumerate(timeline_steps):
        cx = 100 + i * (t_w + t_gap)
        diag.add_card(cx, ty + 106, t_w, 310, time_lbl, items=st_items, subtitle=step_name, accent_color=st_col, badge=f"Phase {i+1}", badge_color="#0f172a")
        if i < len(timeline_steps) - 1:
            diag.add_arrow(cx + t_w, ty + 250, cx + t_w + t_gap, ty + 250, color="#fb7185", stroke_width=2.5, marker="arrow-rose")

    return diag.export_svg(), diag.export_excalidraw()


def build_cluster_topology_scale_down() -> Tuple[str, dict]:
    title = "AeroStream Cluster Topology & Zero-Downtime Scale-Down"
    subtitle = "3-Node Raft Consensus Quorum, Multi-Broker Storage Mesh, and Automated Partition Draining"
    badges = [
        ("3-Node Raft Quorum", "#0284c7", "#ffffff", "#38bdf8"),
        ("3+ Storage Brokers", "#059669", "#ffffff", "#34d399"),
        ("POST /api/brokers/{id}/drain", "#e11d48", "#ffffff", "#fb7185"),
        ("Automated ISR Handoff", "#7c3aed", "#ffffff", "#a78bfa"),
        ("Zero Message Loss", "#047857", "#ffffff", "#10b981")
    ]
    diag = DiagramArchitect(title, subtitle, badges)

    qy = 220
    diag.add_container(60, qy, 3080, 480, "1. CONTROL PLANE: 3-NODE RAFT CONSENSUS QUORUM (PORT 7001)",
                       "Maintains linearizable cluster metadata, topic partition layouts, and broker lease state with 2/3 majority consensus",
                       accent_color="#38bdf8", border_color="#0284c7", badge="Raft Quorum")

    c_w = 940
    c_gap = 80
    controllers = [
        ("Controller 1 (Raft Leader)", "Elected Leader • Terms & Proclamations", "#38bdf8", "#0369a1", [
            "Port 7001 (Raft) • Port 8001 (gRPC) • Port 9001 (REST/UI)",
            "Coordinates Raft consensus writes to BoltDB FSM",
            "Manages active broker lease table & health sweeper",
            "Processes scale-down drain requests: POST /api/brokers/{id}/drain"
        ]),
        ("Controller 2 (Raft Follower)", "Quorum Follower • Hot Standby", "#818cf8", "#4338ca", [
            "Port 7001 (Raft) • Port 8001 (gRPC) • Port 9001 (REST/UI)",
            "Maintains replicated BoltDB metadata log snapshot",
            "Participates in Raft log consensus votes (Majority 2/3)",
            "Instantaneous leader election candidate upon Leader failure"
        ]),
        ("Controller 3 (Raft Follower)", "Quorum Follower • Hot Standby", "#818cf8", "#4338ca", [
            "Port 7001 (Raft) • Port 8001 (gRPC) • Port 9001 (REST/UI)",
            "Maintains replicated BoltDB metadata log snapshot",
            "Participates in Raft log consensus votes (Majority 2/3)",
            "Prevents split-brain scenarios across availability zones"
        ])
    ]

    for i, (ctitle, csub, ccol, cbadge_col, citems) in enumerate(controllers):
        cx = 100 + i * (c_w + c_gap)
        diag.add_card(cx, qy + 120, c_w, 320, ctitle, items=citems, subtitle=csub, accent_color=ccol, badge="Controller", badge_color=cbadge_col)
        if i < 2:
            diag.add_arrow(cx + c_w, qy + 260, cx + c_w + c_gap, qy + 260, color="#818cf8", stroke_width=3, dashed=True, marker="arrow-purple", label="Raft AppendEntries (7001)")

    by = 740
    diag.add_container(60, by, 3080, 580, "2. DATA PLANE: MULTI-BROKER STORAGE MESH WITH PARTITION REPLICATION",
                       "Topic partitions replicated across racks. Shows Partition 0, 1, and 2 distributed with active ISR sets",
                       accent_color="#34d399", border_color="#059669", badge="Storage Mesh")

    brokers = [
        ("Broker 1 (Rack us-east-1a)", "Active Node • Healthy", "#34d399", "#047857", [
            "Port 9091 (Native) • Port 9092 (Kafka)",
            "LEADER: Topic 'orders' Partition 0 (LEO: 10450)",
            "FOLLOWER: Topic 'orders' Partition 1 (LEO: 8900)",
            "FOLLOWER: Topic 'orders' Partition 2 (LEO: 14200)",
            "Status: HEALTHY • Disk Usage: 42% NVMe"
        ]),
        ("Broker 2 (Rack us-east-1b)", "Active Node • Healthy", "#34d399", "#047857", [
            "Port 9091 (Native) • Port 9092 (Kafka)",
            "LEADER: Topic 'orders' Partition 1 (LEO: 8900)",
            "FOLLOWER: Topic 'orders' Partition 0 (LEO: 10450)",
            "FOLLOWER: Topic 'orders' Partition 2 (LEO: 14200)",
            "Status: HEALTHY • Disk Usage: 39% NVMe"
        ]),
        ("Broker 3 (Rack us-east-1c)", "TARGET SCALE-DOWN BROKER (DRAINING)", "#fb7185", "#9f1239", [
            "Port 9091 (Native) • Port 9092 (Kafka)",
            "ORIGINAL LEADER: Topic 'orders' Partition 2 (LEO: 14200)",
            "TARGET FOR DECOMMISSIONING / DRAIN",
            "All partitions migrating to Broker 1 and Broker 2",
            "Status: DRAINING • POST /api/brokers/3/drain"
        ])
    ]

    for i, (btitle, bsub, bcol, bbadge_col, bitems) in enumerate(brokers):
        bx = 100 + i * (c_w + c_gap)
        diag.add_card(bx, by + 120, c_w, 420, btitle, items=bitems, subtitle=bsub, accent_color=bcol, badge=f"Broker {i+1}", badge_color=bbadge_col)

    diag.add_arrow(1040, by + 300, 1120, by + 300, color="#34d399", stroke_width=3, marker="arrow-emerald", label="Cmd 3 ReplicaFetch")
    diag.add_arrow(2080, by + 300, 2160, by + 300, color="#fb7185", stroke_width=3, marker="arrow-rose", label="Drain Replication Handoff")

    wy = 1360
    diag.add_container(60, wy, 3080, 750, "3. STEP-BY-STEP ZERO-DOWNTIME SCALE-DOWN & DRAIN WORKFLOW",
                       "How AeroStream cleanly drains a node without dropping messages, corrupting logs, or interrupting clients",
                       accent_color="#fbbf24", border_color="#fbbf24", badge="Drain Workflow")

    drain_steps = [
        ("Step 1: Drain Trigger", "POST /api/brokers/3/drain", "#fbbf24", [
            "Operator or Kubernetes auto-scaler calls /drain endpoint.",
            "Controller verifies cluster quorum & healthy replicas.",
            "Broker 3 status updated to DRAINING in Raft metadata."
        ]),
        ("Step 2: Reject New Traffic", "Ingress Fencing", "#fbbf24", [
            "Controller fences Broker 3 from new partition allocations.",
            "Kafka metadata marks Broker 3 as leaving.",
            "Client SDKs redirect new connection handshakes."
        ]),
        ("Step 3: Leadership Move", "Reassign Leaders", "#38bdf8", [
            "Controller elects Broker 1 as new Leader for Partition 2.",
            "Broker 3 steps down to Follower role gracefully.",
            "Inflight produces finish with zero lost offsets."
        ]),
        ("Step 4: Catch-Up Sync", "Full Replica Catch-up", "#34d399", [
            "Broker 1 & 2 pull remaining un-replicated records.",
            "Replica lag reaches exactly 0 (LEO_follower == LEO_leader).",
            "High Watermark confirmed synchronized."
        ]),
        ("Step 5: ISR Set Update", "Raft Metadata Commit", "#a855f7", [
            "Controller proposes CmdUpdateISR removing Broker 3.",
            "Raft commits new topology: ISR = [Broker 1, Broker 2].",
            "Updated topology broadcast via gRPC stream."
        ]),
        ("Step 6: Safe Shutdown", "Clean Process Exit", "#34d399", [
            "Broker 3 flushes all active segment mmap buffers.",
            "sync_file_range flushes dirty pages to NVMe.",
            "Broker 3 process exits cleanly with exit code 0."
        ])
    ]

    dw = 470
    dgap = 36
    for i, (dtitle, dsub, dcol, ditems) in enumerate(drain_steps):
        dx = 100 + i * (dw + dgap)
        diag.add_card(dx, wy + 120, dw, 580, dtitle, items=ditems, subtitle=dsub, accent_color=dcol, badge=f"Phase {i+1}", badge_color="#0f172a")
        if i < len(drain_steps) - 1:
            diag.add_arrow(dx + dw, wy + 380, dx + dw + dgap, wy + 380, color="#fbbf24", stroke_width=3, marker="arrow-amber")

    return diag.export_svg(), diag.export_excalidraw()


def build_tiered_storage_pipeline() -> Tuple[str, dict]:
    title = "AeroStream Multi-Cloud Tiered Storage Pipeline"
    subtitle = "Active NVMe Segment Rollover, Async Compression, Multi-Cloud Offload (S3/GCS/Azure), and Transparent Cold Read"
    badges = [
        ("Tiered Storage Kernel", "#059669", "#ffffff", "#34d399"),
        ("Local NVMe Tier (Hot)", "#0284c7", "#ffffff", "#38bdf8"),
        ("Cloud Object Tier (Cold)", "#b45309", "#ffffff", "#fbbf24"),
        ("Transparent Cold Read", "#7c3aed", "#ffffff", "#a78bfa"),
        ("Multi-Cloud S3 / GCS / Azure", "#047857", "#ffffff", "#10b981")
    ]
    diag = DiagramArchitect(title, subtitle, badges)

    hx, hy, hw, hh = 60, 220, 940, 1380
    diag.add_container(hx, hy, hw, hh, "TIER 1: LOCAL NVMe STORAGE (HOT DATA)",
                       "Sub-millisecond latency append log & active segment writes",
                       accent_color="#38bdf8", bg_fill="url(#grad-cyan-card)", border_color="#0284c7", badge="Local NVMe")

    diag.add_card(hx + 36, hy + 130, hw - 72, 360, "Active Head Segment (.log & .idx)",
                  items=[
                      "mmap memory-mapped append buffer: 00000000000000000000.log",
                      "Sub-microsecond write latency with single-syscall write_all_at",
                      "In-place base offset patching into record batch headers",
                      "Paced dirty page writeback via sync_file_range(2)",
                      "Sparse offset index (.idx) & timestamp index (.timeindex)",
                      "All recent producer writes & consumer fetches served here"
                  ],
                  subtitle="Active Write Buffer • High Performance",
                  accent_color="#38bdf8", badge="Active Head", badge_color="#0369a1")

    diag.add_card(hx + 36, hy + 530, hw - 72, 340, "Rollover Trigger & Segment Sealing",
                  items=[
                      "Condition 1: Segment size reaches max_segment_size (1 GB)",
                      "Condition 2: Segment age reaches segment_ms (e.g. 7 days)",
                      "roll_over() initiates sealing of active segment:",
                      "• Flushes pending mmap writes and closes active file handles",
                      "• Creates new active segment for next base offset",
                      "• Fast hard-link or copy to data/cold_storage/ staging"
                  ],
                  subtitle="Segment Rollover Thresholds",
                  accent_color="#fbbf24", badge="Rollover", badge_color="#b45309")

    diag.add_card(hx + 36, hy + 910, hw - 72, 430, "Closed Local Segment Staging",
                  items=[
                      "Immutable local segment files ready for archival",
                      "Named: {base_offset:020}.log & {base_offset:020}.idx",
                      "Fast hard-linking allows instantaneous unblocking of write thread",
                      "Log retention policy tracks local NVMe disk usage threshold",
                      "Local segments pruned after successful cloud upload confirmation"
                  ],
                  subtitle="Cold Storage Staging Directory",
                  accent_color="#38bdf8", badge="Staged", badge_color="#0284c7")

    ox, oy, ow, oh = 1040, 220, 1080, 1380
    diag.add_container(ox, oy, ow, oh, "ASYNCHRONOUS OFFLOAD ENGINE",
                       "Lock-free task pipeline, optional Zstandard compression, and cloud upload workers",
                       accent_color="#34d399", border_color="#34d399", badge="Async Engine")

    diag.add_card(ox + 36, oy + 130, ow - 72, 360, "Lock-Free Offload Task Queue (mpsc)",
                  items=[
                      "Non-blocking try_send(OffloadTask) from log rollover thread",
                      "Carries metadata: topic, partition, base_offset, file paths",
                      "Zero latency impact on main Shard-per-Core storage engine",
                      "Bounded task capacity with backpressure monitoring",
                      "Worker thread pool polls tasks concurrently"
                  ],
                  subtitle="OffloadTask MPSC Pipeline",
                  accent_color="#34d399", badge="Task Queue", badge_color="#047857")

    diag.add_card(ox + 36, oy + 530, ow - 72, 340, "Cold Segment Compression (Zstd / LZ4)",
                  items=[
                      "Optional background compression of sealed segment chunks",
                      "Zstandard level 19 or LZ4 high-throughput codecs",
                      "Reduces cloud storage volume by 60% – 80%",
                      "Compresses both commit log (.log) and sparse index (.idx)",
                      "Computes SHA-256 integrity checksum before upload"
                  ],
                  subtitle="Storage Cost Optimization Pipeline",
                  accent_color="#a855f7", badge="Zstd / LZ4", badge_color="#6b21a8")

    diag.add_card(ox + 36, oy + 910, ow - 72, 430, "TieredStorageOffloader Worker Pool",
                  items=[
                      "Multi-threaded Tokio async workers executing cloud I/O",
                      "Standardized Object Key Format:",
                      "  tiered/{topic}/partition_{id}/{base_offset:020}.log",
                      "  tiered/{topic}/partition_{id}/{base_offset:020}.idx",
                      "Multipart chunked upload with exponential backoff & retries",
                      "Notifies Go Controller of archived segment offset range"
                  ],
                  subtitle="Unified TieredStorageProvider Interface",
                  accent_color="#34d399", badge="Upload Worker", badge_color="#047857")

    cx, cy, cw, ch = 2160, 220, 980, 1380
    diag.add_container(cx, cy, cw, ch, "TIER 2: MULTI-CLOUD OBJECT STORAGE (COLD)",
                       "Infinite retention at 90% reduced infrastructure storage cost",
                       accent_color="#fbbf24", border_color="#fbbf24", badge="Cloud Tier")

    clouds = [
        ("AWS S3 & MinIO Provider", "S3StorageProvider • Custom endpoints & path-style addressing", "#fbbf24", [
            "Official AWS SDK with IAM Role / Web Identity federation",
            "Configurable S3 buckets, storage classes (STANDARD, GLACIER)",
            "MinIO local / on-premise object storage support"
        ]),
        ("Google Cloud Storage (GCS)", "GcsStorageProvider • Native JSON API & chunked uploads", "#38bdf8", [
            "Native Google Cloud service account authentication",
            "Chunked resumable upload sessions for multi-gigabyte logs",
            "Multi-region bucket redundancy across availability zones"
        ]),
        ("Azure Blob Storage", "AzureBlobStorageProvider • SharedKey & Bearer auth", "#818cf8", [
            "Azure Blob storage accounts & container management",
            "BlockBlob upload with parallel block staging",
            "Azure Managed Identity & Service Principal auth"
        ]),
        ("Local / NFS Storage", "LocalStorageProvider • Network filesystem mounts", "#34d399", [
            "High-speed NFS, CephFS, or Lustre network storage mounts",
            "Zero cloud egress costs for enterprise on-premise clusters",
            "Atomic file rename and directory sync guarantees"
        ])
    ]

    for i, (ctitle, csub, ccol, citems) in enumerate(clouds):
        sy = cy + 130 + i * 295
        diag.add_card(cx + 36, sy, cw - 72, 260, ctitle, items=citems, subtitle=csub, accent_color=ccol, badge="Provider", badge_color="#0f172a")

    diag.add_arrow(1000, 400, 1040, 400, color="#38bdf8", stroke_width=4, marker="arrow-cyan", label="roll_over()")
    diag.add_arrow(2120, 1140, 2160, 1140, color="#34d399", stroke_width=4, marker="arrow-emerald", label="put_segment()")

    ty = 1640
    diag.add_container(60, ty, 3080, 470, "TRANSPARENT COLD SEGMENT RETRIEVAL & LRU DISK CACHE EVICTION",
                       "How consumers seamlessly read historical data that has been pruned from local NVMe disks",
                       accent_color="#a855f7", border_color="#a855f7", badge="Cold Retrieval")

    retrieval_steps = [
        ("1. Historical Fetch", "Consumer Request", "#a855f7", [
            "Consumer requests offset X.",
            "Offset X is older than earliest local NVMe segment.",
            "Storage kernel detects local cache miss."
        ]),
        ("2. Index Search", "find_cold_segment", "#38bdf8", [
            "Binary search on cloud segment metadata index.",
            "Identifies exact object key holding offset X.",
            "Resolves byte range within remote .log file."
        ]),
        ("3. Cloud Fetch", "get_segment Range Read", "#fbbf24", [
            "Issues HTTP Range GET to S3/GCS/Azure provider.",
            "Downloads requested segment chunk or full index.",
            "Decompresses Zstandard chunk in RAM buffer."
        ]),
        ("4. Stream to Client", "sendfile / Socket Write", "#34d399", [
            "Streams decompressed records directly to consumer.",
            "Client SDK perceives zero difference in stream format.",
            "No manual operator intervention or restore jobs."
        ]),
        ("5. Local LRU Cache", "Cache Management", "#38bdf8", [
            "Recently fetched cold chunks cached in staging NVMe.",
            "Background LRU cleaner evicts least recently accessed data.",
            "Maintains configured local disk capacity ceiling."
        ])
    ]

    rw = 570
    rgap = 40
    for i, (rtitle, rsub, rcol, ritems) in enumerate(retrieval_steps):
        rx = 100 + i * (rw + rgap)
        diag.add_card(rx, ty + 120, rw, 320, rtitle, items=ritems, subtitle=rsub, accent_color=rcol, badge=f"Step {i+1}", badge_color="#0f172a")
        if i < len(retrieval_steps) - 1:
            diag.add_arrow(rx + rw, ty + 270, rx + rw + rgap, ty + 270, color="#a855f7", stroke_width=3, marker="arrow-purple")

    return diag.export_svg(), diag.export_excalidraw()


def build_native_and_kafka_dual_protocol() -> Tuple[str, dict]:
    title = "AeroStream Dual-Protocol Engine: Native vs. Kafka Wire Protocol"
    subtitle = "Side-by-Side Binary Framing Comparison & Unified Multi-Language SDK Ecosystem (Go, Rust, Java, .NET, Node.js)"
    badges = [
        ("Dual Protocol Engine", "#0284c7", "#ffffff", "#38bdf8"),
        ("Port 9091 Native", "#059669", "#ffffff", "#34d399"),
        ("Port 9092 Kafka Drop-In", "#b45309", "#ffffff", "#fbbf24"),
        ("7-Byte Native Framing", "#047857", "#ffffff", "#10b981"),
        ("5 Official SDKs", "#7c3aed", "#ffffff", "#a78bfa")
    ]
    diag = DiagramArchitect(title, subtitle, badges)

    nx, ny, nw, nh = 60, 220, 1500, 1380
    diag.add_container(nx, ny, nw, nh, "AEROSTREAM NATIVE PROTOCOL (PORT 9091)",
                       "Ultra-compact 7-byte binary header, zero serialization bloat, sub-millisecond p99.9 latency",
                       accent_color="#34d399", bg_fill="url(#grad-emerald-card)", border_color="#059669", badge="Port 9091 Native")

    diag.add_card(nx + 40, ny + 130, nw - 80, 320, "7-Byte Request Frame Header Specification",
                  items=[
                      "Bytes 0 – 1: Protocol Magic Prefix: [0xAE, 0x01] (Identifies AeroStream protocol)",
                      "Byte 2: Command Identifier: [cmd: u8]",
                      "  • 1 = ProduceRequest  |  2 = FetchRequest  |  3 = ReplicaFetchRequest  |  4 = MultiFetchRequest",
                      "Bytes 3 – 6: Body Payload Length: [body_len: u32 BE] (Big-Endian unsigned 32-bit integer)",
                      "Total Request Header Size: Exactly 7 Bytes (Fixed size, single read_exact syscall)"
                  ],
                  subtitle="Header Framing Layout",
                  accent_color="#34d399", badge="7-Byte Header", badge_color="#047857",
                  code_block="+-------------------+-------------+------------------------+\n| Magic: 0xAE 0x01  | Cmd: u8 (1) | Body Length: u32 BE (4)|\n|   (2 Bytes)       |  (1 Byte)   |       (4 Bytes)        |\n+-------------------+-------------+------------------------+")

    diag.add_card(nx + 40, ny + 480, nw - 80, 440, "Native Request Body Binary Layouts",
                  items=[
                      "Produce Body (cmd=1):",
                      "  [topic_len: u16 BE][topic: utf-8][partition: u32 BE][payload: raw bytes]",
                      "Fetch Body (cmd=2):",
                      "  [topic_len: u16 BE][topic: utf-8][partition: u32 BE][start_offset: u64 BE][max_bytes: u32 BE]",
                      "ReplicaFetch Body (cmd=3):",
                      "  [replica_id: i32 BE][topic_len: u16 BE][topic: utf-8][partition: u32 BE][start_offset: u64 BE][max_bytes: u32 BE]",
                      "MultiFetch Body (cmd=4):",
                      "  Same as Fetch Body + [max_wait_ms: u32 BE] for long-polling support"
                  ],
                  subtitle="Deterministic Binary Body Layouts",
                  accent_color="#34d399", badge="Body Layout", badge_color="#047857",
                  code_block="Produce: [topic_len(2)][topic(N)][partition(4)][payload(M)]\nFetch:   [topic_len(2)][topic(N)][partition(4)][start_offset(8)][max_bytes(4)]\nReplica: [replica_id(4)][topic_len(2)][topic(N)][partition(4)][start_offset(8)][max_bytes(4)]")

    diag.add_card(nx + 40, ny + 950, nw - 80, 400, "Native Response Frames Specification",
                  items=[
                      "Produce Ack Response: [0xAE, 0x01][status=0][offset: u64 BE] (11 Bytes total)",
                      "Fetch Data Response Header: [0xAE, 0x01][status=2][bytes_to_read: u32 BE] (7 Bytes) + DMA data",
                      "Fetch Empty Response: [0xAE, 0x01][status=1] (3 Bytes total)",
                      "OutOfOrderSequence Error: [0xAE, 0x01][status=45] (3 Bytes total)",
                      "Key Benefit: Zero deserialization overhead, zero memory allocation during header parsing"
                  ],
                  subtitle="Ultra-Fast Response Framing",
                  accent_color="#34d399", badge="Responses", badge_color="#047857",
                  code_block="Produce Ack: [0xAE 0x01][0x00][assigned_offset: 8 bytes BE]\nFetch Data:  [0xAE 0x01][0x02][bytes_to_follow: 4 bytes BE] + [sendfile DMA bytes]\nEmpty Fetch: [0xAE 0x01][0x01]")

    kx, ky, kw, kh = 1640, 220, 1500, 1380
    diag.add_container(kx, ky, kw, kh, "APACHE KAFKA WIRE PROTOCOL (PORT 9092)",
                       "Drop-in wire compatibility with existing Kafka ecosystems, enterprise drivers, and streaming tools",
                       accent_color="#fbbf24", border_color="#fbbf24", badge="Port 9092 Kafka")

    diag.add_card(kx + 40, ky + 130, kw - 80, 320, "Kafka RequestHeader v2 Specification",
                  items=[
                      "message_size: i32 BE (Total remaining length of the request frame in bytes)",
                      "api_key: i16 BE (Identifies the Kafka API, e.g., 0=Produce, 1=Fetch, 3=Metadata)",
                      "api_version: i16 BE (Supported API version for protocol schema negotiation)",
                      "correlation_id: i32 BE (Unique request/response tracking identifier)",
                      "client_id: nullable string (i16 length prefix followed by UTF-8 bytes)",
                      "tagged_fields: varint count + tags (KIP-482 flexible fields support)"
                  ],
                  subtitle="Standard Kafka Framing Layout",
                  accent_color="#fbbf24", badge="Header v2", badge_color="#b45309",
                  code_block="+------------------+--------------+------------------+--------------------+----------------+\n| message_size: i32| api_key: i16 | api_version: i16 | correlation_id: i32| client_id: str |\n|     (4 Bytes)    |   (2 Bytes)  |     (2 Bytes)    |      (4 Bytes)     |   (2+N Bytes)  |\n+------------------+--------------+------------------+--------------------+----------------+")

    diag.add_card(kx + 40, ky + 480, kw - 80, 440, "Supported Kafka API Keys (0 through 36)",
                  items=[
                      "ApiKey 0 (Produce v0-v7): RecordBatch v2 ingestion, compression (zstd, lz4, snappy, gzip)",
                      "ApiKey 1 (Fetch v0-v11): High Watermark tracking, sendfile DMA streaming, KIP-392 rack awareness",
                      "ApiKey 2 (ListOffsets v0-v5): Earliest (-2), Latest (-1), timestamp-based lookups",
                      "ApiKey 3 (Metadata v0-v5): Cluster topology, broker list, partition leaders, racks",
                      "ApiKey 8/9 (OffsetCommit / OffsetFetch): Consumer group offset management",
                      "ApiKey 10/11/12/13/14 (FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup)",
                      "ApiKey 18 (ApiVersions v0-v3): Protocol negotiation and version ranges",
                      "ApiKey 22-25 (Transactions): InitProducerId, AddPartitionsToTxn, EndTxn (2PC)"
                  ],
                  subtitle="Full Enterprise Kafka Wire Implementation",
                  accent_color="#fbbf24", badge="ApiKeys 0-36", badge_color="#b45309",
                  code_block="Produce:    ApiKey 0  | Fetch:        ApiKey 1  | ListOffsets: ApiKey 2\nMetadata:   ApiKey 3  | OffsetCommit: ApiKey 8  | OffsetFetch: ApiKey 9\nFindCoord:  ApiKey 10 | JoinGroup:    ApiKey 11 | Heartbeat:   ApiKey 12\nSyncGroup:  ApiKey 14 | ApiVersions:  ApiKey 18 | InitProdId:  ApiKey 22")

    diag.add_card(kx + 40, ky + 950, kw - 80, 400, "Drop-In Client Compatibility",
                  items=[
                      "Zero Client Modification: existing Kafka applications work simply by updating bootstrap servers",
                      "Standard Drivers Supported: librdkafka, kafka-python, Java KafkaClient, confluent-kafka-go",
                      "Ecosystem Integrations: Kafka Streams, Debezium CDC, Apache Flink, Apache Spark",
                      "Full SASL Security: SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512, TLS/mTLS",
                      "Transparent Protocol Bridging: Native and Kafka clients read and write the exact same partitions"
                  ],
                  subtitle="100% Drop-In Replacement",
                  accent_color="#fbbf24", badge="Drop-In", badge_color="#b45309",
                  code_block="# Switch from Kafka to AeroStream with one line of configuration:\nbootstrap.servers=localhost:9092\n# Zero code changes required in your application!")

    sy = 1640
    diag.add_container(60, sy, 3080, 470, "OFFICIAL MULTI-LANGUAGE CLIENT SDK ECOSYSTEM",
                       "Native & Kafka protocol client libraries engineered for maximum performance, ergonomic APIs, and production reliability",
                       accent_color="#818cf8", border_color="#818cf8", badge="5 Official SDKs")

    sdks = [
        ("Go SDK (`sdks/go`)", "Goroutine Channels & Pooling", "#38bdf8", [
            "Native Go channels for produce & consume",
            "Automatic connection pooling & reconnects",
            "Zero allocation record deserializer",
            "Full context.Context lifecycle support"
        ]),
        ("Rust SDK (`sdks/rust`)", "Tokio Async & Zero-Copy", "#34d399", [
            "Pure async Tokio client with zero-copy bytes",
            "Built-in batching with configurable linger_ms",
            "Futures-based stream consumer interface",
            "Type-safe error handling with thiserror"
        ]),
        ("Java SDK (`sdks/java`)", "High-Throughput NIO", "#fbbf24", [
            "High-throughput Java NIO socket channel engine",
            "CompletableFuture asynchronous pipeline",
            "Drop-in Producer / Consumer interface",
            "Compatible with Spring Boot & Micronaut"
        ]),
        (".NET SDK (`sdks/dotnet`)", "C# 12 / .NET 8 ValueTask", "#818cf8", [
            "Modern C# 12 / .NET 8 async streaming",
            "ValueTask and System.IO.Pipelines zero-copy",
            "IAsyncEnumerable consumer stream support",
            "Native AOT compilation compatible"
        ]),
        ("Node.js SDK (`sdks/nodejs`)", "TypeScript Stream Buffers", "#fb7185", [
            "Full TypeScript types and ESM / CJS dual build",
            "Node.js EventEmitter and Readable/Writable streams",
            "High-performance native Buffer operations",
            "Zero runtime external dependencies"
        ])
    ]

    sdk_w = 580
    sdk_gap = 35
    for i, (stitle, ssub, scol, sitems) in enumerate(sdks):
        sx = 100 + i * (sdk_w + sdk_gap)
        diag.add_card(sx, sy + 120, sdk_w, 320, stitle, items=sitems, subtitle=ssub, accent_color=scol, badge="Official SDK", badge_color="#0f172a")

    return diag.export_svg(), diag.export_excalidraw()


def main():
    generators = [
        ("dual_engine_architecture", build_dual_engine),
        ("shard_per_core_architecture", build_shard_per_core),
        ("produce_fetch_pipeline", build_produce_fetch_pipeline),
        ("controller_broker_orchestration", build_controller_broker_orchestration),
        ("cluster_topology_scale_down", build_cluster_topology_scale_down),
        ("tiered_storage_pipeline", build_tiered_storage_pipeline),
        ("native_and_kafka_dual_protocol", build_native_and_kafka_dual_protocol)
    ]

    print("=== AeroStream Architecture Diagram Generator ===")
    print(f"Docs Diagrams:      {DOCS_DIAGRAMS}")
    print(f"Core Docs Diagrams: {CORE_DOCS_DIAGRAMS}")
    print(f"Docs Images:        {DOCS_IMAGES}")
    print()

    for name, gen_func in generators:
        print(f"[*] Processing '{name}'...")
        svg_content, exc_json = gen_func()

        # Save Excalidraw JSON
        exc_docs = os.path.join(DOCS_DIAGRAMS, f"{name}.excalidraw")
        exc_core = os.path.join(CORE_DOCS_DIAGRAMS, f"{name}.excalidraw")
        with open(exc_docs, "w") as f:
            json.dump(exc_json, f, indent=2)
        with open(exc_core, "w") as f:
            json.dump(exc_json, f, indent=2)
        print(f"    -> Excalidraw: {exc_docs} ({os.path.getsize(exc_docs):,} bytes, {len(exc_json['elements'])} elements)")

        # Save SVG
        svg_docs = os.path.join(DOCS_DIAGRAMS, f"{name}.svg")
        svg_core = os.path.join(CORE_DOCS_DIAGRAMS, f"{name}.svg")
        with open(svg_docs, "w") as f:
            f.write(svg_content)
        with open(svg_core, "w") as f:
            f.write(svg_content)
        print(f"    -> SVG: {svg_docs} ({os.path.getsize(svg_docs):,} bytes)")

        # Render High-Resolution PNG
        png_docs = os.path.join(DOCS_IMAGES, f"{name}.png")
        cmd = ["rsvg-convert", "-w", str(WIDTH), "-h", str(HEIGHT), svg_docs, "-o", png_docs]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            print(f"    [!] Error running rsvg-convert for {name}: {res.stderr}")
            sys.exit(1)
        else:
            print(f"    -> PNG (3200x2160): {png_docs} ({os.path.getsize(png_docs):,} bytes)")

    # Ensure core/docs/images is linked or populated
    if not os.path.exists(CORE_DOCS_IMAGES):
        try:
            os.symlink(os.path.relpath(DOCS_IMAGES, os.path.dirname(CORE_DOCS_IMAGES)), CORE_DOCS_IMAGES)
            print(f"[*] Created symlink: {CORE_DOCS_IMAGES} -> {DOCS_IMAGES}")
        except Exception as e:
            print(f"[!] Could not create symlink for core/docs/images: {e}")

    print()
    print("=== All 7 Architecture Diagrams Successfully Generated & Rendered! ===")

if __name__ == "__main__":
    main()
