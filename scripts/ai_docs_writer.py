#!/usr/bin/env python3
"""
AeroStream AI Documentation Writer (Google Antigravity & Gemini Engine)
======================================================================
Automated tool for CI/CD pipelines and local development that:
1. Detects code diffs (Rust Broker, Go Controller, Proto, Client SDKs, UI).
2. Filters out non-code changes to prevent redundant updates.
3. Invokes Google Antigravity (`agy` CLI) or Gemini API (`GEMINI_API_KEY` / Service Account).
4. Synchronizes and updates MkDocs Material docs (`docs/`), Core whitepapers (`core/docs/`),
   and root `README.md`.
5. Validates MkDocs build integrity.
6. Generates PR metadata for `peter-evans/create-pull-request`.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.request
import urllib.error
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configuration
REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"
CORE_DOCS_DIR = REPO_ROOT / "core" / "docs"
PR_SUMMARY_FILE = REPO_ROOT / "pr_summary.md"

# Code paths monitored for documentation updates
MONITORED_PATHS = [
    "rust-broker",
    "go-controller",
    "proto",
    "client",
    "site/src",
    "Dockerfile",
    "docker-compose.yml",
]

# Ignored patterns (docs, build outputs, lock files)
IGNORED_PATTERNS = [
    r"^docs/",
    r"^core/docs/",
    r"^site/dist/",
    r"^\.github/workflows/ai-docs-writer\.yml",
    r"\.md$",
    r"\.lock$",
    r"target/",
    r"node_modules/",
    r"__pycache__/",
    r"\.git/",
]


def run_cmd(cmd: List[str], cwd: Optional[Path] = None, check: bool = True) -> Tuple[int, str, str]:
    """Run shell command and return (exit_code, stdout, stderr)."""
    proc = subprocess.run(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        capture_output=True,
        text=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr}")
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def get_git_diff(base_ref: Optional[str] = None, head_ref: Optional[str] = None) -> Tuple[str, List[str], str]:
    """
    Get git diff and list of changed files between base and head.
    Falls back intelligently depending on CI/CD environment.
    """
    # 1. Determine base and head
    if not head_ref:
        head_ref = "HEAD"

    if not base_ref:
        # Check GitHub Actions environment variables
        github_base_ref = os.getenv("GITHUB_BASE_REF")
        github_event_before = os.getenv("GITHUB_EVENT_BEFORE")

        if github_base_ref:
            base_ref = f"origin/{github_base_ref}"
        elif github_event_before and github_event_before != "0000000000000000000000000000000000000000":
            base_ref = github_event_before
        else:
            # Check if HEAD~1 exists
            ret, _, _ = run_cmd(["git", "rev-parse", "--verify", "HEAD~1"], check=False)
            if ret == 0:
                base_ref = "HEAD~1"
            else:
                base_ref = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"  # empty tree hash

    print(f"[AI Doc Writer] Analyzing diff: {base_ref} ... {head_ref}")

    # 2. Get list of changed files
    ret, files_out, _ = run_cmd(["git", "diff", "--name-only", base_ref, head_ref], check=False)
    if ret != 0:
        print(f"[AI Doc Writer] Warning: Failed to diff against {base_ref}. Falling back to HEAD~1.")
        base_ref = "HEAD~1"
        _, files_out, _ = run_cmd(["git", "diff", "--name-only", base_ref, head_ref])

    all_changed_files = [f.strip() for f in files_out.splitlines() if f.strip()]

    # 3. Filter monitored code files
    code_changed_files = []
    for f in all_changed_files:
        # Check if ignored
        if any(re.search(pat, f) for pat in IGNORED_PATTERNS):
            continue
        # Check if under monitored paths
        if any(f.startswith(mon) for mon in MONITORED_PATHS):
            code_changed_files.append(f)

    if not code_changed_files:
        return "", [], ""

    # 4. Get commit log summary
    _, commit_log, _ = run_cmd(["git", "log", f"{base_ref}..{head_ref}", "--oneline", "-n", "10"], check=False)

    # 5. Get actual code diff
    _, raw_diff, _ = run_cmd(["git", "diff", base_ref, head_ref, "--"] + code_changed_files, check=False)

    # Truncate diff if excessively large (> 80KB)
    if len(raw_diff) > 80000:
        print(f"[AI Doc Writer] Note: Diff is large ({len(raw_diff)} bytes), truncating for context window.")
        raw_diff = raw_diff[:80000] + "\n\n... [DIFF TRUNCATED FOR CONTEXT LIMIT] ...\n"

    return raw_diff, code_changed_files, commit_log


def list_existing_docs() -> List[str]:
    """Returns relative paths of existing documentation files."""
    docs = []
    if DOCS_DIR.exists():
        for p in DOCS_DIR.glob("**/*.md"):
            docs.append(str(p.relative_to(REPO_ROOT)))
    if CORE_DOCS_DIR.exists():
        for p in CORE_DOCS_DIR.glob("**/*.md"):
            docs.append(str(p.relative_to(REPO_ROOT)))
    docs.append("README.md")
    return sorted(docs)


def generate_with_antigravity_cli(prompt: str) -> Optional[str]:
    """Invoke local Antigravity CLI (`agy`) in headless print mode."""
    agy_path = shutil.which("agy")
    if not agy_path:
        return None

    print("[AI Doc Writer] Detected Google Antigravity CLI (`agy`). Executing in headless mode...")
    cmd = [
        agy_path,
        "-p", prompt,
        "--output-format", "text",
        "--effort", "high",
    ]
    try:
        ret, stdout, stderr = run_cmd(cmd, check=False)
        if ret == 0 and stdout:
            return stdout
        print(f"[AI Doc Writer] Antigravity CLI returned {ret}: {stderr}")
    except Exception as e:
        print(f"[AI Doc Writer] Antigravity CLI execution error: {e}")
    return None


def generate_with_gemini_api(prompt: str, api_key: str, model: str = "gemini-2.5-flash") -> Optional[str]:
    """Invoke Gemini API using direct HTTP request (zero external dependencies)."""
    print(f"[AI Doc Writer] Calling Gemini API ({model})...")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt}
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 8192,
        }
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            candidates = data.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                text_parts = [p.get("text", "") for p in parts if "text" in p]
                return "".join(text_parts)
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        print(f"[AI Doc Writer] Gemini API HTTPError ({e.code}): {error_body}")
    except Exception as e:
        print(f"[AI Doc Writer] Gemini API error: {e}")
    return None


def call_ai_engine(prompt: str) -> str:
    """Orchestrates AI engine selection: agy CLI -> GEMINI_API_KEY."""
    # 1. Check local agy CLI
    output = generate_with_antigravity_cli(prompt)
    if output:
        return output

    # 2. Check GEMINI_API_KEY environment variable
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        # Try gemini-2.5-flash then gemini-2.5-pro
        for model in ["gemini-2.5-flash", "gemini-2.5-pro"]:
            output = generate_with_gemini_api(prompt, api_key, model=model)
            if output:
                return output

    # If neither succeeded, raise informative error
    raise RuntimeError(
        "No AI authentication available. Please provide GEMINI_API_KEY secret in GitHub Actions "
        "or ensure `agy` (Google Antigravity CLI) is installed and authenticated."
    )


def build_analysis_prompt(changed_files: List[str], commit_log: str, raw_diff: str, existing_docs: List[str]) -> str:
    """Builds structured prompt instructing the model to generate doc changes."""
    docs_list_str = "\n".join(f"- {d}" for d in existing_docs)
    changed_files_str = "\n".join(f"- {f}" for f in changed_files)

    return f"""You are the Technical Documentation Architect & Writer for AeroStream (a high-performance dual-engine distributed streaming platform combining a Rust Shard-per-Core zero-copy broker with a Go Raft controller, 100% Kafka wire protocol compatible).

Code changes have recently been pushed. Your task is to analyze the git diff and update or create relevant documentation files to ensure the docs accurately reflect the latest code.

### Repository Context & Guidelines:
1. MkDocs Material Website Documentation lives in `docs/`:
   - `docs/index.md`: Quickstart & Overview (Quay image `quay.io/gradientgeeks/aerostream:latest`, ports 9091, 9092, 9001, 8001, 7001).
   - `docs/architecture.md`: Dual-Engine Architecture (Go Raft control plane, Rust zero-copy data plane, ShardRouter, sendfile pipeline).
   - `docs/kafka-protocol.md`: Apache Kafka Compatibility (Port 9092, ApiKeys 0-36, in-place base offset patching, long polling).
   - `docs/schema-registry.md`: Built-in Schema Registry (Avro, JSON, Protobuf).
   - `docs/transforms.md`: In-Broker Stream Transforms & WASM.
   - `docs/security-rbac.md`: Enterprise Security, SASL PLAIN/SCRAM, Zero-Trust RBAC.
   - `docs/tiered-storage.md`: Multi-Cloud Tiered Storage (hot logs vs cold archive hard-links).
   - `docs/operations.md`: Production Operations, CPU affinity, broker draining, Prometheus metrics.
   - `docs/benchmarks.md`: Performance Measurement vs Apache Kafka.
2. Low-level Core Architectural Whitepapers live in `core/docs/`:
   - `core/docs/ARCHITECTURE.md`, `core/docs/API_REFERENCE.md`, `core/docs/SHARD_PER_CORE.md`, `core/docs/OPERATOR_GUIDE.md`, etc.
3. Root `README.md`: High-level feature matrix and getting started.

### Recent Commits:
{commit_log}

### Changed Code Files:
{changed_files_str}

### Existing Documentation Files:
{docs_list_str}

### Code Diff:
```diff
{raw_diff}
```

### Instructions:
1. Determine if the code changes introduce new features, API keys, endpoints, configuration options, architecture alterations, or bug fixes that impact documentation.
2. If NO documentation updates are needed (e.g. internal refactoring with no behavioral change), respond with:
   NO_DOC_UPDATES_NEEDED: <reason>
3. If updates ARE needed:
   - Provide the complete updated content for each modified or new documentation file.
   - Format each file clearly using delimiters:
     <<<START_FILE: relative/path/to/file.md>>>
     [Full Markdown Content for this file]
     <<<END_FILE>>>
   - Maintain the highest standard of technical accuracy, proper MkDocs Material admonitions (`!!! note`, `!!! tip`), code snippets, and table structures.
   - Provide a concise summary of the changes in a block at the end:
     <<<PR_SUMMARY>>>
     [Summary of updates made for the Pull Request description]
     <<<END_PR_SUMMARY>>>
"""


def apply_doc_updates(ai_response: str) -> Tuple[List[str], str]:
    """Parses delimiters from AI response and writes updated doc files."""
    if "NO_DOC_UPDATES_NEEDED:" in ai_response:
        reason = ai_response.split("NO_DOC_UPDATES_NEEDED:", 1)[1].strip()
        print(f"[AI Doc Writer] AI determined no doc updates are required: {reason}")
        return [], ""

    file_pattern = re.compile(
        r"<<<START_FILE:\s*([^\n>]+)>>>\s*\n(.*?)\n<<<END_FILE>>>",
        re.DOTALL
    )

    summary_pattern = re.compile(
        r"<<<PR_SUMMARY>>>\s*\n(.*?)\n<<<END_PR_SUMMARY>>>",
        re.DOTALL
    )

    updated_files = []
    for match in file_pattern.finditer(ai_response):
        rel_path = match.group(1).strip()
        content = match.group(2)

        target_file = REPO_ROOT / rel_path
        target_file.parent.mkdir(parents=True, exist_ok=True)

        with open(target_file, "w", encoding="utf-8") as f:
            f.write(content.rstrip() + "\n")

        print(f"[AI Doc Writer] Updated: {rel_path}")
        updated_files.append(rel_path)

    summary_match = summary_pattern.search(ai_response)
    pr_summary = summary_match.group(1).strip() if summary_match else "Documentation updated based on code changes."

    return updated_files, pr_summary


def validate_mkdocs() -> bool:
    """Verifies that MkDocs builds cleanly with the updated documentation."""
    mkdocs_cmd = shutil.which("mkdocs")
    if not mkdocs_cmd:
        print("[AI Doc Writer] mkdocs binary not found; skipping build validation.")
        return True

    print("[AI Doc Writer] Validating documentation build with `mkdocs build`...")
    ret, stdout, stderr = run_cmd([mkdocs_cmd, "build", "--quiet"], check=False)
    if ret != 0:
        print(f"[AI Doc Writer] MkDocs validation warning/error:\n{stderr or stdout}")
        return False
    print("[AI Doc Writer] ✓ MkDocs build succeeded cleanly.")
    return True


def main():
    parser = argparse.ArgumentParser(description="AeroStream Automated AI Documentation Writer")
    parser.add_argument("--base", help="Git base commit/branch (default: auto-detected or HEAD~1)")
    parser.add_argument("--head", help="Git head commit (default: HEAD)")
    parser.add_argument("--dry-run", action="store_true", help="Perform analysis without writing files")
    parser.add_argument("--force-all", action="store_true", help="Force doc inspection of all tracked code")
    args = parser.parse_args()

    print("=================================================================")
    print("🚀 AeroStream AI Documentation Writer (Google Antigravity/Gemini)")
    print("=================================================================")

    # 1. Compute git diff
    raw_diff, changed_files, commit_log = get_git_diff(args.base, args.head)

    if not changed_files and not args.force_all:
        print("[AI Doc Writer] No code changes detected under monitored paths. Exiting cleanly.")
        sys.exit(0)

    print(f"[AI Doc Writer] Detected {len(changed_files)} changed code file(s):")
    for f in changed_files[:10]:
        print(f"  • {f}")
    if len(changed_files) > 10:
        print(f"  ... and {len(changed_files) - 10} more files.")

    # 2. List existing docs
    existing_docs = list_existing_docs()

    # 3. Construct AI Prompt
    prompt = build_analysis_prompt(changed_files, commit_log, raw_diff, existing_docs)

    if args.dry_run:
        print("[AI Doc Writer] Dry-run enabled. Prompt prepared. Skipping AI execution.")
        print(f"Prompt length: {len(prompt)} characters.")
        sys.exit(0)

    # 4. Invoke AI Engine
    ai_response = call_ai_engine(prompt)

    # 5. Apply documentation updates
    updated_files, pr_summary = apply_doc_updates(ai_response)

    if not updated_files:
        print("[AI Doc Writer] No documentation files were updated.")
        sys.exit(0)

    # 6. Validate MkDocs build
    validate_mkdocs()

    # 7. Write PR Summary for GitHub Actions
    summary_markdown = f"""### 🤖 Automated Documentation Update

This Pull Request was generated by **Google Antigravity & Gemini Document Writer** in response to recent codebase changes.

#### 📝 Updated Documentation Files:
{chr(10).join(f"- `{f}`" for f in updated_files)}

#### 📋 Summary of Changes:
{pr_summary}

---
*Generated automatically by `.github/workflows/ai-docs-writer.yml`*
"""
    with open(PR_SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(summary_markdown)

    print(f"[AI Doc Writer] Wrote PR summary to {PR_SUMMARY_FILE}")
    print("[AI Doc Writer] ✓ Documentation synchronization completed successfully.")


if __name__ == "__main__":
    main()
