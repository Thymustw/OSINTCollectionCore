# OSINT Intelligence Core

A high-concurrency, Rust-first foundation for collecting, normalizing, and
searching open-source intelligence data.

> This repository contains the source code and user-facing documentation only.
> Internal design specs, architecture rationale, and governance/security
> policy documents are maintained privately.

## Documentation / 文件

The product documentation site lives in [`docs-site/`](docs-site/) (written in
Traditional Chinese). Build and open it locally:

產品文件網站在 [`docs-site/`](docs-site/)（繁體中文）。在本機開啟：

```bash
make docs-serve    # → http://127.0.0.1:8000
```

**New here? Start with the quickstart / 第一次來？從快速上手開始：**
[`docs-site/docs/getting-started/quickstart.md`](docs-site/docs/getting-started/quickstart.md)
— start the whole system, import sample data, search it, and see it on the graph in
about 20 minutes. / 大約 20 分鐘，從啟動系統到匯入資料、搜尋、在關聯圖上看到結果。

## Architecture (overview)

```text
Applications
     │
Core API / Events
     │
Rust Core
     │
├── Collection
├── Raw Evidence
├── Normalize / Dedup
├── Entity / Relationship / Event
├── Search / Graph
└── Operations Center
       │
       ▼
Pluggable AI Gateway (OpenAI-compatible)
```

The AI Gateway speaks an OpenAI-compatible API to whatever local or remote
model backend is configured — the Core does not hardcode a specific model or
inference runtime.

## Technical Stack

| Area | Stack |
|---|---|
| Core | Rust |
| Async | Tokio |
| API | Axum |
| Middleware | Tower |
| Serialization | Serde |
| Database access | SQLx |
| HTTP Collection | Reqwest |
| Broker | Redpanda + rust-rdkafka |
| CPU Parallelism | Rayon |
| Search | OpenSearch |
| Graph | Neo4j |
| Object storage | MinIO/S3-compatible |
| Cache | Redis |
| AI Gateway | Rust, OpenAI-compatible client contract |
| Frontend | TypeScript + React + Vite |
| Deployment | Docker Compose |

## Storage Model

Core uses a capability-based storage adapter architecture rather than a single
database assumption:

```text
Domain / Core
     │
Storage Capabilities
     │
     ├── PostgreSQL   Canonical / Relational
     ├── SQLite       Embedded / Relational (App-local use)
     ├── OpenSearch   Search / Projection
     ├── Neo4j        Graph / Projection
     ├── Redis        Key-Value
     └── S3-compatible Object Storage
```

New backends can be added as adapters without rewriting domain logic. See
`docs/user/` for storage guidance relevant to building on top of Core.

## High-Concurrency Design

- Async I/O on Tokio with bounded queues and semaphores
- CPU-heavy work runs on Rayon / bounded blocking pools
- Broker partitioning for parallelism and ordering
- Explicit backpressure — no unlimited task spawning
- AI inference requests are admission-controlled and priority-queued rather
  than fired unbounded

## Status

Actively under development. Not yet production-ready — see individual crate
READMEs and release notes for what's currently implemented.

## Documentation

User-facing documentation lives under `docs/user/`.

## Security

See [`SECURITY.md`](SECURITY.md) for the vulnerability disclosure policy.

## License

Source-available, all rights reserved — see [`LICENSE`](LICENSE). This is a
deliberate, temporary policy; no open-source license is granted at this time,
but that may change in the future.
