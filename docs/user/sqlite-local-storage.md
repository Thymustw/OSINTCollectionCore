# SQLite / Local App Storage

SQLite is supported as an embedded/local relational storage option for specialized Apps, offline workspaces, and selected projections.

It is not the default canonical database of OSINT Core.

Typical architecture:

```text
OSINT Core
   ↓ API / Events
Specialized App
   ↓
SQLite
```

This keeps the App portable without coupling it to Core PostgreSQL internals.

Detailed App-specific instructions should be added when the first SQLite-based App is implemented.
