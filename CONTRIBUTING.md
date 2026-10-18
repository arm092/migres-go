# Contributing

Thanks for helping improve Migres Go. Start with a reproducible issue for data correctness, recovery, or compatibility changes.

## Bug reports

Include MySQL/ClickHouse versions and settings, engine/topology, redacted configuration, the source table definition, triggering DDL/transaction, operation status, and expected versus actual behavior. Prefer a small fixture that reproduces the problem.

Do not post credentials, private keys, production rows, debug payload archives, or raw journals. Replace source values with synthetic data while preserving the failing type/key/schema shape.

## Development

Use Go 1.26.9 and run relevant checks from [verification](docs/verification.md). Database/fault fixtures are disposable; source-purge tests and replica-loss tests deliberately mutate their named test services.

Keep Producer, Transformer, and Consumer responsibilities explicit. Preserve table order, atomic durable handoffs, stable replay identities, and ownership checks. Add focused regression coverage for a demonstrated correctness or recovery gap; documentation-only changes need link/example verification rather than redundant tests.

For larger changes, describe the guarantees, failure states, recovery path, resource bounds, and acceptance cases before implementation. Unimplemented ideas belong in the [roadmap](ROADMAP.md), not the supported feature list.

Use `tasks/` for contribution branch names. The default branch is `master`. Keep private process notes, local credentials, generated journals, and test reports out of commits. Public user documentation belongs in `docs/`.

## Pull requests

Explain the concrete problem, resulting behavior, verification, and remaining limitations. Update user documentation when behavior or configuration changes. Keep unrelated refactors separate.

Contributions are covered by the repository's [MIT license](LICENSE).
