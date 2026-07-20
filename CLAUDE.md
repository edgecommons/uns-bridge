# uns-bridge (Claude Code)

EdgeCommons **bridge** component (Rust), `com.mbreissi.edgecommons.UnsBridge`. The full picture —
what this component is, config location, and the org conventions it follows — lives in `AGENTS.md`
and is shared with every agent tool. It is imported here in full:

@AGENTS.md

## Local-dev notes

- **Dependency pin**: `Cargo.toml`'s `edgecommons` dependency is a git `rev` pin (CI resolves this
  exact rev via the committed `Cargo.lock`). For local dev against an uncommitted sibling
  `core/libs/rust` checkout, a **gitignored** `.cargo/config.toml` `[patch]` override redirects the
  dependency without touching the committed pin:

  ```toml
  [patch."https://github.com/edgecommons/edgecommons.git"]
  edgecommons = { path = "../core/libs/rust" }

  [net]
  git-fetch-with-cli = true
  ```

- **Regenerating `Cargo.lock`**: only ever regenerate it from a checkout with **no**
  `.cargo/config.toml` in scope (this repo's own, or any ancestor directory's — Cargo searches
  upward). A build made with the `[patch]` override active will otherwise rewrite the in-memory
  resolution to the local sibling path and can leave a `[[patch.unused]]` marker in `Cargo.lock`
  when you run `cargo generate-lockfile`/`cargo update` under it — never commit that. See
  `DESIGN.md` D-UB-1.
- **Coverage**: `cargo llvm-cov --ignore-filename-regex 'main\.rs' --fail-under-lines 90` mirrors the
  CI `coverage` job locally; install `cargo-llvm-cov` first (`cargo install cargo-llvm-cov` or
  `taiki-e/install-action@cargo-llvm-cov` in CI).
