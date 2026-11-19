Baza
====

Fork of the archived [`instrumentisto/baza`](https://github.com/instrumentisto/baza): a small, single-node S3-compatible server on top of a plain directory tree.

**The work plan lives in [`docs/plan.md`](docs/plan.md).** Read its "Shared context" section and the phase you're working on before you start, and update its Progress list and "Notes for later phases" when a phase is done.




## Layout

An S3 object `bucket/key` is stored as `<root>/data/<bucket>/<key>`. There's no database, and buckets are top-level directories. `<root>/tmp/` is wiped on every start.

- `src/main.rs`: `baza` binary, the CLI (`clap`).
- `lib/src/lib.rs`: `baza-lib` (imported as `baza`), the filesystem storage. Each operation is a struct with an `impl Exec<Op> for Storage`. Build paths only through `RelativePath`, which prevents path traversal.
- `api/s3/src/lib.rs`: `baza-api-s3`, `S3<T>` implementing `s3s::S3` (`s3s` 0.17, `hyper` 1). Unimplemented methods fall back to `s3s`' default `NotImplemented`.
- `e2e/`: Cucumber tests using `rusoto_s3` against a server on `localhost:9294`. They inspect `.cache/baza/data` directly.
- `helm/baza/`: Helm chart.




## Conventions

- Match the existing style: every public item has a doc comment ending in a period, with intra-doc links. Errors use `tracerr::wrap!()`/`tracerr::new!()`, and operations use `#[tracing::instrument(level = "debug", err(Debug))]`.
- Sort attributes, and the items inside them, alphabetically (`CONTRBUTING.md`).
- Update `CHANGELOG.md` (and `helm/baza/CHANGELOG.md` for chart changes) for user-visible changes, under the `unreleased` section.




## Verification

Run all of these before finishing a phase:

```bash
make cargo.fmt check=yes      # nightly rustfmt
make lint                     # clippy -D warnings
make test.unit
make test.e2e start-app=yes   # builds, starts Baza, runs Cucumber, stops it
```

CI pins Rust via `RUST_VER` in `.github/workflows/ci.yml` (1.99); MSRV is 1.96 (required by `s3s`). Single E2E scenario: `make test.e2e start-app=yes only='<regex>'`.




## Git

- `origin` is the fork (`SleepySquash/baza`), `upstream` is the archived original. **Never push to `upstream`.**
- Work directly on `main` and leave changes **unstaged**. Don't create branches, stage, commit or push: the user does that.
- Open PRs with `gh pr create --repo SleepySquash/baza --base main`. Without `--repo`, `gh` targets the upstream parent.
