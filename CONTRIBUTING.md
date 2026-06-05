# Contributing to worklease

Thank you for your interest in contributing. This document covers how to report bugs, propose
changes, and submit pull requests.

---

## Reporting bugs

Open a GitHub issue. Include:

- Go version (`go version`)
- worklease version or commit SHA
- A minimal reproduction — the smallest code that triggers the bug
- What you expected to happen and what actually happened

---

## Proposing changes

For non-trivial changes, open an issue before writing code. This lets us discuss whether the
change fits the library's scope and how it should be designed before you invest time implementing
it.

For minor fixes (documentation, typos, obvious bugs), a pull request without a prior issue is
fine.

---

## Design decisions and ADRs

`worklease` maintains an [Architecture Decision Record](docs/adr/) for every significant design
choice. If your change involves a new design decision — a new API, a change to backend
semantics, a new option type — include an ADR in your pull request.

ADRs live in `docs/adr/` and follow the format of the existing files. Use the next available
number and a short slug: `NNNN-short-description.md`.

---

## Development setup

```bash
git clone https://github.com/aetomala/worklease
cd worklease
go build ./...
go test -race ./...
```

The PostgreSQL integration tests require a running PostgreSQL instance. Set `WORKLEASE_TEST_DSN`
to a valid DSN before running them:

```bash
WORKLEASE_TEST_DSN="postgres://user:pass@localhost/worklease_test?sslmode=disable" \
    go test -race -tags integration ./backend/postgres/...
```

---

## Pull request guidelines

- **One logical change per PR.** Keep pull requests focused — a bug fix is a bug fix, not a
  bug fix plus a refactor plus a new feature.
- **Tests required.** New behavior must have tests. Bug fixes should include a test that would
  have caught the bug.
- **Table-driven tests.** Unit tests use table-driven format with named cases. Test names are
  sentences describing the scenario, not identifiers.
- **Pass CI.** All checks must pass before review. Run `make ci` locally first.
- **Update CHANGELOG.md.** Add a line under `[Unreleased]` describing the change.

---

## Branch naming

```
feat/short-description
fix/short-description
chore/short-description
docs/short-description
```

---

## Code style

- All exported identifiers have godoc-format comments starting with the identifier name.
- `doc.go` at the package root states the package's single responsibility.
- Why comments only — inline comments explain intent, not what the code does.
- Errors returned from library functions are wrapped with `fmt.Errorf` including enough context
  to identify the operation without a stack trace.
- No abbreviations in exported names: `workID` not `wid`, `holderID` not `hid`.

---

## License

By contributing, you agree that your contributions will be licensed under the
[Apache 2.0 License](LICENSE).
