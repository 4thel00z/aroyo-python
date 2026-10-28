# AGENTS.md

Read `HANDOFF.md` first. It holds the goal, the sources and the open decisions.

## Repository

- Public repo: `github.com/4thel00z/aroyo-python`. Remote `origin` uses the SSH host alias
  `4thel00z.github.com`.
- Commit as `4thel00z <4thel00z@gmail.com>`, GPG-signed. Conventional commit titles
  (`feat(writer): ...`); the body is a few sentences plus verification evidence.
- Public repo: never mention employers, customers, internal services, tenants,
  bucket names, endpoints or local paths in code, tests, commits or PRs. Test data is
  synthetic.

## Rust

- No `.unwrap()`, `.expect()`, `panic!`, `unreachable!`, `todo!` or `unimplemented!`
  outside `#[cfg(test)]`. Deny them in workspace lints (`unwrap_used`, `expect_used`,
  `panic`, `unreachable`, `todo`, `unimplemented`). Allow them in tests via
  `clippy.toml` (`allow-unwrap-in-tests`, `allow-expect-in-tests`). Never use `#[allow]`
  to silence them. A panic across PyO3 becomes an exception that hides the real error.
- Errors map to Python exceptions in one place in `aroyo-py`.

## Python

- Full type annotations everywhere, including tests.
- No `assert` outside tests; raise `ValueError` / `RuntimeError`.
- Guard clauses: handle the early-exit case first, no if/else arms.
- No leading underscores on names.
- Tests use real components or in-memory fakes; no `mock.patch`.
- One import per line.

## Naming

- Name functions by what they return or do. No `<thing>_for(...)` names.
- Keep to the words in `HANDOFF.md` (upload, part, checkpoint, state, trailing bytes,
  commit, finish). Don't invent new metaphors.

## Docstrings and comments

- Short and factual.
