# Development workflow

This is a single-developer project. There are no reviewers, no roles and no approval
gates — a pull request here is a **checkpoint and a record**, not a request for
someone else's sign-off.

It is still worth doing. The PR is where the evidence for a change lives: what was
measured, what was rejected, what is still open. `main` stays a clean sequence of
completed, CI-verified changes rather than a stream of work in progress.

## Branching

`main` is never committed to directly.

```bash
git switch -c feat/phase-2-dbt-models     # or fix/, chore/, docs/, refactor/, test/
# ... work ...
make check                                 # lint + types + unit tests
make test-container                        # DAG integrity + integration
git push -u origin HEAD
gh pr create --fill
```

Merge with **squash**, so one PR becomes one commit on `main`. The working history
stays visible inside the PR. Delete the branch afterwards.

## Enforcement

Run once after cloning:

```bash
make hooks        # or: .\make.ps1 hooks
```

That points `core.hooksPath` at [`.githooks/`](../.githooks/), which refuses a direct
push to `main`. Override deliberately with `WIKIFLOW_ALLOW_MAIN_PUSH=1 git push`.

This is a client-side guard, not real enforcement. Server-side enforcement needs a
GitHub **ruleset**, which is unavailable on a free private repository:

> Upgrade to GitHub Pro or make this repository public to enable this feature.

The intended configuration — require a PR, require both CI jobs to pass, squash only,
block force-push and deletion — is kept ready to apply at
[`.github/rulesets/main.json`](../.github/rulesets/main.json):

```bash
gh api -X POST repos/:owner/:repo/rulesets --input .github/rulesets/main.json
```

It requires no approving reviews, since on a solo repo that would simply be a lock.

## Quality gates

Both run in CI and both must pass before merge:

| Job | Covers |
|---|---|
| `lint / types / unit tests` | ruff lint + format, mypy strict, unit tests. No Docker, no network. |
| `DAG integrity` | Builds the real Airflow image, imports every DAG, runs DAG + integration tests. |

Locally: `make check` and `make test-container` (`.\make.ps1` on Windows).

## Commits

Explain *why*, not *what* — the diff shows what changed. Where a decision rests on
evidence, put the numbers in the message. Several commits here record measured results
that justify a choice, and that is deliberate: it is the difference between "switched
to a CSV load" and "switched to a CSV load, 23.6s → 3.5s, identical output".

Anything that changes a design decision adds or updates an ADR in
[`docs/adr/`](adr/).

## A note on history

Commits `a49c2de` through `bb70f79` (Phases 0 and 1) were pushed directly to `main`
before this workflow was adopted. They are left in place rather than rewritten:
rewriting published history to make a process look tidier is a worse habit than the
untidiness it hides.
