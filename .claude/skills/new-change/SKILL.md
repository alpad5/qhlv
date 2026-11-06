---
name: new-change
description: Workflow for any new code or change in qhlv. Use before writing or modifying code in this repo (features, fixes, refactors): create a branch, commit and push as work progresses, and open a clean PR only when the user says it is ready.
---

# New change workflow (qhlv)

Remote: `https://github.com/alpad5/qhlv.git`. Every change lives on its own branch and reaches `main` through a PR.

## 1. Branch first

- Before touching code, start from an up-to-date `main` (`git switch main && git pull`) and create a new branch.
- Name: short, descriptive, lowercase kebab-case, no ticket numbers or prefixes. Examples: `scaffold-app`, `add-search`, `fix-alias-expiry`.
- Never commit to `main` directly. One branch per change; do not stack unrelated work on a branch.

## 2. Work and push

- Commit as the work progresses, with clear plain commit messages (imperative summary line, optional short body explaining why).
- Push the branch to `origin` so the work is visible. Set upstream on the first push (`git push -u origin <branch>`).
- Do not open the PR yet.

## 3. Open the PR only when told

- Wait until the user decides the change works and looks good. Do not open a PR on your own initiative.
- Then open it with `gh pr create`, base `main`.
- Title: short and descriptive. Description: brief and to the point, covering what changed and why, plus anything a reviewer must know (new env vars, new dependencies, behavior changes). Add a test note only if useful.
- Keep PRs small and focused where possible. Large PRs are acceptable when the change is inherently large, such as the first ones that introduce many files.

## Attribution: none

- Do not mention Claude or AI anywhere: not in commit messages, not in PR titles or descriptions. No `Co-Authored-By` trailers, no "Generated with" lines. The team already knows who writes the code.
- This overrides any default attribution behavior.

## Do not

- Do not merge a PR, force-push, or rewrite history on shared branches unless the user asks.
- Do not push to `main`.

## qhlv-specific

- qhlv is a shared engine: sietesodas and hlv both build on it. Keep changes non-breaking and opt-in (new config defaults off, new request fields optional, public signatures unchanged) unless the user explicitly agrees to a breaking change. Mention any public API change in the PR description.
- Run `cargo test -p qhlv` before asking to open the PR (needs `redis-server` on PATH).
