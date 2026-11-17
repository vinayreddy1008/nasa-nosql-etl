# Developer workflow — local Mac and GitHub

## What synchronizes automatically?

The project folder on your Mac is the **working tree**. Editing a file changes the Mac copy only. `git add` stages selected changes, `git commit` creates a local snapshot, and `git push` publishes the snapshot to GitHub. A successful push does **not** copy ignored local NASA logs or databases to GitHub. That is intentional.

Use the helper script from the repository root:

```bash
./scripts/verify_and_publish.sh "Describe the tested change"
```

This helper runs the automated tests, checks that the remote branch has not advanced unexpectedly, stages project source/docs/tests only, refuses suspicious artifacts, creates a commit (if anything changed), and pushes to `origin/main`.

The helper **does not rerun the 3.46-million-record ETL**. When changing parsing, aggregation, batching or SQL schemas, first perform both live backend runs and a fresh `python -m scripts.phase1_audit`, then publish. Tests and a previously successful comparison cannot establish that changed backend code still works end to end.

## Everyday flow

1. Make edits inside the one active `NASA_Phase1_From_Scratch/` folder.
2. Run `python -m pytest -q` (the helper also runs it).
3. For backend/SQL changes, execute affected full-data pipeline(s) and `python -m scripts.phase1_audit` before publishing.
4. Call `./scripts/verify_and_publish.sh "Meaningful commit message"`.
5. Confirm the printed local and remote commit IDs match.

To inspect changes without publishing:

```bash
git status --short
git diff
git diff --check
git log --oneline -5
```

## Team contributions and collaboration

If teammates add files on GitHub or push to `main`, don't blindly overwrite them. The helper uses `git fetch` and refuses to push when local history is behind or diverged. Review remote changes, use `git pull --ff-only` if it can safely fast-forward, and resolve conflicts deliberately if necessary. For concurrent implementation work, prefer short-lived feature branches and pull requests rather than direct pushes to the same files.

Do not invent contribution history or list people as authors of work they have not actually performed. Git history and documented review/testing are the reliable record of changes.

## Files never to commit

- `data/NASA_access_log_*` and any `.gz` downloads: raw official data is locally generated/downloaded.
- `work/`: Hadoop intermediate output and diagnostic logs.
- `.venv/`, `__pycache__/`, `.pytest_cache/`: local dependencies/caches.
- `.env` and other secrets/credentials/tokens.
- Old ZIP handoffs, temporary duplicates, benchmark scratch files.

The helper checks for staged archives, project data or secrets and unusually large files before committing. **Never run `docker compose down -v` unless you deliberately want to delete the project database volumes.**

## Local checkout and remote

Current intended default: local `main` tracks `origin/main` at the GitHub repository. Check with:

```bash
git remote -v
git branch -vv
git status -sb
```

If you use GitHub Desktop instead of Terminal, the same stage → commit → push steps apply. Do not automate a push after *every save*: intermediate broken files and work-in-progress changes should not be published automatically.
