#!/usr/bin/env bash
# Validate, safely stage, commit and push the project in one explicitly invoked command.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -ne 1 || -z "${1// /}" ]]; then
    echo 'Usage: ./scripts/verify_and_publish.sh "Meaningful change description"' >&2
    exit 2
fi

if [[ "$(git branch --show-current)" != "main" ]]; then
    echo 'Stopped: checkout main, or use the feature-branch review workflow.' >&2
    exit 2
fi

if ! git remote get-url origin >/dev/null 2>&1; then
    echo 'Stopped: Git remote origin is not configured.' >&2
    exit 2
fi

if ! git diff --quiet -- .gitignore; then
    echo 'Note: .gitignore is modified; review it carefully before publishing.' >&2
fi

printf '== Checking remote history ==\n'
git fetch origin main
if ! git merge-base --is-ancestor origin/main HEAD; then
    echo 'Stopped: remote main has changes not in local HEAD. Review and pull before publishing.' >&2
    exit 2
fi

printf '== Running Python regression tests ==\n'
if [[ -x .venv/bin/python ]]; then
    .venv/bin/python -m pytest -q
else
    python3 -m pytest -q
fi

printf '== Staging only source code, tests and docs ==\n'
git add -u
git add README.md requirements.txt compose.yaml Dockerfile.hadoop
# Stage Git's tracked configuration and project sources, but never data/work contents.
git add .gitignore .dockerignore .env.example
git add app common pipelines sql scripts tests docs .github

git diff --cached --check

printf '== Checking staged file safety ==\n'
python3 - <<'PY'
from pathlib import Path
import subprocess

staged = subprocess.check_output(
    ['git', 'diff', '--cached', '--name-only', '-z'],
).split(b'\0')
for raw in staged:
    if not raw:
        continue
    name = raw.decode('utf-8', errors='replace')
    path = Path(name)
    safe_placeholder = name in {'data/.gitkeep', 'work/.gitkeep'}
    if (name.startswith(('data/', 'work/', '.venv/')) and not safe_placeholder):
        raise SystemExit(f'Refusing staged generated file: {name}')
    if path.name in {'.env', 'id_rsa', 'id_ed25519'}:
        raise SystemExit(f'Refusing staged credentials: {name}')
    if path.suffix.lower() in {'.zip', '.gz', '.pyc', '.pem', '.p12', '.key'}:
        raise SystemExit(f'Refusing staged archive/key/cache: {name}')
    if path.is_file() and path.stat().st_size > 5 * 1024 * 1024:
        raise SystemExit(f'Refusing file over 5 MB: {name}')
print('PASS: no raw data, generated outputs, keys or large files staged')
PY

if git diff --cached --quiet; then
    echo 'Nothing new to commit. Checking that local and remote branches match.'
else
    printf '== Committing reviewed files ==\n'
    git diff --cached --stat
    git commit -m "$1"
fi

printf '== Publishing main to GitHub ==\n'
git push origin main

git fetch origin main >/dev/null
local_commit="$(git rev-parse HEAD)"
remote_commit="$(git rev-parse origin/main)"
if [[ "$local_commit" != "$remote_commit" ]]; then
    echo 'Push verification failed: local and origin/main differ.' >&2
    exit 1
fi

echo "PASS: GitHub and local main synchronized at ${local_commit:0:12}"
git status -sb
