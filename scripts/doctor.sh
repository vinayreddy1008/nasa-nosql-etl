#!/usr/bin/env bash
set -eu
printf 'Python: '; python3 --version
printf 'Docker: '; if command -v docker >/dev/null; then docker --version; else echo 'NOT INSTALLED'; fi
printf 'Docker Compose: '; if command -v docker >/dev/null; then docker compose version || true; else echo 'NOT INSTALLED'; fi
printf 'CPU: '; uname -m
printf 'Official files: '; for f in data/NASA_access_log_Jul95 data/NASA_access_log_Aug95; do if [ -s "$f" ]; then echo "OK $f"; else echo "MISSING $f"; fi; done
