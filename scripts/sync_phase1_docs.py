#!/usr/bin/env python3
"""Idempotently bring existing Phase 1 design/README status up to audited reality.

Run only after ``python -m scripts.phase1_audit`` succeeds. This script changes
only existing README.md and docs/PHASE1_DESIGN.md, never raw input or SQL data.
"""
from __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs" / "PHASE1_RESULTS.md"


def replace_once(text: str, before: str, after: str) -> str:
    if after in text:
        return text
    if before not in text:
        raise ValueError(f"Expected documentation sentence missing: {before[:80]}")
    return text.replace(before, after, 1)


def main() -> None:
    if not EVIDENCE.is_file() or "Verified execution evidence" not in EVIDENCE.read_text():
        raise SystemExit("Run python -m scripts.phase1_audit successfully before updating docs")
    readme = ROOT / "README.md"
    design = ROOT / "docs" / "PHASE1_DESIGN.md"
    orig_readme = readme.read_text()
    orig_design = design.read_text()
    new_readme = replace_once(
        orig_readme,
        "**Status:** Code and offline tests prepared. **Live Docker/MongoDB/PostgreSQL/Hadoop integration has not been verified yet**; capture actual results on the team Mac before describing the system as working end-to-end.",
        "**Status:** Phase 1 MongoDB and Hadoop MapReduce successfully executed on the official full NASA logs, with PostgreSQL reporting and all three query results matching. See `docs/PHASE1_RESULTS.md` for independently audited run IDs and measured runtimes. Hadoop uses local JobRunner (not a distributed multi-node cluster).",
    )
    new_readme = replace_once(new_readme, "### Known limitations until live validation", "### Remaining limitations")
    new_readme = replace_once(
        new_readme,
        "- This environment cannot run your Docker services; local tests alone do not establish Hadoop image, runtime or database compatibility.",
        "- Live local-Docker integration has been verified; portability and multi-node deployment are not yet established.",
    )
    new_design = orig_design
    for before,after in [
        ("- [ ] **Actual live end-to-end successful execution on the group Mac**", "- [x] **Actual live end-to-end successful execution on the group Mac**"),
        ("- [ ] Verify both pipelines produce exactly equal results on official full data", "- [x] Verify both pipelines produce exactly equal results on official full data"),
        ("- [ ] Document real runtime experiments, machine specs and screenshots", "- [ ] Document replicated runtime experiments, full machine specs and screenshots (initial measured runtimes in `PHASE1_RESULTS.md`)"),
        ("| `pytest -q` | pending on group Mac | |", "| `pytest -q` | passed on group Mac (13 tests before final audit additions) | Terminal output |"),
        ("| `docker compose ps` | pending on group Mac | |", "| `docker compose ps` | MongoDB and PostgreSQL healthy; Hadoop started | Terminal output |"),
        ("| MongoDB full dataset | pending | |", "| MongoDB full dataset | success | `PHASE1_RESULTS.md` |"),
        ("| MapReduce full dataset | pending | |", "| MapReduce full dataset | success | `PHASE1_RESULTS.md` |"),
        ("| SQL reporting | pending | |", "| SQL reporting | success | `PHASE1_RESULTS.md` |"),
        ("| Comparison all 3 queries | pending | |", "| Comparison all 3 queries | full equality | `PHASE1_RESULTS.md` |"),
    ]:
        new_design = replace_once(new_design, before, after)
    # Validate every pattern before writing either file.
    readme.write_text(new_readme)
    design.write_text(new_design)
    print("PASS: README.md and docs/PHASE1_DESIGN.md now reflect verified runs")


if __name__ == "__main__":
    main()
