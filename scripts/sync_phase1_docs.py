#!/usr/bin/env python3
"""One-time status migration for old Phase 1 docs; safe with newer reader docs.

The detailed reader-facing README and architecture document are maintained by
hand. This script will not overwrite or truncate those documents. It only
migrates known pre-live wording, and requires a successful evidence audit.
"""
from __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / 'docs' / 'PHASE1_RESULTS.md'


def replace_once(text: str, before: str, after: str) -> str:
    """Keep strict replacement behavior for the existing regression tests."""
    if after in text:
        return text
    if before not in text:
        raise ValueError(f'Expected documentation sentence missing: {before[:80]}')
    return text.replace(before, after, 1)


def main() -> None:
    if not EVIDENCE.is_file() or 'Verified execution evidence' not in EVIDENCE.read_text():
        raise SystemExit('Run python -m scripts.phase1_audit successfully first')
    readme = ROOT / 'README.md'
    design = ROOT / 'docs' / 'PHASE1_DESIGN.md'
    original_readme = readme.read_text()
    original_design = design.read_text()

    old_status = ('**Status:** Code and offline tests prepared. **Live Docker/MongoDB/'
                  'PostgreSQL/Hadoop integration has not been verified yet**; capture actual '
                  'results on the team Mac before describing the system as working end-to-end.')
    new_status = ('**Status:** Phase 1 MongoDB and Hadoop MapReduce successfully executed '
                  'on the official full NASA logs, with PostgreSQL reporting and all three '
                  'query results matching. See `docs/PHASE1_RESULTS.md` for independently '
                  'audited run IDs and measured runtimes. Hadoop uses local JobRunner '
                  '(not a distributed multi-node cluster).')

    if old_status in original_readme:
        updated_readme = original_readme.replace(old_status, new_status, 1)
    elif ('Two execution backends implemented and verified' in original_readme or
          new_status in original_readme):
        updated_readme = original_readme
    else:
        raise ValueError('README status wording is unknown; review manually, no overwrite')

    replacements = (
        ('- [ ] **Actual live end-to-end successful execution on the group Mac**',
         '- [x] **Actual live end-to-end successful execution on the group Mac**'),
        ('- [ ] Verify both pipelines produce exactly equal results on official full data',
         '- [x] Verify both pipelines produce exactly equal results on official full data'),
    )
    updated_design = original_design
    for before, after in replacements:
        if before in updated_design:
            updated_design = updated_design.replace(before, after, 1)

    if ('MongoDB and Hadoop MapReduce' not in updated_design or
        'PHASE1_RESULTS.md' not in updated_design):
        raise ValueError('Design document lacks verified-run context; review manually')

    # Validate all changes before writing either file.
    if updated_readme != original_readme:
        readme.write_text(updated_readme)
    if updated_design != original_design:
        design.write_text(updated_design)
    print('PASS: reader-oriented Phase 1 docs preserved; verified status present')


if __name__ == '__main__':
    main()
