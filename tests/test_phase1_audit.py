"""Pure offline checks for the Phase 1 evidence audit."""
from pathlib import Path
import pytest
from scripts.phase1_audit import AuditError, expected_batches, raw_file_info, require


def test_input_line_count_includes_unterminated_last_record(tmp_path: Path):
    raw = tmp_path / "raw.log"
    raw.write_bytes(b"alpha\nbeta")
    got = raw_file_info(raw, block_size=2)
    assert got["records"] == 2
    assert got["size_bytes"] == 10


def test_newline_terminated_file_not_double_counted(tmp_path: Path):
    raw = tmp_path / "raw.log"
    raw.write_bytes(b"alpha\nbeta\n")
    assert raw_file_info(raw, block_size=2)["records"] == 2


def test_batches_reset_at_source_file_boundary():
    inputs = [{"name": "July", "records": 5}, {"name": "August", "records": 4}]
    assert expected_batches(inputs, 3) == [
        ("July", 3), ("July", 2), ("August", 3), ("August", 1)
    ]


def test_failed_invariant_raises_in_optimized_python_too():
    with pytest.raises(AuditError):
        require(False, "bad")
