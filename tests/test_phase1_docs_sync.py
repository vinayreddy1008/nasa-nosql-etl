import pytest
from scripts.sync_phase1_docs import replace_once


def test_idempotent_replacement():
    assert replace_once('old', 'old', 'new') == 'new'
    assert replace_once('new', 'old', 'new') == 'new'


def test_unknown_version_not_changed():
    with pytest.raises(ValueError, match='Expected'):
        replace_once('custom content', 'old', 'new')
