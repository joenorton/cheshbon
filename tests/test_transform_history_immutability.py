"""Tests for transform history immutability enforcement."""

import pytest
from cheshbon.kernel.transform_registry import TransformEntry, TransformHistory, ImplFingerprint, Signature


def test_transform_history_is_frozen():
    """Test that TransformHistory entries are frozen (cannot be mutated)."""
    history = TransformHistory(
        timestamp="2024-01-01T00:00:00Z",
        impl_fingerprint=ImplFingerprint(
            algo="sha256",
            source="builtin",
            ref="test",
            digest="a" * 64
        ),
        params_schema_hash=None
    )
    
    # Should be frozen - mutation should raise error
    with pytest.raises(Exception):  # Pydantic raises ValidationError or similar
        history.timestamp = "bad"


def test_history_is_tuple():
    """Test that history field is a tuple (immutable container)."""
    entry = TransformEntry(
        id="t:test",
        version="1.0.0",
        kind="builtin",
        signature=Signature(inputs=["string"], output="string"),
        impl_fingerprint=ImplFingerprint(
            algo="sha256",
            source="builtin",
            ref="test",
            digest="a" * 64
        ),
        history=()  # Empty tuple
    )
    
    assert isinstance(entry.history, tuple)
    
    # Tuple is immutable - cannot append
    with pytest.raises(AttributeError):
        entry.history.append(None)


def test_add_history_entry_returns_new_entry():
    """Test that add_history_entry() returns a new TransformEntry (persistent data style)."""
    entry = TransformEntry(
        id="t:test",
        version="1.0.0",
        kind="builtin",
        signature=Signature(inputs=["string"], output="string"),
        impl_fingerprint=ImplFingerprint(
            algo="sha256",
            source="builtin",
            ref="test",
            digest="a" * 64
        ),
        history=()
    )
    
    original_history_len = len(entry.history)
    
    # add_history_entry returns new entry
    new_entry = entry.add_history_entry(timestamp="2024-01-02T00:00:00Z", change_reason="test")
    
    # Original entry unchanged
    assert len(entry.history) == original_history_len
    
    # New entry has appended history
    assert len(new_entry.history) == original_history_len + 1
    assert new_entry.history[-1].change_reason == "test"
    
    # They are different objects
    assert entry is not new_entry
