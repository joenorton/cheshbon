"""Centralized canonical JSON serialization.

This module provides a single function for byte-stable JSON serialization
used everywhere: spec writes, change writes, pointers writes, content_hash computation, test snapshots.

Critical: This prevents non-equal hashes between platforms and ensures byte-stable evidence.
"""

import json
from typing import Any


def canonical_dumps(obj: Any) -> str:
    """
    Canonical JSON serialization for byte-stable evidence.
    
    Rules:
    - UTF-8 encoding
    - Sorted keys
    - Stable separators (",", ":")
    - Deterministic list ordering (lists must already be sorted before calling)
    - No trailing whitespace
    
    Args:
        obj: Python object to serialize
        
    Returns:
        Canonical JSON string (UTF-8 encoded)
    """
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False  # UTF-8 encoding
    )
