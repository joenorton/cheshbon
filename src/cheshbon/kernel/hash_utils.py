"""Hash utilities with explicit canonicalization rules for stable hashing.

This module provides canonicalization and hashing functions that guarantee
stable, deterministic output across different Python versions and environments.

Key rules:
- Object keys sorted recursively
- Arrays preserve order (sets sorted with stable comparator)
- Floats BANNED (hard validation error)
- Strings normalized to NFC
- Non-JSON types forbidden
"""

import json
import hashlib
import unicodedata
from typing import Any, Dict, List, Union
from collections.abc import Mapping, Sequence
from pathlib import Path


class CanonicalizationError(ValueError):
    """Raised when an object cannot be canonicalized."""
    pass


def _get_type_tag(obj: Any) -> str:
    """Get a stable type tag for ordering mixed-type collections."""
    if obj is None:
        return "0_null"
    elif isinstance(obj, bool):
        return f"1_bool_{obj}"
    elif isinstance(obj, int):
        return f"2_int_{obj}"
    elif isinstance(obj, str):
        return f"3_str_{obj}"
    elif isinstance(obj, dict):
        return "4_dict"
    elif isinstance(obj, list):
        return "5_list"
    else:
        raise CanonicalizationError(
            f"Unsupported type for canonicalization: {type(obj).__name__}"
        )


def _normalize_string(s: str) -> str:
    """Normalize string to NFC (Unicode Normalization Form Canonical Composition)."""
    return unicodedata.normalize('NFC', s)


def _validate_json_type(obj: Any, path: str = "") -> None:
    """Validate that object contains only JSON-compatible types.
    
    Raises CanonicalizationError if non-JSON types are found.
    
    Note: None vs missing keys - we treat None explicitly. Missing keys in dicts
    are not represented (they simply don't exist). This is a design decision:
    - None is a valid JSON value
    - Missing keys are not represented in JSON (they're just absent)
    - We do NOT treat missing keys as equivalent to None
    """
    if obj is None:
        return  # None is explicitly allowed (represents null in JSON)
    elif isinstance(obj, bool):
        return
    elif isinstance(obj, int):
        # Check for NaN/Inf (shouldn't happen with int, but be safe)
        if obj != obj or obj == float('inf') or obj == float('-inf'):
            raise CanonicalizationError(
                f"Invalid number at {path}: NaN or Inf not allowed"
            )
    elif isinstance(obj, float):
        # BAN FLOATS - hard validation error
        raise CanonicalizationError(
            f"Floats are not allowed in params (at {path}). Use strings for decimals instead."
        )
    elif isinstance(obj, str):
        return
    elif isinstance(obj, dict):
        for key, value in obj.items():
            if not isinstance(key, str):
                raise CanonicalizationError(
                    f"Dictionary keys must be strings at {path}, got {type(key).__name__}"
                )
            _validate_json_type(value, f"{path}.{key}" if path else key)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            _validate_json_type(item, f"{path}[{i}]" if path else f"[{i}]")
    else:
        raise CanonicalizationError(
            f"Non-JSON type at {path}: {type(obj).__name__}. "
            f"Only None, bool, int, str, dict, and list are allowed."
        )


def _canonicalize_value(obj: Any, is_set: bool = False) -> Any:
    """Canonicalize a single value.
    
    Args:
        obj: The value to canonicalize
        is_set: If True, treat as set element (for stable sorting)
    
    Returns:
        Canonicalized value
    """
    if obj is None:
        return None
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, int):
        return obj
    elif isinstance(obj, str):
        return _normalize_string(obj)
    elif isinstance(obj, dict):
        # Sort keys recursively
        return {
            _normalize_string(k): _canonicalize_value(v, is_set=False)
            for k, v in sorted(obj.items())
        }
    elif isinstance(obj, list):
        if is_set:
            # For sets, sort by stable comparator: (type_tag, value) ordering
            # This ensures deterministic ordering across mixed types
            canonicalized_items = [_canonicalize_value(item, is_set=False) for item in obj]
            # Sort by (type_tag, value) tuple for stable ordering
            sorted_items = sorted(
                canonicalized_items,
                key=lambda x: (_get_type_tag_for_sorting(x), _get_sort_key(x))
            )
            return sorted_items
        else:
            # Arrays preserve order
            return [_canonicalize_value(item, is_set=False) for item in obj]
    else:
        raise CanonicalizationError(
            f"Unsupported type: {type(obj).__name__}"
        )


def _get_type_tag_for_sorting(obj: Any) -> str:
    """Get type tag for stable sorting in sets.
    
    Returns type tag that ensures consistent ordering:
    - null < bool < int < str < dict < list
    """
    if obj is None:
        return "0_null"
    elif isinstance(obj, bool):
        return "1_bool"
    elif isinstance(obj, int):
        return "2_int"
    elif isinstance(obj, str):
        return "3_str"
    elif isinstance(obj, dict):
        return "4_dict"
    elif isinstance(obj, list):
        return "5_list"
    else:
        raise CanonicalizationError(f"Unsupported type: {type(obj).__name__}")


def _get_sort_key(obj: Any) -> Any:
    """Get sort key for value within same type.
    
    For use with type_tag to create stable (type_tag, value) ordering.
    """
    if obj is None:
        return None
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, int):
        return obj
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, dict):
        # For dicts, use canonical JSON string as sort key
        return json.dumps(obj, sort_keys=True, ensure_ascii=False)
    elif isinstance(obj, list):
        # For lists, use canonical JSON string as sort key
        return json.dumps(obj, sort_keys=True, ensure_ascii=False)
    else:
        raise CanonicalizationError(f"Unsupported type: {type(obj).__name__}")


def canonicalize_json(obj: Any, array_as_set: bool = False) -> str:
    """Canonicalize a JSON-serializable object to a stable string representation.
    
    Rules:
    - Object keys sorted recursively (all nested objects)
    - Arrays preserve order (unless array_as_set=True, then sorted with stable comparator)
    - Numbers: int allowed, floats BANNED (hard error)
    - Strings: Unicode normalization (NFC)
    - Types: Only JSON types allowed (None, bool, int, str, dict, list)
    
    Args:
        obj: The object to canonicalize
        array_as_set: If True, treat top-level array as a set and sort it
    
    Returns:
        Canonical JSON string
    
    Raises:
        CanonicalizationError: If object contains floats, non-JSON types, or other invalid values
    """
    # First validate types
    _validate_json_type(obj)
    
    # Then canonicalize
    if isinstance(obj, list) and array_as_set:
        canonicalized = _canonicalize_value(obj, is_set=True)
    else:
        canonicalized = _canonicalize_value(obj, is_set=False)
    
    # Serialize to JSON with sorted keys
    return json.dumps(canonicalized, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def hash_params(params: dict) -> str:
    """Compute SHA256 hash of canonicalized params.
    
    Args:
        params: Dictionary of transform parameters
    
    Returns:
        SHA256 hash as hex string (prefixed with "sha256:")
    
    Raises:
        CanonicalizationError: If params contain floats or non-JSON types
    """
    if params is None:
        params = {}
    
    canonical_str = canonicalize_json(params)
    digest = hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()
    return f"sha256:{digest}"


def hash_impl(content: Union[str, bytes]) -> str:
    """Compute SHA256 hash of implementation content.
    
    Args:
        content: Implementation content as string or bytes
    
    Returns:
        SHA256 hash as hex string (prefixed with "sha256:")
    """
    if isinstance(content, str):
        content_bytes = content.encode('utf-8')
    else:
        content_bytes = content
    
    digest = hashlib.sha256(content_bytes).hexdigest()
    return f"sha256:{digest}"


def hash_schema(schema: dict) -> str:
    """Compute SHA256 hash of JSON schema.
    
    Args:
        schema: JSON schema dictionary
    
    Returns:
        SHA256 hash as hex string (prefixed with "sha256:")
    
    Raises:
        CanonicalizationError: If schema contains floats or non-JSON types
    """
    canonical_str = canonicalize_json(schema)
    digest = hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()
    return f"sha256:{digest}"


def compute_canonical_json_sha256(path: Union[str, Path]) -> str:
    """Compute SHA256 of canonicalized JSON file contents.

    Canonicalization rules:
    - sort_keys=True
    - separators=(",", ":")
    - ensure_ascii=False
    """
    from pathlib import Path
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    canonical = json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
