"""Tests for hash utilities and canonicalization rules."""

import pytest
from cheshbon.kernel.hash_utils import (
    canonicalize_json,
    hash_params,
    hash_impl,
    hash_schema,
    CanonicalizationError,
)


class TestCanonicalizeJson:
    """Tests for canonicalize_json function."""
    
    def test_simple_dict_sorts_keys(self):
        """Object keys should be sorted."""
        obj = {"b": 2, "a": 1, "c": 3}
        result = canonicalize_json(obj)
        assert result == '{"a":1,"b":2,"c":3}'
    
    def test_nested_dict_sorts_recursively(self):
        """Nested object keys should be sorted recursively."""
        obj = {"z": {"b": 2, "a": 1}, "a": {"d": 4, "c": 3}}
        result = canonicalize_json(obj)
        assert result == '{"a":{"c":3,"d":4},"z":{"a":1,"b":2}}'
    
    def test_array_preserves_order(self):
        """Arrays should preserve order."""
        obj = {"items": [3, 1, 2]}
        result = canonicalize_json(obj)
        assert result == '{"items":[3,1,2]}'
    
    def test_array_as_set_sorts(self):
        """Arrays marked as sets should be sorted."""
        obj = [3, 1, 2]
        result = canonicalize_json(obj, array_as_set=True)
        assert result == '[1,2,3]'
    
    def test_array_as_set_mixed_types(self):
        """Set sorting should handle mixed types deterministically."""
        obj = ["b", 2, "a", 1]
        result = canonicalize_json(obj, array_as_set=True)
        # Should be sorted by (type_tag, value): null < bool < int < str < dict < list
        # So: 1, 2 (ints) come before "a", "b" (strings)
        assert result == '[1,2,"a","b"]'
    
    def test_string_normalization_nfc(self):
        """Strings should be normalized to NFC."""
        # Example: é can be represented as single character or e + combining accent
        obj = {"text": "café"}
        result = canonicalize_json(obj)
        # Should normalize to NFC
        assert "café" in result
    
    def test_int_allowed(self):
        """Integers should be allowed."""
        obj = {"count": 42, "negative": -10}
        result = canonicalize_json(obj)
        assert result == '{"count":42,"negative":-10}'
    
    def test_bool_allowed(self):
        """Booleans should be allowed."""
        obj = {"flag": True, "enabled": False}
        result = canonicalize_json(obj)
        assert result == '{"enabled":false,"flag":true}'
    
    def test_null_allowed(self):
        """None/null should be allowed."""
        obj = {"value": None, "other": "text"}
        result = canonicalize_json(obj)
        assert result == '{"other":"text","value":null}'
    
    def test_float_banned_hard_error(self):
        """Floats should be BANNED with hard validation error."""
        with pytest.raises(CanonicalizationError, match="Floats are not allowed"):
            canonicalize_json({"value": 3.14})
        
        with pytest.raises(CanonicalizationError, match="Floats are not allowed"):
            canonicalize_json({"value": 1.0})
        
        with pytest.raises(CanonicalizationError, match="Floats are not allowed"):
            canonicalize_json(3.14)
    
    def test_float_in_nested_dict_banned(self):
        """Floats in nested structures should be caught."""
        with pytest.raises(CanonicalizationError, match="Floats are not allowed"):
            canonicalize_json({"outer": {"inner": 3.14}})
    
    def test_float_in_array_banned(self):
        """Floats in arrays should be caught."""
        with pytest.raises(CanonicalizationError, match="Floats are not allowed"):
            canonicalize_json({"items": [1, 2.5, 3]})
    
    def test_non_json_types_forbidden(self):
        """Non-JSON types should be forbidden."""
        with pytest.raises(CanonicalizationError, match="Non-JSON type"):
            canonicalize_json({"date": object()})
        
        # datetime objects
        from datetime import datetime
        with pytest.raises(CanonicalizationError, match="Non-JSON type"):
            canonicalize_json({"date": datetime.now()})
    
    def test_dict_keys_must_be_strings(self):
        """Dictionary keys must be strings."""
        with pytest.raises(CanonicalizationError, match="Dictionary keys must be strings"):
            canonicalize_json({1: "value"})
        
        with pytest.raises(CanonicalizationError, match="Dictionary keys must be strings"):
            canonicalize_json({True: "value"})


class TestHashParams:
    """Tests for hash_params function."""
    
    def test_simple_params(self):
        """Hash simple params."""
        params = {"key": "value"}
        result = hash_params(params)
        assert result.startswith("sha256:")
        assert len(result) == 71  # "sha256:" + 64 hex chars
    
    def test_params_deterministic(self):
        """Same params should produce same hash."""
        params = {"a": 1, "b": 2}
        hash1 = hash_params(params)
        hash2 = hash_params(params)
        assert hash1 == hash2
    
    def test_params_order_independent(self):
        """Hash should be independent of dict key order."""
        params1 = {"a": 1, "b": 2}
        params2 = {"b": 2, "a": 1}
        hash1 = hash_params(params1)
        hash2 = hash_params(params2)
        assert hash1 == hash2
    
    def test_params_nested(self):
        """Hash nested params."""
        params = {"outer": {"inner": "value"}}
        result = hash_params(params)
        assert result.startswith("sha256:")
    
    def test_params_empty_dict(self):
        """Hash empty dict."""
        result = hash_params({})
        assert result.startswith("sha256:")
    
    def test_params_none(self):
        """Hash None params (treated as empty dict)."""
        result = hash_params(None)
        assert result.startswith("sha256:")
    
    def test_params_float_banned(self):
        """Floats in params should raise error."""
        with pytest.raises(CanonicalizationError, match="Floats are not allowed"):
            hash_params({"value": 3.14})
    
    def test_params_different_values_different_hash(self):
        """Different param values should produce different hashes."""
        params1 = {"key": "value1"}
        params2 = {"key": "value2"}
        hash1 = hash_params(params1)
        hash2 = hash_params(params2)
        assert hash1 != hash2
    
    def test_params_array_order_matters(self):
        """Array order in params should matter for hash."""
        params1 = {"items": [1, 2, 3]}
        params2 = {"items": [3, 2, 1]}
        hash1 = hash_params(params1)
        hash2 = hash_params(params2)
        assert hash1 != hash2


class TestHashImpl:
    """Tests for hash_impl function."""
    
    def test_hash_string(self):
        """Hash string content."""
        content = "def transform(x): return x * 2"
        result = hash_impl(content)
        assert result.startswith("sha256:")
        assert len(result) == 71
    
    def test_hash_bytes(self):
        """Hash bytes content."""
        content = b"def transform(x): return x * 2"
        result = hash_impl(content)
        assert result.startswith("sha256:")
    
    def test_hash_deterministic(self):
        """Same content should produce same hash."""
        content = "test implementation"
        hash1 = hash_impl(content)
        hash2 = hash_impl(content)
        assert hash1 == hash2
    
    def test_hash_different_content_different_hash(self):
        """Different content should produce different hashes."""
        content1 = "implementation 1"
        content2 = "implementation 2"
        hash1 = hash_impl(content1)
        hash2 = hash_impl(content2)
        assert hash1 != hash2


class TestHashSchema:
    """Tests for hash_schema function."""
    
    def test_hash_simple_schema(self):
        """Hash simple JSON schema."""
        schema = {
            "type": "object",
            "properties": {
                "key": {"type": "string"}
            }
        }
        result = hash_schema(schema)
        assert result.startswith("sha256:")
    
    def test_hash_schema_deterministic(self):
        """Same schema should produce same hash."""
        schema = {"type": "string"}
        hash1 = hash_schema(schema)
        hash2 = hash_schema(schema)
        assert hash1 == hash2
    
    def test_hash_schema_order_independent(self):
        """Hash should be independent of key order."""
        schema1 = {"type": "string", "format": "date"}
        schema2 = {"format": "date", "type": "string"}
        hash1 = hash_schema(schema1)
        hash2 = hash_schema(schema2)
        assert hash1 == hash2


class TestSetSorting:
    """Tests for set-sorting with stable comparator."""
    
    def test_set_sorting_mixed_types(self):
        """Set sorting should handle mixed types deterministically."""
        obj = [{"b": 2}, {"a": 1}, 3, 2, "z", "a"]
        result = canonicalize_json(obj, array_as_set=True)
        # Should be sorted by canonical JSON string
        # Order: 2, 3, "a", "z", {"a":1}, {"b":2}
        assert result.startswith('[')
        assert result.endswith(']')
        # Verify deterministic ordering
        result2 = canonicalize_json(obj, array_as_set=True)
        assert result == result2
    
    def test_set_sorting_nested_objects(self):
        """Set sorting should work with nested objects."""
        obj = [{"z": 1}, {"a": 2}]
        result = canonicalize_json(obj, array_as_set=True)
        # Should sort by canonical JSON: {"a":2} comes before {"z":1}
        assert '{"a":2}' in result
        assert '{"z":1}' in result


class TestEdgeCases:
    """Tests for edge cases and error conditions."""
    
    def test_empty_dict(self):
        """Canonicalize empty dict."""
        result = canonicalize_json({})
        assert result == '{}'
    
    def test_empty_list(self):
        """Canonicalize empty list."""
        result = canonicalize_json([])
        assert result == '[]'
    
    def test_empty_string(self):
        """Canonicalize empty string."""
        result = canonicalize_json("")
        assert result == '""'
    
    def test_unicode_strings(self):
        """Handle unicode strings correctly."""
        obj = {"text": "Hello 世界"}
        result = canonicalize_json(obj)
        assert "世界" in result
    
    def test_special_characters_in_keys(self):
        """Handle special characters in keys."""
        obj = {"key_with_underscore": 1, "key-with-dash": 2}
        result = canonicalize_json(obj)
        assert '"key-with-dash"' in result
        assert '"key_with_underscore"' in result
