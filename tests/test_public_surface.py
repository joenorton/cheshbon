"""Test public API surface - ensure imports work correctly and no side effects.

This test verifies:
- cheshbon.api exposes diff, validate
- Functions work on tiny fixtures
- No import side effects create attribute collisions
- Module imports don't shadow function exports
"""

import pytest
from pathlib import Path

# Test that cheshbon.api is the programmatic entrypoint
def test_api_exports_core_functions():
    """Test that cheshbon.api exports diff, validate."""
    from cheshbon.api import diff, validate
    
    # Verify they are callable functions
    assert callable(diff)
    assert callable(validate)
    
    # Verify they are not modules
    import types
    assert isinstance(diff, types.FunctionType) or hasattr(diff, '__call__')
    assert isinstance(validate, types.FunctionType) or hasattr(validate, '__call__')


def test_api_functions_work_on_fixtures():
    """Test that diff and validate work on tiny fixtures."""
    from cheshbon.api import diff, validate, DiffResult, ValidationResult
    
    HERE = Path(__file__).resolve().parent
    FIXTURES = HERE.parent / "fixtures"
    
    # Test validate on a simple spec
    spec_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    if spec_path.exists():
        result = validate(spec=spec_path)
        assert isinstance(result, ValidationResult)
        assert result.ok is True
    
    # Test diff on simple specs
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    if spec_v1_path.exists() and spec_v2_path.exists():
        result = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
        assert isinstance(result, DiffResult)
        assert hasattr(result, 'validation_failed')
        assert hasattr(result, 'impacted_ids')


def test_no_module_shadowing():
    """Test that importing cheshbon.diff module doesn't shadow cheshbon.api.diff function."""
    # Import the function first
    from cheshbon.api import diff as diff_func
    assert callable(diff_func)
    
    # Now import the module (if it exists)
    try:
        import cheshbon.diff as diff_module
        # Verify the module import didn't affect the function
        from cheshbon.api import diff as diff_func_after
        assert diff_func is diff_func_after
        assert callable(diff_func_after)
        # Module should be a module, not a function
        import types
        assert isinstance(diff_module, types.ModuleType)
    except ImportError:
        # If cheshbon.diff doesn't exist as importable module, that's fine
        pass


def test_root_imports_dont_export_diff():
    """Test that diff is NOT in __all__ (to avoid name conflicts).
    
    Note: Python will add cheshbon.diff to the namespace if imported as a module,
    but it should not be in __all__ and should not be the function.
    """
    import cheshbon
    
    # diff should NOT be in __all__ (not explicitly exported)
    assert 'diff' not in cheshbon.__all__, "diff should not be in __all__ to avoid name conflicts"
    
    # Initially, diff should not be in namespace (unless someone imported cheshbon.diff)
    # But if it is, it should be the module, not the function
    if hasattr(cheshbon, 'diff'):
        import types
        # If diff exists, it might be the module (from import cheshbon.diff)
        # But it should NOT be the function from api
        from cheshbon.api import diff as api_diff_func
        assert cheshbon.diff is not api_diff_func, "cheshbon.diff should not be the function from api"
    
    # validate should be in __all__ and namespace
    assert 'validate' in cheshbon.__all__
    assert hasattr(cheshbon, 'validate')
    
    # And result types should be
    assert hasattr(cheshbon, 'DiffResult')
    assert hasattr(cheshbon, 'ValidationResult')


def test_api_imports_no_side_effects():
    """Test that importing cheshbon.api doesn't create attribute collisions."""
    import cheshbon
    import cheshbon.api
    
    # Verify api module has the functions
    assert hasattr(cheshbon.api, 'diff')
    assert hasattr(cheshbon.api, 'validate')
    
    # Verify they are functions, not modules
    import types
    assert isinstance(cheshbon.api.diff, types.FunctionType) or hasattr(cheshbon.api.diff, '__call__')
    assert isinstance(cheshbon.api.validate, types.FunctionType) or hasattr(cheshbon.api.validate, '__call__')
    
    # Verify importing cheshbon.diff module (if it exists) doesn't affect cheshbon.api.diff
    try:
        import cheshbon.diff
        # Re-import to check
        import importlib
        importlib.reload(cheshbon.api)
        assert hasattr(cheshbon.api, 'diff')
        assert callable(cheshbon.api.diff)
    except ImportError:
        pass
