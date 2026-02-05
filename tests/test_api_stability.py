"""Test that stable API modules work independently of root exports.

This test enforces that the stable API (cheshbon.api and cheshbon.contracts)
works correctly without depending on root exports. This ensures we can change
or remove root exports without breaking the stable API contract.
"""

import pytest
from pathlib import Path


def test_stable_api_modules_work_independently():
    """Test that cheshbon.api and cheshbon.contracts work without root exports.
    
    This ensures the stable API is self-contained and doesn't depend on
    convenience aliases in the root package.
    """
    # Import directly from stable modules (not from root)
    from cheshbon.api import diff, validate, DiffResult, ValidationResult
    from cheshbon.contracts import CompatibilityIssue, CompatibilityReport
    
    # Verify all imports work
    assert callable(diff)
    assert callable(validate)
    
    # Verify result types are classes
    assert isinstance(DiffResult, type)
    assert isinstance(ValidationResult, type)
    assert isinstance(CompatibilityIssue, type)
    assert isinstance(CompatibilityReport, type)
    
    # Test that functions work (using fixtures if available)
    HERE = Path(__file__).resolve().parent
    FIXTURES = HERE.parent / "fixtures"
    
    spec_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    if spec_path.exists():
        # Test validate
        result = validate(spec=spec_path)
        assert isinstance(result, ValidationResult)
        
        # Test diff
        spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
        if spec_v2_path.exists():
            diff_result = diff(from_spec=spec_path, to_spec=spec_v2_path)
            assert isinstance(diff_result, DiffResult)
    
    # Test that compatibility models can be instantiated
    issue = CompatibilityIssue(
        object_type="spec",
        path="test.json",
        found_version="0.7",
        required_version="0.7",
        action="accept",
        reason="ok"
    )
    assert issue.object_type == "spec"
    
    report = CompatibilityReport(
        ok=True,
        mode="permissive",
        unknown_fields="preserve",
        issues=[],
        warnings=[]
    )
    assert report.ok is True


def test_stable_api_doesnt_depend_on_root():
    """Test that stable API modules don't require root package to be imported first."""
    # Don't import cheshbon root at all
    # Import directly from stable modules
    from cheshbon.api import diff, DiffResult
    from cheshbon.contracts import CompatibilityIssue
    
    # Verify they work
    assert callable(diff)
    assert isinstance(DiffResult, type)
    assert isinstance(CompatibilityIssue, type)
    
    # Verify we can use them
    HERE = Path(__file__).resolve().parent
    FIXTURES = HERE.parent / "fixtures"
    
    spec_v1_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v1.json"
    spec_v2_path = FIXTURES / "scenario1_rename_no_impact" / "spec_v2.json"
    if spec_v1_path.exists() and spec_v2_path.exists():
        result = diff(from_spec=spec_v1_path, to_spec=spec_v2_path)
        assert isinstance(result, DiffResult)


def test_root_exports_are_convenience_only():
    """Test that root exports exist but are not required for stable API.
    
    This documents that root exports are convenience aliases and may change.
    """
    import cheshbon
    
    # Root exports should exist (for convenience)
    assert hasattr(cheshbon, 'validate')
    assert hasattr(cheshbon, 'DiffResult')
    assert hasattr(cheshbon, 'ValidationResult')
    assert hasattr(cheshbon, 'CompatibilityIssue')
    assert hasattr(cheshbon, 'CompatibilityReport')
    
    # But diff should NOT be in root (to avoid name conflict with cheshbon.diff module)
    assert 'diff' not in cheshbon.__all__
    
    # Verify root exports point to same objects as stable modules
    from cheshbon.api import validate as api_validate
    from cheshbon.api import DiffResult as api_DiffResult
    from cheshbon.contracts import CompatibilityIssue as contracts_CompatibilityIssue
    
    assert cheshbon.validate is api_validate
    assert cheshbon.DiffResult is api_DiffResult
    assert cheshbon.CompatibilityIssue is contracts_CompatibilityIssue
    
    # But the key point: stable API works without root
    # (tested in test_stable_api_doesnt_depend_on_root)
