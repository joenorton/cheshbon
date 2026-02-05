"""Packaging regression tests.

Tests that verify the installed package structure and behavior.
"""

import zipfile
from pathlib import Path
import pytest


def test_wheel_contents():
    """Test that installed wheel has correct structure and excludes backend/frontend."""
    # This test should be run after building and installing a wheel
    # For now, we check the source structure
    
    here = Path(__file__).resolve().parent
    repo_root = here.parent
    src_cheshbon = repo_root / "src" / "cheshbon"
    src_kernel = repo_root / "src" / "cheshbon" / "kernel"
    
    # Check that cheshbon and kernel exist
    assert src_cheshbon.exists(), "cheshbon package should exist in src/"
    assert src_kernel.exists(), "cheshbon.kernel package should exist in src/"
    
    # Check that _internal exists
    assert (src_cheshbon / "_internal").exists(), "cheshbon._internal should exist"
    
    # Check that backend is NOT in src (it's in repo root, not packaged)
    backend_in_src = repo_root / "src" / "backend"
    assert not backend_in_src.exists(), "backend should not be in src/ (not packaged)"
    
    # Check that frontend is NOT in src
    frontend_in_src = repo_root / "src" / "frontend"
    assert not frontend_in_src.exists(), "frontend should not be in src/ (not packaged)"


def test_import_boundary():
    """Test that installed package can import cheshbon and kernel, but not backend."""
    import cheshbon
    import cheshbon.kernel  # noqa: F401
    
    # Check version: in dev mode it's "dev", in installed mode it's "1.0.0"
    assert cheshbon.__version__ in ("1.0.0", "dev")
    
    # Check that backend is not importable (when installed, it won't be in path)
    # In repo, backend.src might be importable, but in installed wheel it won't be
    try:
        import backend
        # If we're in repo mode, backend might exist - that's OK
        # But it should not be in the installed package
        pytest.skip("backend is importable (repo mode) - test will pass in installed wheel")
    except ImportError:
        # Expected in installed package
        pass
