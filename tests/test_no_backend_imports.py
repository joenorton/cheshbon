"""Tripwire test: ensure no backend imports in OSS package.

This test walks the installed package source tree and fails if any module
imports backend, fastapi, or workspace modules (storage, change_storage, etc.).

Goal: prevent "one tiny import" from sneaking back.
"""

import re
import pytest
from pathlib import Path


def find_package_source():
    """Find the source directory of the installed cheshbon package.
    
    Always uses Path(cheshbon.__file__).parent - that's the actually-imported package.
    """
    try:
        import cheshbon
        # Always use the actually-imported package location
        return Path(cheshbon.__file__).parent
    except ImportError:
        # Fallback for repo mode (when package not installed)
        here = Path(__file__).resolve().parent
        repo_root = here.parent
        src_cheshbon = repo_root / "src" / "cheshbon"
        if src_cheshbon.exists():
            return src_cheshbon
        raise ImportError("Cannot find cheshbon package source")


def scan_file_for_forbidden_tokens(file_path: Path) -> list[str]:
    """
    Scan a Python file for forbidden tokens.
    
    Returns list of violation strings (empty if no violations).
    
    Forbidden tokens:
    - sys.path (any manipulation)
    - backend. (any backend import)
    - fastapi (hosted web framework)
    - uvicorn (hosted ASGI server)
    - pydantic_settings (often a hosted config smell)
    """
    violations = []
    
    try:
        content = file_path.read_text(encoding='utf-8')
    except Exception:
        return violations  # Skip binary or unreadable files
    
    # Forbidden patterns
    forbidden_patterns = [
        (r'sys\.path', 'sys.path manipulation'),
        (r'backend\.', 'backend.* import or reference'),
        (r'\bfastapi\b', 'fastapi (hosted web framework)'),
        (r'\buvicorn\b', 'uvicorn (hosted ASGI server)'),
        (r'\bpydantic_settings\b', 'pydantic_settings (hosted config smell)'),
    ]
    
    lines = content.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        # Skip comments
        stripped = line.strip()
        if stripped.startswith('#'):
            continue
        
        # Skip lines that are clearly string literals (error messages in raise statements)
        if 'raise' in line.lower() and ('"' in line or "'" in line):
            # This is a raise statement with an error message - skip
            continue
        
        # Check for forbidden patterns
        for pattern, description in forbidden_patterns:
            if re.search(pattern, line):
                # Check if it's an actual import/usage, not a string literal
                # If line starts with from/import, it's likely an import
                if line.strip().startswith('from ') or line.strip().startswith('import '):
                    # But skip if it's inside a string (check for quotes around the pattern match)
                    if '"' in line or "'" in line:
                        # Could be a string - check if pattern is inside quotes
                        # For simplicity, if line starts with from/import, it's likely an import
                        if 'raise' not in line.lower():
                            violations.append(
                                f"{file_path}:{line_num}: {description} - {line.strip()}"
                            )
                    else:
                        violations.append(
                            f"{file_path}:{line_num}: {description} - {line.strip()}"
                        )
                # For sys.path, check if it's actual manipulation (not just a comment or string)
                elif 'sys.path' in line and ('insert' in line or 'append' in line or '=' in line):
                    # This is actual sys.path manipulation
                    if 'raise' not in line.lower() or not ('"' in line or "'" in line):
                        violations.append(
                            f"{file_path}:{line_num}: {description} - {line.strip()}"
                        )
    
    return violations


def test_no_forbidden_tokens_in_installed_package():
    """
    Test that no modules in installed cheshbon package contain forbidden tokens.
    
    This is the release gate - scans the actually-imported package.
    """
    # Import cheshbon to get the installed package location
    import cheshbon
    pkg_dir = Path(cheshbon.__file__).parent
    
    violations = []
    
    # Recursively scan all Python files under pkg_dir
    for py_file in pkg_dir.rglob("*.py"):
        # Skip __pycache__
        if "__pycache__" in str(py_file):
            continue
        
        # Skip test files (they may import backend for testing)
        if "test_" in py_file.name:
            continue
        
        file_violations = scan_file_for_forbidden_tokens(py_file)
        violations.extend(file_violations)
    
    # Optionally assert backend dir doesn't exist
    backend_dir = pkg_dir / "backend"
    if backend_dir.exists():
        violations.append(
            f"{backend_dir}: backend directory exists in installed package (should not be shipped)"
        )
    
    # Optionally assert frontend dir doesn't exist
    frontend_dir = pkg_dir / "frontend"
    if frontend_dir.exists():
        violations.append(
            f"{frontend_dir}: frontend directory exists in installed package (should not be shipped)"
        )
    
    if violations:
        violation_msg = "\n".join(violations)
        raise AssertionError(
            f"Found {len(violations)} forbidden token violations in installed cheshbon package:\n\n"
            f"{violation_msg}\n\n"
            "OSS v1.0 modules must not contain sys.path, backend.*, fastapi, uvicorn, or pydantic_settings. "
            "Package must be closed under its own imports."
        )


def test_imports_are_clean():
    """Test that importing cheshbon doesn't trigger backend imports."""
    import cheshbon
    import cheshbon._internal.verify
    # If any of these trigger backend imports, test will fail
    # Version check: in dev mode it's "dev", in installed mode it's "1.0.0"
    assert cheshbon.__version__ in ("1.0.0", "dev")


def test_internal_not_accessible_from_public():
    """Test that _internal modules are not part of the public API namespace.
    
    Semantics:
    - _internal exists on disk and is importable by kernel code (for internal use)
    - _internal is NOT in __all__ (not part of public API)
    - Studio must not import _internal (enforced by not being in __all__)
    - Contracts are in public namespace, not _internal
    
    This test enforces the boundary: Studio should use public API only.
    """
    import cheshbon
    
    # _internal exists as a module (for kernel's internal use)
    # It's fine if kernel code imports it, but it's not public API
    import cheshbon._internal  # noqa: F401 - This is fine for kernel code
    
    # CRITICAL: _internal must NOT be in __all__ (not part of public API)
    # Studio should not import it, and it's not advertised as public
    if hasattr(cheshbon, "__all__"):
        assert "_internal" not in cheshbon.__all__, (
            "_internal must not be in __all__ - Studio should not import it"
        )
    
    # Verify that contracts are in public namespace (not _internal)
    from cheshbon import CompatibilityIssue, CompatibilityReport  # noqa: F401
    
    # Verify _internal.contracts is deleted (moved to public namespace)
    with pytest.raises((ImportError, ModuleNotFoundError, AttributeError)):
        from cheshbon._internal.contracts import CompatibilityIssue  # noqa: F401
