"""Pytest configuration for tests.

No sys.path hacks - tests should import from installed cheshbon package.
"""

import os
import pytest
from pathlib import Path

# Tests should import from installed package, not backend paths
# If backend.src modules are needed for test setup, import them explicitly
# but they are not part of the OSS package

def pytest_addoption(parser):
    """Add gated perf test option."""
    parser.addoption(
        "--run-perf",
        action="store_true",
        default=False,
        help="Run performance sentinel benchmarks (gated)."
    )


def pytest_collection_modifyitems(config, items):
    """Skip perf-marked tests unless --run-perf is set."""
    if config.getoption("--run-perf"):
        return
    skip_perf = pytest.mark.skip(reason="perf tests gated; pass --run-perf")
    for item in items:
        if "perf" in item.keywords:
            item.add_marker(skip_perf)

def pytest_sessionfinish(session, exitstatus):
    """Best-effort cleanup for basetemp on Windows without patching pytest internals."""
    if os.name != "nt":
        return
    basetemp = getattr(session.config.option, "basetemp", None)
    if not basetemp:
        return
    basetemp_path = Path(basetemp)
    if not basetemp_path.exists():
        return
    try:
        import shutil
        shutil.rmtree(basetemp_path)
    except (PermissionError, OSError):
        # If cleanup fails, let it surface as a warning rather than masking errors.
        import warnings
        warnings.warn(f"Could not remove basetemp: {basetemp_path}")




