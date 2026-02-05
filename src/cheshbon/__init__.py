"""cheshbon: kernel-grade artifact verification + diff tooling."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("cheshbon")
except PackageNotFoundError:
    __version__ = "dev"

# Public API exports
# Note: diff and validate are exported from cheshbon.api, not from root
# This avoids name conflicts with cheshbon.diff (CLI module)
from cheshbon.api import validate, DiffResult, ValidationResult
from cheshbon.contracts import CompatibilityIssue, CompatibilityReport
from cheshbon.codes import ValidationCode

__all__ = [
    "__version__",
    "validate",
    "DiffResult",
    "ValidationResult",
    "ValidationCode",
    "CompatibilityIssue",
    "CompatibilityReport",
]
