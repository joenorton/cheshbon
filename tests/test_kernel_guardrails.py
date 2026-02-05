"""Guardrails to keep kernel free of side effects and OS-specific dependencies."""

import re
from pathlib import Path


FORBIDDEN_PATTERNS = {
    "argparse": re.compile(r"\bargparse\b"),
    "pathlib.Path": re.compile(r"\bpathlib\.Path\b"),
    "open(": re.compile(r"(?<![A-Za-z0-9_])open\s*\("),
    "print(": re.compile(r"(?<![A-Za-z0-9_])print\s*\("),
    "warnings.": re.compile(r"\bwarnings\."),
    "datetime.now": re.compile(r"\bdatetime\.now\b"),
    "time.time": re.compile(r"\btime\.time\b"),
    "os.path": re.compile(r"\bos\.path\b"),
}


def test_kernel_has_no_forbidden_tokens():
    kernel_dir = Path(__file__).resolve().parents[1] / "src" / "cheshbon" / "kernel"
    offenders = []

    for path in kernel_dir.glob("*.py"):
        contents = path.read_text(encoding="utf-8")
        for token, pattern in FORBIDDEN_PATTERNS.items():
            if pattern.search(contents):
                offenders.append(f"{path.name}: {token}")

    assert not offenders, "Forbidden kernel tokens found: " + ", ".join(offenders)
