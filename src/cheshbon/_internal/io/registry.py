"""Registry I/O helpers (internal)."""

from pathlib import Path
from typing import Union

from cheshbon.kernel.transform_registry import TransformRegistry


def load_registry_from_path(path: Union[str, Path]) -> TransformRegistry:
    """Load transform registry from a JSON file path."""
    registry_path = Path(path)
    data = registry_path.read_bytes()
    return TransformRegistry.from_json_bytes(data)
