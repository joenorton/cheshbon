"""Registry loading and hashing utilities."""

import json
import hashlib
from pathlib import Path
from typing import Dict

from .canonical_json import canonical_dumps


def load_registry(registry_path: Path) -> Dict:
    """Load registry from JSON file and return as dict.
    
    Args:
        registry_path: Path to registry JSON file
        
    Returns:
        Registry data as dictionary
    """
    with open(registry_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def compute_registry_hash(registry_dict: Dict) -> str:
    """Compute SHA256 hash of canonicalized registry.
    
    Args:
        registry_dict: Registry data as dictionary
        
    Returns:
        SHA256 hash as hex string (prefixed with "sha256:")
    """
    canonical_str = canonical_dumps(registry_dict)
    digest = hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()
    return f"sha256:{digest}"
