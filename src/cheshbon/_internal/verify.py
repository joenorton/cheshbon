"""Evidence verification module.

This module provides functions to verify integrity of change objects and registry files.
"""

import hashlib
from pathlib import Path
from typing import Dict, Optional

from .canonical_json import canonical_dumps
from .registry import load_registry, compute_registry_hash


def verify_change(change: Dict, change_storage, ws_id: str) -> Dict:
    """
    Verify change object integrity.
    
    Checks:
    1. content_hash recomputation
    2. parent exists
    3. parent_hash matches parent content_hash
    
    Returns partial truth, does not crash.
    
    Args:
        change: Change object dict
        change_storage: ChangeStorage instance
        ws_id: Workspace ID
        
    Returns:
        Dict with verification results:
        - change_id: str
        - content_hash_ok: bool
        - parent_exists: bool
        - parent_hash_ok: Optional[bool] (None if parent missing)
    """
    change_id = change.get("change_id", "")
    
    # 1. Recompute content_hash
    change_without_hash = {
        k: v for k, v in change.items()
        if k != "content_hash"  # Exclude content_hash only
    }
    canonical_json = canonical_dumps(change_without_hash)
    recomputed_hash = hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
    stored_hash = change.get("content_hash", "")
    
    # Remove "sha256:" prefix if present for comparison
    if stored_hash.startswith("sha256:"):
        stored_hash_hex = stored_hash[7:]
    else:
        stored_hash_hex = stored_hash
    
    content_hash_ok = recomputed_hash == stored_hash_hex
    
    # 2. Check parent
    parent_change_id = change.get("parent_change_id")
    parent_exists = False
    parent_hash_ok = None
    
    if parent_change_id:
        # Extract numeric version for filename lookup
        if parent_change_id.startswith("chg:"):
            file_key = parent_change_id[4:]
        else:
            file_key = parent_change_id
        
        parent_change = change_storage.read_change(ws_id, file_key)
        
        if parent_change:
            parent_exists = True
            # Compare parent's content_hash to this change's parent_hash
            parent_content_hash = parent_change.get("content_hash", "")
            this_parent_hash = change.get("parent_hash", "")
            
            # Remove "sha256:" prefix if present
            if parent_content_hash.startswith("sha256:"):
                parent_content_hash_hex = parent_content_hash[7:]
            else:
                parent_content_hash_hex = parent_content_hash
            
            if this_parent_hash.startswith("sha256:"):
                this_parent_hash_hex = this_parent_hash[7:]
            else:
                this_parent_hash_hex = this_parent_hash
            
            parent_hash_ok = parent_content_hash_hex == this_parent_hash_hex
    
    return {
        "change_id": change_id,
        "content_hash_ok": content_hash_ok,
        "parent_exists": parent_exists,
        "parent_hash_ok": parent_hash_ok
    }


def verify_registry(registry_path: Path) -> Dict:
    """
    Verify registry file integrity.
    
    Checks:
    1. File loaded successfully
    2. Hash recomputation successful
    
    Returns partial truth, does not crash.
    
    Args:
        registry_path: Path to registry file
        
    Returns:
        Dict with verification results:
        - registry_hash: Optional[str] (None if file missing)
        - file_loaded_ok: bool
        - hash_recomputation_ok: bool
    """
    if not registry_path.exists():
        return {
            "registry_hash": None,
            "file_loaded_ok": False,
            "hash_recomputation_ok": False
        }
    
    try:
        registry_dict = load_registry(registry_path)
        registry_hash = compute_registry_hash(registry_dict)
        
        return {
            "registry_hash": registry_hash,
            "file_loaded_ok": True,
            "hash_recomputation_ok": True
        }
    except Exception:
        # File load or hash computation failed
        return {
            "registry_hash": None,
            "file_loaded_ok": False,
            "hash_recomputation_ok": False
        }
