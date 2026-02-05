"""SANS bundle verification logic."""

import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import List, Tuple, Any, Optional
from cheshbon.codes import ValidationCode
from cheshbon._internal.io.sans_bundle import SansBundle, resolve_bundle_path
from cheshbon.kernel.hash_utils import canonicalize_json, compute_canonical_json_sha256


class BundleVerificationError(Exception):
    """Exception raised when bundle verification fails."""
    def __init__(self, code: ValidationCode, message: str):
        self.code = code
        self.message = message
        super().__init__(f"[{code.value}] {message}")


def verify_bundle(bundle: SansBundle, bundle_dir: Path) -> None:
    """Verify integrity and consistency of a SANS bundle."""
    
    def _normalize_relpath(path_str: str) -> str:
        return str(PurePosixPath(path_str.replace("\\", "/")))

    def _find_artifact_by_path(path_str: str) -> Optional[Any]:
        normalized = _normalize_relpath(path_str)
        for artifact in bundle.report.artifacts:
            if _normalize_relpath(artifact.path) == normalized:
                return artifact
        return None

    def _find_artifact_by_name(name: str) -> Optional[Any]:
        for artifact in bundle.report.artifacts:
            if artifact.name == name:
                return artifact
        return None

    def _entry_hash(path: Path, report_path: str) -> str:
        if report_path.lower().endswith(".json"):
            return compute_canonical_json_sha256(path)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    # Rule 1: sha256(plan.ir.json canonical JSON) must match report/evidence hash
    actual_plan_hash = _entry_hash(bundle.plan_path, bundle.plan_relpath)
    expected_plan_hash: Optional[str] = None
    plan_artifact = _find_artifact_by_path(bundle.plan_relpath) or _find_artifact_by_name("plan.ir.json")
    if plan_artifact:
        expected_plan_hash = plan_artifact.sha256
    elif bundle.evidence and bundle.evidence.plan_ir:
        expected_plan_hash = bundle.evidence.plan_ir.sha256

    if not expected_plan_hash:
        raise BundleVerificationError(
            ValidationCode.INVALID_STRUCTURE,
            "Missing plan hash in report.json artifacts or runtime.evidence.json"
        )
    if actual_plan_hash != expected_plan_hash:
        raise BundleVerificationError(
            ValidationCode.HASH_MISMATCH,
            f"Plan hash mismatch. Expected {expected_plan_hash}, got {actual_plan_hash}"
        )

    # Rule 2: for any step index i: missing registry.index[i]
    for i in range(len(bundle.plan.steps)):
        if str(i) not in bundle.registry.index:
            raise BundleVerificationError(
                ValidationCode.REGISTRY_INDEX_INCOMPLETE,
                f"Step {i} missing from registry index"
            )

    # Rule 3: for any step i: registry.index[i] != plan.steps[i].transform_id
    for i, step in enumerate(bundle.plan.steps):
        transform_id = bundle.registry.index[str(i)]
        if transform_id != step.transform_id:
            raise BundleVerificationError(
                ValidationCode.STEP_ID_CONFLICT,
                f"Step {i} transform_id mismatch. Plan says {step.transform_id}, Registry index says {transform_id}"
            )

    # Rule 4: for any step i: plan.steps[i].step_id != sha256(canon({transform_id, inputs, outputs}))
    for i, step in enumerate(bundle.plan.steps):
        step_payload = {
            "transform_id": step.transform_id,
            "inputs": step.inputs,
            "outputs": step.outputs
        }
        expected_step_id = hashlib.sha256(canonicalize_json(step_payload).encode("utf-8")).hexdigest()
        if step.step_id != expected_step_id:
            raise BundleVerificationError(
                ValidationCode.STEP_ID_CONFLICT,
                f"Step {i} step_id mismatch. Expected {expected_step_id} based on wiring, got {step.step_id}"
            )

    # Rule 5: for any report entry: referenced file missing OR hash mismatch
    for entry in bundle.report.inputs + bundle.report.outputs:
        p = resolve_bundle_path(bundle_dir, entry.path)
        if not p.exists():
            table_name = entry.name or Path(entry.path).name
            raise BundleVerificationError(
                ValidationCode.FILE_NOT_FOUND,
                f"File not found for table '{table_name}': {entry.path}"
            )

        actual_hash = _entry_hash(p, entry.path)
        if actual_hash != entry.sha256:
            table_name = entry.name or Path(entry.path).name
            raise BundleVerificationError(
                ValidationCode.HASH_MISMATCH,
                f"Hash mismatch for table '{table_name}'. Expected {entry.sha256}, got {actual_hash}"
            )

    # Rule 5b: verify report artifacts (canonical hash for json)
    for artifact in bundle.report.artifacts:
        artifact_path = resolve_bundle_path(bundle_dir, artifact.path)
        if not artifact_path.exists():
            raise BundleVerificationError(
                ValidationCode.FILE_NOT_FOUND,
                f"File not found for artifact '{artifact.name}': {artifact.path}"
            )
        actual_hash = _entry_hash(artifact_path, artifact.path)
        if actual_hash != artifact.sha256:
            raise BundleVerificationError(
                ValidationCode.HASH_MISMATCH,
                f"Artifact hash mismatch for '{artifact.name}'. Expected {artifact.sha256}, got {actual_hash}"
            )

    # Rule 6: any transform_id referenced by registry.index is absent from registry.transforms
    registered_ids = {t.transform_id for t in bundle.registry.transforms}
    for i in range(len(bundle.plan.steps)):
        transform_id = bundle.registry.index[str(i)]
        if transform_id not in registered_ids:
            raise BundleVerificationError(
                ValidationCode.TRANSFORM_NOT_FOUND,
                f"Transform {transform_id} (step {i}) not found in registry transforms"
            )

    # Rule 7: any registry transform missing spec
    from cheshbon.kernel.hash_utils import CanonicalizationError
    for t in bundle.registry.transforms:
        if not t.spec:
            raise BundleVerificationError(
                ValidationCode.INVALID_STRUCTURE,
                f"Transform {t.transform_id} missing REQUIRED 'spec'"
            )
        
        # Semantic check: transform_id MUST match sha256(canon(spec))
        try:
            expected_tid = hashlib.sha256(canonicalize_json(t.spec).encode("utf-8")).hexdigest()
        except CanonicalizationError as e:
            raise BundleVerificationError(
                ValidationCode.INVALID_STRUCTURE,
                f"Transform {t.transform_id} has invalid spec: {str(e)}"
            )
            
        if t.transform_id != expected_tid:
            raise BundleVerificationError(
                ValidationCode.STEP_ID_CONFLICT,
                f"Transform {t.transform_id} id mismatch. Based on spec, it should be {expected_tid}"
            )
