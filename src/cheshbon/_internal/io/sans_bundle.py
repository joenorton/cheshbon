"""SANS bundle loaders and models."""

import json
from pathlib import Path, PurePosixPath
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ConfigDict, AliasChoices, ValidationError

SUPPORTED_REPORT_SCHEMA_VERSIONS = {"0.2", "0.3"}


class SansStep(BaseModel):
    kind: str
    op: str
    params: Dict[str, Any]
    transform_id: str
    inputs: List[str]
    outputs: List[str]
    step_id: str
    loc: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="ignore")


class SansPlanIR(BaseModel):
    steps: List[SansStep]
    tables: List[str]
    table_facts: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="ignore")


class PathHash(BaseModel):
    path: str
    sha256: str


class InputEvidence(BaseModel):
    name: Optional[str] = None
    path: str
    format: Optional[str] = None
    bytes_sha256: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("bytes_sha256", "sha256")
    )
    canonical_sha256: Optional[str] = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class OutputEvidence(BaseModel):
    name: Optional[str] = None
    path: str
    format: Optional[str] = None
    bytes_sha256: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("bytes_sha256", "sha256")
    )
    canonical_sha256: Optional[str] = None
    row_count: Optional[int] = None
    columns: Optional[List[str]] = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class SansEvidence(BaseModel):
    sans_version: Optional[str] = None
    run_id: Optional[str] = None
    created_at: Optional[str] = None
    plan_ir: Optional[PathHash] = None
    bindings: Optional[Dict[str, str]] = None
    inputs: List[InputEvidence] = Field(default_factory=list)
    outputs: List[OutputEvidence] = Field(default_factory=list)
    step_evidence: Optional[List[Dict[str, Any]]] = None

    model_config = ConfigDict(extra="ignore")


class SansTransformCandidate(BaseModel):
    transform_id: str
    kind: str
    version: Optional[str] = "0.1"
    spec: Dict[str, Any]
    io_signature: Optional[Dict[str, Any]] = None
    impl_fingerprint: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


class SansRegistryCandidate(BaseModel):
    registry_version: str
    transforms: List[SansTransformCandidate]
    index: Dict[str, str]  # step_index (str in JSON keys) -> transform_id

    model_config = ConfigDict(extra="ignore")


class SansReportInput(BaseModel):
    role: Optional[str] = None
    name: Optional[str] = None
    path: str
    sha256: str

    model_config = ConfigDict(extra="ignore")


class SansReportArtifact(BaseModel):
    name: str
    path: str
    sha256: str

    model_config = ConfigDict(extra="ignore")


class SansReportOutput(BaseModel):
    name: Optional[str] = None
    path: str
    sha256: str
    rows: Optional[int] = None
    columns: Optional[List[str]] = None

    model_config = ConfigDict(extra="ignore")


class SansReport(BaseModel):
    report_schema_version: str
    plan_path: Optional[str] = None
    inputs: List[SansReportInput] = Field(default_factory=list)
    artifacts: List[SansReportArtifact] = Field(default_factory=list)
    outputs: List[SansReportOutput] = Field(default_factory=list)
    run_id: Optional[str] = None
    created_at: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


class SansBundle(BaseModel):
    report: SansReport
    plan: SansPlanIR
    evidence: Optional[SansEvidence] = None
    registry: SansRegistryCandidate
    plan_path: Path
    registry_path: Path
    evidence_path: Optional[Path] = None
    plan_relpath: str
    registry_relpath: str
    evidence_relpath: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


def _normalize_report_relpath(path_str: str) -> str:
    """Normalize a report path to forward-slash, bundle-relative form."""
    return str(PurePosixPath(path_str.replace("\\", "/")))


def resolve_bundle_path(bundle_dir: Path, relpath: str) -> Path:
    """Resolve a bundle-relative report path against the bundle root."""
    normalized = _normalize_report_relpath(relpath)
    posix_path = PurePosixPath(normalized)
    if posix_path.is_absolute() or ".." in posix_path.parts:
        raise ValueError(f"Report path must be bundle-relative: {relpath}")
    return bundle_dir.joinpath(*posix_path.parts)


def _find_artifact(report: SansReport, name: str) -> Optional[SansReportArtifact]:
    for artifact in report.artifacts:
        if artifact.name == name:
            return artifact
    return None


def load_bundle(bundle_dir: Path) -> SansBundle:
    """Load SANS bundle artifacts from directory."""
    report_path = bundle_dir / "report.json"
    if not report_path.exists():
        raise FileNotFoundError(f"Missing report.json in {bundle_dir}")

    report_data = json.loads(report_path.read_text(encoding="utf-8"))
    report = SansReport(**report_data)
    if report.report_schema_version not in SUPPORTED_REPORT_SCHEMA_VERSIONS:
        raise ValueError(
            "Unsupported report_schema_version "
            f"{report.report_schema_version!r}. Supported: {sorted(SUPPORTED_REPORT_SCHEMA_VERSIONS)}"
        )

    if not report.plan_path:
        raise ValueError("report.json missing required plan_path")

    plan_relpath = _normalize_report_relpath(report.plan_path)
    plan_path = resolve_bundle_path(bundle_dir, plan_relpath)
    if not plan_path.exists():
        raise FileNotFoundError(f"Missing plan at {plan_relpath} referenced by report.json")

    registry_relpath: Optional[str] = None
    registry_artifact = _find_artifact(report, "registry.candidate.json")
    if registry_artifact:
        registry_relpath = _normalize_report_relpath(registry_artifact.path)
    else:
        fallback = _normalize_report_relpath("artifacts/registry.candidate.json")
        if resolve_bundle_path(bundle_dir, fallback).exists():
            registry_relpath = fallback

    if not registry_relpath:
        raise FileNotFoundError("Missing registry.candidate.json (not listed in report.json artifacts)")

    registry_path = resolve_bundle_path(bundle_dir, registry_relpath)
    if not registry_path.exists():
        raise FileNotFoundError(f"Missing registry at {registry_relpath} referenced by report.json")

    evidence_relpath: Optional[str] = None
    evidence_artifact = _find_artifact(report, "runtime.evidence.json")
    if evidence_artifact:
        evidence_relpath = _normalize_report_relpath(evidence_artifact.path)
    else:
        fallback = _normalize_report_relpath("artifacts/runtime.evidence.json")
        if resolve_bundle_path(bundle_dir, fallback).exists():
            evidence_relpath = fallback

    evidence_path: Optional[Path] = None
    evidence: Optional[SansEvidence] = None
    if evidence_relpath:
        evidence_path = resolve_bundle_path(bundle_dir, evidence_relpath)
        if evidence_path.exists():
            evidence_data = json.loads(evidence_path.read_text(encoding="utf-8"))
            try:
                evidence = SansEvidence(**evidence_data)
            except ValidationError:
                evidence = None

    plan_data = json.loads(plan_path.read_text(encoding="utf-8"))
    registry_data = json.loads(registry_path.read_text(encoding="utf-8"))

    return SansBundle(
        report=report,
        plan=SansPlanIR(**plan_data),
        evidence=evidence,
        registry=SansRegistryCandidate(**registry_data),
        plan_path=plan_path,
        registry_path=registry_path,
        evidence_path=evidence_path,
        plan_relpath=plan_relpath,
        registry_relpath=registry_relpath,
        evidence_relpath=evidence_relpath
    )
