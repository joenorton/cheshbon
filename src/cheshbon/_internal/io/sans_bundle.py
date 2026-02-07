"""SANS bundle loaders and models."""

import json
from pathlib import Path, PurePosixPath
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ConfigDict, AliasChoices, ValidationError

SUPPORTED_REPORT_SCHEMA_VERSIONS = {"0.2", "0.3"}

# Artifact names resolved via report.artifacts by name (authoritative index)
ARTIFACT_NAME_RUNTIME_EVIDENCE = "runtime.evidence.json"
ARTIFACT_NAME_SCHEMA_EVIDENCE = "schema.evidence.json"
ARTIFACT_NAME_SCHEMA_LOCK = "schema.lock.json"
ARTIFACT_NAME_REGISTRY_CANDIDATE = "registry.candidate.json"
ARTIFACT_NAME_PLAN_IR = "plan.ir.json"


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


class PrimaryError(BaseModel):
    """Report primary_error: code, message, optional loc."""

    code: str
    message: str
    loc: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="ignore")


class SansReport(BaseModel):
    report_schema_version: str
    plan_path: Optional[str] = None
    inputs: List[SansReportInput] = Field(default_factory=list)
    artifacts: List[SansReportArtifact] = Field(default_factory=list)
    outputs: List[SansReportOutput] = Field(default_factory=list)
    run_id: Optional[str] = None
    created_at: Optional[str] = None
    status: Optional[str] = None
    primary_error: Optional[PrimaryError] = None
    schema_lock_sha256: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


# --- Schema artifacts (sans v0.1+): explicit shapes ---
# schema.evidence.json: table -> { column: type }
SchemaEvidenceTable = Dict[str, str]  # column -> type


class SchemaEvidence(BaseModel):
    """schema.evidence.json: records table -> {column: type} snapshots."""

    tables: Dict[str, SchemaEvidenceTable] = Field(default_factory=dict)

    model_config = ConfigDict(extra="ignore")

    @classmethod
    def from_raw(cls, data: Dict[str, Any]) -> "SchemaEvidence":
        # Sans may emit "tables": { "table_id": { "col": "type" } } or root as table->cols
        tables: Dict[str, Dict[str, str]] = {}
        if "tables" in data and isinstance(data["tables"], dict):
            for k, v in data["tables"].items():
                if isinstance(v, dict):
                    tables[str(k)] = {str(ck): str(cv) for ck, cv in v.items()}
            return cls(tables=tables)
        for k, v in data.items():
            if k in ("tables",) or not isinstance(v, dict):
                continue
            try:
                tables[str(k)] = {str(ck): str(cv) for ck, cv in v.items()}
            except (TypeError, ValueError):
                pass
        return cls(tables=tables)


# schema.lock.json: real shape from sans (see fixtures/demo_high/dh_out)
# datasources: array of { name, columns: [ {name, type} ] }
class SchemaLockDatasource(BaseModel):
    """Per-datasource schema in schema.lock.json (column name -> type)."""

    columns: Dict[str, str] = Field(default_factory=dict)  # column -> type

    model_config = ConfigDict(extra="ignore")


class SchemaLock(BaseModel):
    """schema.lock.json: first-class contract; datasources keyed by name with column: type."""

    datasources: Dict[str, SchemaLockDatasource] = Field(default_factory=dict)

    model_config = ConfigDict(extra="ignore")

    @classmethod
    def from_raw(cls, data: Dict[str, Any]) -> "SchemaLock":
        # Real sans format: datasources is a list of { name, columns: [ {name, type} ] }
        ds_raw = data.get("datasources")
        datasources: Dict[str, SchemaLockDatasource] = {}
        if isinstance(ds_raw, list):
            for item in ds_raw:
                if not isinstance(item, dict):
                    continue
                ds_name = item.get("name")
                if ds_name is None:
                    continue
                cols_list = item.get("columns")
                if isinstance(cols_list, list):
                    columns = {
                        str(c.get("name", "")): str(c.get("type", ""))
                        for c in cols_list
                        if isinstance(c, dict) and c.get("name") is not None
                    }
                elif isinstance(cols_list, dict):
                    columns = {str(c): str(t) for c, t in cols_list.items()}
                else:
                    columns = {}
                datasources[str(ds_name)] = SchemaLockDatasource(columns=columns)
        elif isinstance(ds_raw, dict):
            for ds_id, ds_val in ds_raw.items():
                if isinstance(ds_val, dict):
                    cols = ds_val.get("columns")
                    if isinstance(cols, dict):
                        datasources[str(ds_id)] = SchemaLockDatasource(
                            columns={str(c): str(t) for c, t in cols.items()}
                        )
                    elif isinstance(cols, list):
                        columns = {
                            str(c.get("name", "")): str(c.get("type", ""))
                            for c in cols
                            if isinstance(c, dict) and c.get("name") is not None
                        }
                        datasources[str(ds_id)] = SchemaLockDatasource(columns=columns)
                    else:
                        datasources[str(ds_id)] = SchemaLockDatasource(columns={})
        return cls(datasources=datasources)


class SchemaArtifacts(BaseModel):
    """Parsed schema artifacts when present (schema.evidence.json, schema.lock.json)."""

    schema_evidence: Optional[SchemaEvidence] = None
    schema_lock: Optional[SchemaLock] = None
    schema_evidence_path: Optional[Path] = None
    schema_lock_path: Optional[Path] = None

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
    schema_artifacts: Optional[SchemaArtifacts] = None

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


def _resolve_artifact_path(
    bundle_dir: Path,
    report: SansReport,
    artifact_name: str,
    fallback_relpaths: Optional[List[str]] = None,
) -> Optional[Path]:
    """Resolve artifact path. report.artifacts is authoritative when non-empty."""
    if report.artifacts:
        art = _find_artifact(report, artifact_name)
        if art:
            return resolve_bundle_path(bundle_dir, art.path)
        # schema.lock.json: also allow bundle root when not listed in artifacts
        if artifact_name == ARTIFACT_NAME_SCHEMA_LOCK:
            root_lock = bundle_dir / "schema.lock.json"
            if root_lock.exists():
                return root_lock
        return None
    # Fallback only when report.artifacts is absent
    for rel in fallback_relpaths or []:
        try:
            p = resolve_bundle_path(bundle_dir, rel)
            if p.exists():
                return p
        except ValueError:
            continue
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

    registry_path = _resolve_artifact_path(
        bundle_dir,
        report,
        ARTIFACT_NAME_REGISTRY_CANDIDATE,
        fallback_relpaths=["artifacts/registry.candidate.json"],
    )
    if not registry_path or not registry_path.exists():
        raise FileNotFoundError(
            "Missing registry.candidate.json (not in report.artifacts or fallback)"
        )
    try:
        registry_relpath = _normalize_report_relpath(
            str(registry_path.relative_to(bundle_dir))
        )
    except ValueError:
        registry_relpath = _normalize_report_relpath(registry_path.name)

    evidence_path = _resolve_artifact_path(
        bundle_dir,
        report,
        ARTIFACT_NAME_RUNTIME_EVIDENCE,
        fallback_relpaths=["artifacts/runtime.evidence.json"],
    )
    evidence_relpath: Optional[str] = None
    evidence: Optional[SansEvidence] = None
    if evidence_path and evidence_path.exists():
        try:
            evidence_relpath = _normalize_report_relpath(
                str(evidence_path.relative_to(bundle_dir))
            )
        except ValueError:
            evidence_relpath = _normalize_report_relpath(evidence_path.name)
        try:
            evidence_data = json.loads(evidence_path.read_text(encoding="utf-8"))
            evidence = SansEvidence(**evidence_data)
        except (ValidationError, json.JSONDecodeError):
            evidence = None

    schema_evidence_path = _resolve_artifact_path(
        bundle_dir, report, ARTIFACT_NAME_SCHEMA_EVIDENCE
    )
    schema_lock_path = _resolve_artifact_path(
        bundle_dir, report, ARTIFACT_NAME_SCHEMA_LOCK
    )
    schema_artifacts: Optional[SchemaArtifacts] = None
    schema_evidence: Optional[SchemaEvidence] = None
    schema_lock: Optional[SchemaLock] = None
    if schema_evidence_path and schema_evidence_path.exists():
        try:
            schema_evidence = SchemaEvidence.from_raw(
                json.loads(schema_evidence_path.read_text(encoding="utf-8"))
            )
        except (json.JSONDecodeError, TypeError, ValueError):
            schema_evidence = None
    if schema_lock_path and schema_lock_path.exists():
        try:
            schema_lock = SchemaLock.from_raw(
                json.loads(schema_lock_path.read_text(encoding="utf-8"))
            )
        except (json.JSONDecodeError, TypeError, ValueError):
            schema_lock = None
    if schema_evidence is not None or schema_lock is not None:
        schema_artifacts = SchemaArtifacts(
            schema_evidence=schema_evidence,
            schema_lock=schema_lock,
            schema_evidence_path=schema_evidence_path if schema_evidence_path else None,
            schema_lock_path=schema_lock_path if schema_lock_path else None,
        )

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
        evidence_relpath=evidence_relpath,
        schema_artifacts=schema_artifacts,
    )
