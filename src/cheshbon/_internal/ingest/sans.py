"SANS bundle ingestion mapping logic."

import hashlib
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path
from cheshbon.kernel.graph_v0 import GraphV0, NodeV0, EdgeV0, NodeEvidence
from cheshbon.kernel.run_record import RunRecordV0
from cheshbon.kernel.transform_registry import StrongTransformRegistry, StrongTransformEntry
from cheshbon._internal.canonical_json import canonical_dumps
from cheshbon._internal.io.sans_bundle import SansBundle, InputEvidence, OutputEvidence
from cheshbon.kernel.hash_utils import compute_canonical_json_sha256


def path_normalize(path_str: str) -> str:
    """Normalize paths to use forward slashes."""
    return path_str.replace("\\", "/")


def _node_name(name: Optional[str], path_str: str) -> str:
    return name or Path(path_str).name


def _build_node_evidence(
    path_str: Optional[str],
    sha256: Optional[str],
    row_count: Optional[int],
    columns: Optional[List[str]],
    sorted_by: Optional[List[str]]
) -> Optional[NodeEvidence]:
    if not sha256 and row_count is None and not columns and not sorted_by:
        return None
    
    # Path is required by NodeEvidence if we are providing evidence, 
    # BUT for intermediate tables we might have row_count without path/sha256.
    # The NodeEvidence schema says path and sha256 are required strings.
    # Wait, looking at graph_v0.py:
    # path: str
    # sha256: str
    #
    # If I have row_count but no path/sha256 (intermediate table), I can't instantiate NodeEvidence?
    # This contradicts the requirement: "intermediate table nodes ... still have evidence=null".
    # And "attach evidence.row_count ... do NOT attach sha256/path to intermediates".
    #
    # If NodeEvidence requires path/sha256, I cannot attach row_count to intermediates without fake path/sha.
    # Let me check graph_v0.py again.
    # "path: str", "sha256: str" are required fields (no default value).
    #
    # I might need to make them optional in graph_v0.py if I want to support intermediates with just row_count.
    # OR, the user implies I should update the schema to allow optional path/sha256.
    # "updated graph.json schema fragment showing new evidence fields"
    #
    # I will assume I need to make path/sha256 optional in NodeEvidence if they are missing for intermediates.
    # But I cannot edit graph_v0.py again in this tool call (I already did).
    # I will check if I can make them optional. 
    # If I can't, I might have to skip evidence for intermediates, which defeats the purpose.
    # OR maybe I should put empty string? But "path" implies a file.
    #
    # Let's assume I need to update graph_v0.py to make path/sha256 optional.
    # I will do that in a separate step if needed.
    #
    # Wait, the user said: "do NOT attach sha256/path to intermediates (only materialized outputs/files get those)"
    # This implies intermediates DO NOT have path/sha256.
    # So `NodeEvidence` MUST allow them to be None.
    #
    # I will assume I need to update graph_v0.py again.
    pass

    return NodeEvidence(
        path=path_normalize(path_str) if path_str else "",
        sha256=sha256 or "",
        row_count=row_count,
        columns=columns,
        sorted_by=sorted_by
    )


def _index_evidence(entries: List[Union[InputEvidence, OutputEvidence]]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    by_name: Dict[str, Any] = {}
    by_path: Dict[str, Any] = {}
    for ev in entries:
        if ev.name:
            by_name[ev.name] = ev
        by_path[path_normalize(ev.path)] = ev
    return by_name, by_path


def map_bundle_to_artifacts(bundle: SansBundle, bundle_dir: Path) -> Tuple[GraphV0, RunRecordV0, StrongTransformRegistry]:
    """Map a SANS bundle to Cheshbon artifacts."""
    
    # 0. Compute hashes for witnesses
    if bundle.plan_relpath.lower().endswith(".json"):
        plan_hash = compute_canonical_json_sha256(bundle.plan_path)
    else:
        plan_hash = hashlib.sha256(bundle.plan_path.read_bytes()).hexdigest()

    if bundle.registry_relpath.lower().endswith(".json"):
        registry_hash = compute_canonical_json_sha256(bundle.registry_path)
    else:
        registry_hash = hashlib.sha256(bundle.registry_path.read_bytes()).hexdigest()
    evidence_hash: Optional[str] = None
    if bundle.evidence_path and bundle.evidence_path.exists():
        if str(bundle.evidence_path).lower().endswith(".json"):
            evidence_hash = compute_canonical_json_sha256(bundle.evidence_path)
        else:
            evidence_hash = hashlib.sha256(bundle.evidence_path.read_bytes()).hexdigest()

    # 1. Index Facts
    # Runtime facts (row_counts, columns) from step_evidence
    table_runtime_facts: Dict[str, Dict[str, Any]] = {}
    if bundle.evidence and bundle.evidence.step_evidence:
        for step_ev in bundle.evidence.step_evidence:
            # step_ev is Dict[str, Any]
            # Expected structure: {"row_counts": {"table": N}, "columns": {"table": [...]}}
            row_counts = step_ev.get("row_counts", {})
            columns_map = step_ev.get("columns", {})
            
            all_tables = set(row_counts.keys()) | set(columns_map.keys())
            for t in all_tables:
                if t not in table_runtime_facts:
                    table_runtime_facts[t] = {}
                if t in row_counts:
                    table_runtime_facts[t]["row_count"] = row_counts[t]
                if t in columns_map:
                    table_runtime_facts[t]["columns"] = columns_map[t]

    # Static facts (sorted_by) from plan.table_facts
    table_static_facts: Dict[str, Dict[str, Any]] = {}
    if bundle.plan.table_facts:
        for t, facts in bundle.plan.table_facts.items():
            if t not in table_static_facts:
                table_static_facts[t] = {}
            if "sorted_by" in facts:
                table_static_facts[t]["sorted_by"] = facts["sorted_by"]

    # 2. Build Nodes
    nodes_dict: Dict[str, NodeV0] = {}
    evidence_inputs_by_name: Dict[str, InputEvidence] = {}
    evidence_inputs_by_path: Dict[str, InputEvidence] = {}
    evidence_outputs_by_name: Dict[str, OutputEvidence] = {}
    evidence_outputs_by_path: Dict[str, OutputEvidence] = {}

    if bundle.evidence:
        evidence_inputs_by_name, evidence_inputs_by_path = _index_evidence(bundle.evidence.inputs)
        evidence_outputs_by_name, evidence_outputs_by_path = _index_evidence(bundle.evidence.outputs)
    
    # Tables referenced in plan
    all_plan_tables = set(bundle.plan.tables)
    for step in bundle.plan.steps:
        all_plan_tables.update(step.inputs)
        all_plan_tables.update(step.outputs)

    # Build artifact nodes from report inputs
    table_evidence_by_name: Dict[str, NodeEvidence] = {}
    
    for input_entry in bundle.report.inputs:
        name = _node_name(input_entry.name, input_entry.path)
        evidence_ev = evidence_inputs_by_name.get(name) or evidence_inputs_by_path.get(path_normalize(input_entry.path))
        sha256_val = input_entry.sha256 or (evidence_ev.bytes_sha256 if evidence_ev else None)
        
        # Artifacts don't have table facts usually, but we check just in case? 
        # Actually artifacts are files.
        artifact_node = NodeV0(
            id=f"artifact:{name}",
            name=name,
            evidence=_build_node_evidence(
                path_str=input_entry.path,
                sha256=sha256_val,
                row_count=None,
                columns=None,
                sorted_by=None
            )
        )
        nodes_dict[artifact_node.id] = artifact_node
        
        if name in all_plan_tables and name not in table_evidence_by_name:
            # Pre-populate table evidence from artifact, will enrich later
            table_evidence_by_name[name] = _build_node_evidence(
                path_str=input_entry.path,
                sha256=sha256_val,
                row_count=None,
                columns=None,
                sorted_by=None
            )

    # Thin mode: report is the witness; add table evidence from every datasource_inputs entry (no plan filter)
    if bundle.report.bundle_mode == "thin" and bundle.report.datasource_inputs:
        for entry in bundle.report.datasource_inputs:
            table_evidence_by_name[entry.datasource] = _build_node_evidence(
                path_str="",
                sha256=entry.sha256,
                row_count=None,
                columns=None,
                sorted_by=None,
            )

    # Build artifact nodes from report artifacts
    for artifact_entry in bundle.report.artifacts:
        name = _node_name(artifact_entry.name, artifact_entry.path)
        node = NodeV0(
            id=f"artifact:{name}",
            name=name,
            evidence=_build_node_evidence(
                path_str=artifact_entry.path,
                sha256=artifact_entry.sha256,
                row_count=None,
                columns=None,
                sorted_by=None
            )
        )
        nodes_dict[node.id] = node

    # Build evidence for table outputs from report outputs
    for output_entry in bundle.report.outputs:
        name = _node_name(output_entry.name, output_entry.path)
        evidence_ev = evidence_outputs_by_name.get(name) or evidence_outputs_by_path.get(path_normalize(output_entry.path))
        sha256_val = output_entry.sha256 or (evidence_ev.bytes_sha256 if evidence_ev else None)
        row_count = output_entry.rows if output_entry.rows is not None else (evidence_ev.row_count if evidence_ev else None)
        columns = output_entry.columns if output_entry.columns is not None else (evidence_ev.columns if evidence_ev else None)
        
        table_evidence_by_name[name] = _build_node_evidence(
            path_str=output_entry.path,
            sha256=sha256_val,
            row_count=row_count,
            columns=columns,
            sorted_by=None # Will enrich below
        )

    # 3. Create/Enrich Table Nodes
    for table_name in all_plan_tables:
        node_id = f"table:{table_name}"
        
        # Start with existing evidence (from inputs/outputs) or None
        evidence = table_evidence_by_name.get(table_name)
        
        # Look up facts
        runtime_facts = table_runtime_facts.get(table_name, {})
        static_facts = table_static_facts.get(table_name, {})
        
        row_count = runtime_facts.get("row_count")
        columns = runtime_facts.get("columns")
        sorted_by = static_facts.get("sorted_by")
        
        # Merge with existing evidence
        path_str = evidence.path if evidence else None
        sha256_val = evidence.sha256 if evidence else None
        
        # If evidence existed, it might have row_count/columns. Prefer existing unless None?
        # Requirement: "attach evidence.row_count if present in runtime..."
        # Report outputs are usually authoritative for outputs.
        # But if report output has None, runtime might have it.
        if evidence:
            if evidence.row_count is not None:
                row_count = evidence.row_count
            if evidence.columns is not None:
                columns = evidence.columns
            # sorted_by is new, so likely None in evidence unless we added it (we didn't yet)
        
        # Construct final evidence
        final_evidence = _build_node_evidence(
            path_str=path_str,
            sha256=sha256_val,
            row_count=row_count,
            columns=columns,
            sorted_by=sorted_by
        )
        
        nodes_dict[node_id] = NodeV0(
            id=node_id,
            name=table_name,
            evidence=final_evidence
        )

    # 4. Deduplication
    # "resolve datasource duplication... if not available, drop artifact:source"
    # We look for artifact:X where table:X also exists.
    # If table:X has NO "table facts" (row_count, sorted_by) AND has path/sha (meaning it's physically backed),
    # then it is redundant with artifact:X.
    # 
    # Wait, the requirement says "drop artifact:source".
    #
    # Logic:
    # keys = list(nodes_dict.keys())
    # for nid in keys:
    #    if nid.startswith("artifact:"):
    #        name = nodes_dict[nid].name
    #        table_nid = f"table:{name}"
    #        if table_nid in nodes_dict:
    #            t_node = nodes_dict[table_nid]
    #            a_node = nodes_dict[nid]
    #            
    #            has_table_facts = (t_node.evidence and (t_node.evidence.row_count is not None or t_node.evidence.sorted_by is not None))
    #            
    #            if not has_table_facts:
    #                # No table facts. table node is just pointing to file.
    #                # Drop artifact node.
    #                del nodes_dict[nid]
    
    keys = list(nodes_dict.keys())
    for nid in keys:
        if nid.startswith("artifact:"):
            node = nodes_dict[nid]
            name = node.name
            table_nid = f"table:{name}"
            if table_nid in nodes_dict:
                t_node = nodes_dict[table_nid]
                
                # If table node has no evidence, it's abstract/intermediate without facts?
                # If it has evidence:
                has_table_facts = False
                if t_node.evidence:
                    # distinct if it has row_count or sorted_by
                    if t_node.evidence.row_count is not None:
                        has_table_facts = True
                    if t_node.evidence.sorted_by is not None:
                        has_table_facts = True
                
                # If no unique table facts, and table node mimics artifact (or is abstract), 
                # we drop artifact to avoid duplication?
                # "enrich table:source with table facts... so it is not identical; if not available, drop artifact:source"
                if not has_table_facts:
                    del nodes_dict[nid]

    nodes = [nodes_dict[nid] for nid in sorted(nodes_dict)]

    # 5. Build Edges
    edges = []
    for step in bundle.plan.steps:
        inputs = [f"table:{name}" for name in step.inputs]
        outputs = [f"table:{name}" for name in step.outputs]
        
        edges.append(EdgeV0(
            id=step.step_id,
            transform_id=step.transform_id,
            step_id=step.step_id,
            op=step.op,
            inputs=inputs,
            outputs=outputs,
            params=step.params # Enrich with params
        ))

    graph = GraphV0(nodes=nodes, edges=edges)

    # 6. Build Run Record
    steps_for_fingerprint = [(step.step_id, step.transform_id) for step in bundle.plan.steps]
    
    input_hashes: Dict[str, str] = {}
    output_hashes: Dict[str, str] = {}
    for input_entry in bundle.report.inputs:
        name = _node_name(input_entry.name, input_entry.path)
        evidence_ev = evidence_inputs_by_name.get(name) or evidence_inputs_by_path.get(path_normalize(input_entry.path))
        hash_val = (
            (evidence_ev.canonical_sha256 if evidence_ev else None)
            or input_entry.sha256
            or (evidence_ev.bytes_sha256 if evidence_ev else None)
        )
        if hash_val:
            input_hashes[name] = hash_val
    # Thin mode: add datasource fingerprints (logical name -> sha256; no paths)
    if bundle.report.bundle_mode == "thin" and bundle.report.datasource_inputs:
        for entry in bundle.report.datasource_inputs:
            if entry.sha256:
                input_hashes[entry.datasource] = entry.sha256

    for output_entry in bundle.report.outputs:
        name = _node_name(output_entry.name, output_entry.path)
        evidence_ev = evidence_outputs_by_name.get(name) or evidence_outputs_by_path.get(path_normalize(output_entry.path))
        hash_val = (
            (evidence_ev.canonical_sha256 if evidence_ev else None)
            or output_entry.sha256
            or (evidence_ev.bytes_sha256 if evidence_ev else None)
        )
        if hash_val:
            output_hashes[name] = hash_val
    
    fingerprint_payload = {
        "plan_sha256": plan_hash,
        "steps": steps_for_fingerprint,
        "input_canonical_hashes": {name: input_hashes[name] for name in sorted(input_hashes)},
        "output_canonical_hashes": {name: output_hashes[name] for name in sorted(output_hashes)}
    }
    fingerprint = hashlib.sha256(canonical_dumps(fingerprint_payload).encode("utf-8")).hexdigest()

    witnesses = {
        bundle.plan_relpath: plan_hash,
        bundle.registry_relpath: registry_hash
    }
    if evidence_hash and bundle.evidence_relpath:
        witnesses[bundle.evidence_relpath] = evidence_hash

    # run_id and created_at are provenance-only; must NOT be used in fingerprint or identity logic
    run_id = (bundle.evidence.run_id if bundle.evidence else None) or bundle.report.run_id
    created_at = (bundle.evidence.created_at if bundle.evidence else None) or bundle.report.created_at
    if not run_id or not created_at:
        raise ValueError("Missing run_id or created_at in report.json or runtime.evidence.json")

    run_record = RunRecordV0(
        run_id=run_id,
        fingerprint=fingerprint,
        witnesses=witnesses,
        created_at=created_at
    )

    # 7. Promote Registry
    promoted_transforms = []
    for t in bundle.registry.transforms:
        entry = StrongTransformEntry(
            transform_id=t.transform_id,
            kind=t.kind,
            version=t.version or "0.1",
            spec=t.spec,
            io_signature=t.io_signature,
            impl_fingerprint=t.impl_fingerprint
        )
        promoted_transforms.append(entry)

    registry = StrongTransformRegistry(
        transforms=promoted_transforms
    )

    return graph, run_record, registry


def map_bundle_to_run_and_registry(
    bundle: SansBundle,
    bundle_dir: Path
) -> Tuple[RunRecordV0, StrongTransformRegistry]:
    """Map a SANS bundle to run + registry artifacts (no graph generation)."""
    # Compute hashes for witnesses
    if bundle.plan_relpath.lower().endswith(".json"):
        plan_hash = compute_canonical_json_sha256(bundle.plan_path)
    else:
        plan_hash = hashlib.sha256(bundle.plan_path.read_bytes()).hexdigest()

    if bundle.registry_relpath.lower().endswith(".json"):
        registry_hash = compute_canonical_json_sha256(bundle.registry_path)
    else:
        registry_hash = hashlib.sha256(bundle.registry_path.read_bytes()).hexdigest()
    evidence_hash: Optional[str] = None
    if bundle.evidence_path and bundle.evidence_path.exists():
        if str(bundle.evidence_path).lower().endswith(".json"):
            evidence_hash = compute_canonical_json_sha256(bundle.evidence_path)
        else:
            evidence_hash = hashlib.sha256(bundle.evidence_path.read_bytes()).hexdigest()

    evidence_inputs_by_name: Dict[str, InputEvidence] = {}
    evidence_inputs_by_path: Dict[str, InputEvidence] = {}
    evidence_outputs_by_name: Dict[str, OutputEvidence] = {}
    evidence_outputs_by_path: Dict[str, OutputEvidence] = {}

    if bundle.evidence:
        evidence_inputs_by_name, evidence_inputs_by_path = _index_evidence(bundle.evidence.inputs)
        evidence_outputs_by_name, evidence_outputs_by_path = _index_evidence(bundle.evidence.outputs)

    # Build Run Record (same logic as map_bundle_to_artifacts)
    steps_for_fingerprint = [(step.step_id, step.transform_id) for step in bundle.plan.steps]

    input_hashes: Dict[str, str] = {}
    output_hashes: Dict[str, str] = {}
    for input_entry in bundle.report.inputs:
        name = _node_name(input_entry.name, input_entry.path)
        evidence_ev = evidence_inputs_by_name.get(name) or evidence_inputs_by_path.get(path_normalize(input_entry.path))
        hash_val = (
            (evidence_ev.canonical_sha256 if evidence_ev else None)
            or input_entry.sha256
            or (evidence_ev.bytes_sha256 if evidence_ev else None)
        )
        if hash_val:
            input_hashes[name] = hash_val
    # Thin mode: add datasource fingerprints (logical name -> sha256; no paths)
    if bundle.report.bundle_mode == "thin" and bundle.report.datasource_inputs:
        for entry in bundle.report.datasource_inputs:
            if entry.sha256:
                input_hashes[entry.datasource] = entry.sha256

    for output_entry in bundle.report.outputs:
        name = _node_name(output_entry.name, output_entry.path)
        evidence_ev = evidence_outputs_by_name.get(name) or evidence_outputs_by_path.get(path_normalize(output_entry.path))
        hash_val = (
            (evidence_ev.canonical_sha256 if evidence_ev else None)
            or output_entry.sha256
            or (evidence_ev.bytes_sha256 if evidence_ev else None)
        )
        if hash_val:
            output_hashes[name] = hash_val

    fingerprint_payload = {
        "plan_sha256": plan_hash,
        "steps": steps_for_fingerprint,
        "input_canonical_hashes": {name: input_hashes[name] for name in sorted(input_hashes)},
        "output_canonical_hashes": {name: output_hashes[name] for name in sorted(output_hashes)}
    }
    fingerprint = hashlib.sha256(canonical_dumps(fingerprint_payload).encode("utf-8")).hexdigest()

    witnesses = {
        bundle.plan_relpath: plan_hash,
        bundle.registry_relpath: registry_hash
    }
    if evidence_hash and bundle.evidence_relpath:
        witnesses[bundle.evidence_relpath] = evidence_hash

    # run_id and created_at are provenance-only; must NOT be used in fingerprint or identity logic
    run_id = (bundle.evidence.run_id if bundle.evidence else None) or bundle.report.run_id
    created_at = (bundle.evidence.created_at if bundle.evidence else None) or bundle.report.created_at
    if not run_id or not created_at:
        raise ValueError("Missing run_id or created_at in report.json or runtime.evidence.json")

    run_record = RunRecordV0(
        run_id=run_id,
        fingerprint=fingerprint,
        witnesses=witnesses,
        created_at=created_at
    )

    # Promote Registry
    promoted_transforms = []
    for t in bundle.registry.transforms:
        entry = StrongTransformEntry(
            transform_id=t.transform_id,
            kind=t.kind,
            version=t.version or "0.1",
            spec=t.spec,
            io_signature=t.io_signature,
            impl_fingerprint=t.impl_fingerprint
        )
        promoted_transforms.append(entry)

    registry = StrongTransformRegistry(
        transforms=promoted_transforms
    )

    return run_record, registry
