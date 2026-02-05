"""Render human-readable explanations of impact (internal)."""

from typing import List

from cheshbon.kernel.diff import ChangeEvent
from cheshbon.kernel.explain import (
    explain_changes_structured,
    explain_impact_structured,
    ImpactExplanationResult,
)
from cheshbon.kernel.impact import ImpactResult
from cheshbon.kernel.spec import MappingSpec


def explain_changes(change_events: List[ChangeEvent], spec_v1: MappingSpec, spec_v2: MappingSpec) -> str:
    """Generate human-readable summary of what changed."""
    lines = ["## Changes Detected\n"]
    explanations = explain_changes_structured(change_events, spec_v1, spec_v2)

    for event in explanations:
        if event.change_type == "SOURCE_RENAMED":
            lines.append(f"- Source column renamed: `{event.old_value}` -> `{event.new_value}` (ID: {event.element_id})")
        elif event.change_type == "SOURCE_REMOVED":
            lines.append(f"- Source column removed: `{event.old_value}` (ID: {event.element_id})")
        elif event.change_type == "SOURCE_ADDED":
            lines.append(f"- Source column added: `{event.new_value}` (ID: {event.element_id})")
        elif event.change_type == "DERIVED_INPUTS_CHANGED":
            if event.element_name:
                lines.append(f"- Derived variable `{event.element_name}` inputs changed (ID: {event.element_id})")
                if event.details and "old_inputs" in event.details:
                    lines.append(f"  - Old: {event.details['old_inputs']}")
                    lines.append(f"  - New: {event.details['new_inputs']}")
        elif event.change_type == "DERIVED_TRANSFORM_REF_CHANGED":
            if event.element_name:
                lines.append(f"- Derived variable `{event.element_name}` transform reference changed (ID: {event.element_id})")
                lines.append(f"  - Old: `{event.old_value}`")
                lines.append(f"  - New: `{event.new_value}`")
        elif event.change_type == "DERIVED_TRANSFORM_PARAMS_CHANGED":
            if event.element_name:
                transform_ref = event.details.get("transform_ref", "unknown") if event.details else "unknown"
                lines.append(f"- Derived variable `{event.element_name}` transform parameters changed (ID: {event.element_id})")
                lines.append(f"  - Transform: `{transform_ref}`")
                lines.append(f"  - Old params hash: {event.old_value[:16]}...")
                lines.append(f"  - New params hash: {event.new_value[:16]}...")
        elif event.change_type == "TRANSFORM_IMPL_CHANGED":
            lines.append(f"- Transform implementation changed: `{event.element_id}`")
            if event.details:
                old_ref = event.details.get("old_ref", "unknown")
                new_ref = event.details.get("new_ref", "unknown")
                old_digest = event.old_value[:16] + "..." if event.old_value else "unknown"
                new_digest = event.new_value[:16] + "..." if event.new_value else "unknown"

                lines.append(f"  - Old digest: {old_digest}")
                lines.append(f"  - New digest: {new_digest}")
                if old_ref != "unknown":
                    lines.append(f"  - Old reference: {old_ref}")
                if new_ref != "unknown":
                    lines.append(f"  - New reference: {new_ref}")
                old_source = event.details.get("old_source")
                new_source = event.details.get("new_source")
                if old_source and new_source and old_source != new_source:
                    lines.append(f"  - Source: {old_source} -> {new_source}")
        elif event.change_type == "TRANSFORM_ADDED":
            lines.append(f"- Transform added: `{event.element_id}` (version: {event.new_value})")
        elif event.change_type == "TRANSFORM_REMOVED":
            lines.append(f"- Transform removed: `{event.element_id}` (version: {event.old_value})")
        elif event.change_type == "DERIVED_TYPE_CHANGED":
            if event.element_name:
                lines.append(f"- Derived variable `{event.element_name}` type changed: `{event.old_value}` -> `{event.new_value}` (ID: {event.element_id})")
        elif event.change_type == "DERIVED_REMOVED":
            lines.append(f"- Derived variable removed: `{event.old_value}` (ID: {event.element_id})")
        elif event.change_type == "DERIVED_ADDED":
            lines.append(f"- Derived variable added: `{event.new_value}` (ID: {event.element_id})")
        elif event.change_type == "DERIVED_RENAMED":
            if event.element_name:
                lines.append(f"- Derived variable renamed: `{event.old_value}` -> `{event.new_value}` (ID: {event.element_id})")
        elif event.change_type == "CONSTRAINT_REMOVED":
            lines.append(f"- Constraint removed: `{event.old_value}` (ID: {event.element_id})")
        elif event.change_type == "CONSTRAINT_ADDED":
            lines.append(f"- Constraint added: `{event.new_value}` (ID: {event.element_id})")
        elif event.change_type == "CONSTRAINT_RENAMED":
            if event.element_name:
                lines.append(f"- Constraint renamed: `{event.old_value}` -> `{event.new_value}` (ID: {event.element_id})")
        elif event.change_type == "CONSTRAINT_INPUTS_CHANGED":
            if event.element_name:
                lines.append(f"- Constraint `{event.element_name}` inputs changed (ID: {event.element_id})")
                if event.details and "old_inputs" in event.details:
                    lines.append(f"  - Old: {event.details['old_inputs']}")
                    lines.append(f"  - New: {event.details['new_inputs']}")
        elif event.change_type == "CONSTRAINT_EXPRESSION_CHANGED":
            if event.element_name:
                lines.append(f"- Constraint `{event.element_name}` expression changed (ID: {event.element_id})")
                lines.append(f"  - Old: {event.old_value}")
                lines.append(f"  - New: {event.new_value}")

    return "\n".join(lines)


def explain_impact(impact_result: ImpactResult, spec_v1: MappingSpec) -> str:
    """Generate human-readable explanation of impact."""
    structured: ImpactExplanationResult = explain_impact_structured(impact_result, spec_v1)
    lines = ["## Impact Analysis\n"]

    if structured.impacted:
        lines.append(f"### Impacted Variables ({len(structured.impacted)})\n")
        for item in structured.impacted:
            lines.append(f"- **{item.var_name}** (ID: {item.var_id})")
            path_str = " -> ".join(item.path_names)
            lines.append(f"  - Dependency path: {path_str}")
            if item.alternative_path_count > 0:
                lines.append(f"  - Alternative paths: {item.alternative_path_count} additional path(s) exist (diamond dependency)")
            lines.append(f"  - Reason: {item.reason}")

            if item.missing_inputs:
                missing_names = []
                for missing in item.missing_inputs:
                    if missing.ref_name:
                        missing_names.append(f"{missing.ref_name} ({missing.ref_id})")
                    else:
                        missing_names.append(missing.ref_id)
                lines.append(f"  - Missing inputs: {', '.join(missing_names)}")

            if item.missing_bindings:
                missing_descriptions = []
                for missing in item.missing_bindings:
                    if missing.ref_id.startswith("s:"):
                        if missing.ref_name:
                            missing_descriptions.append(f"source ID {missing.ref_id} (canonical name: {missing.ref_name})")
                        else:
                            missing_descriptions.append(f"source ID {missing.ref_id}")
                    else:
                        missing_descriptions.append(f"ID {missing.ref_id}")
                lines.append(f"  - Missing bindings: {', '.join(missing_descriptions)}")

            if item.ambiguous_bindings:
                ambiguous_descriptions = []
                for ambiguous in item.ambiguous_bindings:
                    if ambiguous.ref_id.startswith("s:"):
                        if ambiguous.ref_name:
                            ambiguous_descriptions.append(f"source ID {ambiguous.ref_id} (canonical name: {ambiguous.ref_name})")
                        else:
                            ambiguous_descriptions.append(f"source ID {ambiguous.ref_id}")
                    else:
                        ambiguous_descriptions.append(f"ID {ambiguous.ref_id}")
                lines.append(f"  - Ambiguous bindings: {', '.join(ambiguous_descriptions)} (multiple raw columns map to same source ID)")

            if item.missing_transform_refs:
                lines.append(f"  - Missing transform references: {', '.join(item.missing_transform_refs)}")
                if item.reason != "MISSING_TRANSFORM_REF":
                    lines.append("    (Node is non-executable due to missing transform)")
        lines.append("")
    else:
        lines.append("### Impacted Variables\n")
        lines.append("None.\n")

    if structured.unaffected:
        lines.append(f"### Unaffected Variables ({len(structured.unaffected)})\n")
        for var_id, var_name in structured.unaffected:
            lines.append(f"- {var_name} (ID: {var_id})")
        lines.append("")
    else:
        lines.append("### Unaffected Variables\n")
        lines.append("None.\n")

    return "\n".join(lines)


def format_path(path: List[str]) -> str:
    """Format a dependency path as human-readable text."""
    if len(path) == 1:
        return f"`{path[0]}`"
    if len(path) == 2:
        return f"`{path[0]}` -> `{path[1]}`"
    return " -> ".join(f"`{p}`" for p in path)
