# Context Summary: Mapping Kernel

## What We Are Building

A **minimal kernel** that proves a single invariant: given two versions of a `mapping_spec` (v1 and v2), the system can compute **exactly which derived outputs are impacted by the change** and explain **why**, *without executing any transforms*. This is a pure dependency graph + structural diff problem. The kernel treats `mapping_spec` as a compiler IR-explicit, typed, and diffable. Execution is downstream; explanation is upstream.

## Non-Goals (Explicit)

- No LLM calls
- No execution engine (no pandas transforms, no SAS runtime)
- No UI
- No database or server
- No SDTM semantics beyond toy naming
- No multi-domain orchestration
- No real clinical data requirements
- No "framework" or over-engineering

This is a kernel, not a product. It proves the invariant or fails loudly.

## Glossary

- **mapping_spec**: A structured JSON specification that defines source columns, derived variables, transforms, and dependencies. It is the contract between human intent and code. Versioned and diffable.

- **IR (Intermediate Representation)**: In the broader context, IRs describe raw datasets and target structures. For this kernel, `mapping_spec` *is* the IR-the canonical representation of transformation intent.

- **transform**: A symbolic operation identifier (e.g., `transform_id: "age_calculation"`). Not executable code. Just a label that indicates what kind of derivation is intended.

- **dependency**: An explicit reference from a derived variable to its inputs. Can reference `source:<column>` or `derived:<variable>`. Forms edges in the dependency graph.

- **amendment**: A change between mapping_spec v1 and v2. Examples: source column renamed, derivation logic changed, constraint tightened. Must affect some but not all outputs.

- **impact**: The set of derived variables that become invalid when a change occurs, plus the dependency paths that explain why.

## The Single Invariant

> Given `mapping_spec` v1 and v2, the system can determine *exactly which derived outputs are impacted by the change*, and explain *why*, without re-executing the transform.

This is a graph problem: build dependency graph, compute structural diff, walk affected subgraph, emit explanations.

## Open Questions (Proposed Defaults)

1. **What level of transform detail is needed?**  
   Default: `transform_id` is a symbolic string (e.g., "age_calculation", "ct_map"). No executable expressions. If transform_id changes, it's a change event.

2. **How to handle constraint changes?**  
   Default: Constraints are part of the spec. If a constraint changes, it may affect which variables are considered valid, but for v0 we focus on structural dependency impact.

3. **What if a source column is removed?**  
   Default: All derived variables that depend on it (directly or transitively) are impacted. The explanation path shows the dependency chain.

4. **What if only metadata changes (notes, review status)?**  
   Default: No impact on derived outputs. These are non-structural changes.

5. **How to represent "why" explanations?**  
   Default: Dependency paths as lists of variable names, rendered as human-readable text: "VAR_X is impacted because it depends on VAR_Y, which depends on SOURCE_COL, which was renamed."

## Contradictions / Duplicates Found in Docs

- **Multiple IR schemas mentioned**: `ir_v0` (for profiling raw data) vs `mapping_spec` (for transformation intent). For this kernel, we use `mapping_spec` only. IR_v0 is out of scope.

- **Transform execution vs. symbolic transforms**: Docs mention both executable transforms (pandas, SAS) and symbolic transform IDs. Kernel uses only symbolic-no execution.

- **SDTM focus vs. generic kernel**: Docs oscillate between SDTM-specific and generic transformation kernel. Kernel is generic; SDTM is just the proving ground via toy naming.

- **Amendment scenarios**: Multiple examples given (rename, logic change, constraint). Kernel supports all via structural diff, but we'll implement one concrete scenario first.

## Architecture Direction (Inferred)

The kernel consists of:
1. **spec.py**: Pydantic models for `mapping_spec` with strict validation
2. **diff.py**: Structural diff between v1 and v2 -> change events
3. **graph.py**: Build dependency graph (nodes = sources + derived, edges = depends_on)
4. **impact.py**: Compute impacted set from change events + graph
5. **explain.py**: Render human-readable explanations
6. **cli.py**: Command-line interface to run the analysis

All pure functions. File-based I/O. Deterministic.
