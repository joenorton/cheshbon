# What Cheshbon Will Never Do

This document explicitly lists what Cheshbon will **never** do, to prevent scope creep and maintain focus on the core mission: being a deterministic transformation ledger.

## Execution

**Cheshbon will never:**
- Compute row-level data
- Execute transforms on actual data
- Generate output datasets
- Perform data validation beyond schema structure

**Why:** Cheshbon is a **specification system**, not an execution engine. It proves impact and records decisions, but does not produce data outputs.

## Heuristics and Inference

**Cheshbon will never:**
- Infer renames (treats as add + remove)
- Guess semantics from names
- Auto-complete or suggest mappings
- "Smart" field matching

**Why:** All logic is embarrassingly literal. The system reports what it knows, not what it guesses.

## Mutation of Evidence

**Cheshbon will never:**
- Rewrite change objects
- Modify historical artifacts
- "Fix" or "normalize" stored files
- Regenerate narratives or hashes post-commit

**Why:** Evidence must remain immutable. Compatibility layers produce in-memory normalized views, but original bytes are never altered.

## Permissions and Authentication

**Cheshbon will never:**
- Implement user authentication
- Add role-based access control
- Enforce permissions on projects or environments
- Track user identities

**Why:** v0.x is single-user, local. Multi-user features belong in hosted products built on top of the core.

## Database Storage

**Cheshbon will never:**
- Use a database (SQL, NoSQL, or otherwise)
- Require database setup or migrations
- Store evidence in a database

**Why:** File-backed storage is deterministic, portable, and auditable. Workspaces are self-contained directories.

## Workflows and State Machines

**Cheshbon will never:**
- Implement approval workflows
- Add state machines for spec lifecycle
- Enforce business process rules
- Track workflow state

**Why:** Cheshbon records decisions, not processes. Workflow logic belongs in downstream products.

## CDISC/CDISC Outputs

**Cheshbon will never:**
- Generate CDISC-compliant outputs
- Emit SDTM/ADaM datasets
- Validate against CDISC standards
- Include CDISC-specific transforms

**Why:** Cheshbon is domain-agnostic. CDISC-specific features belong in specialized products built on the core.

## Graph Visualization Libraries

**Cheshbon will never:**
- Integrate react-flow, d3, or other graph libraries
- Add interactive graph editing
- Support drag-and-drop spec creation
- Provide graph layout algorithms beyond basic topological sorting

**Why:** The graph is a read-only projection. Complex visualization belongs in specialized tools.

## Code Editors

**Cheshbon will never:**
- Integrate Monaco or other code editors
- Provide JSON editing UI
- Support syntax highlighting for specs
- Add code completion for spec fields

**Why:** Specs are edited via forms, not code editors. JSON editing is a downstream feature.

## Timeline/Version Browsers

**Cheshbon will never:**
- Build a timeline UI component
- Add version comparison visualizations
- Provide diff viewers beyond basic text comparison

**Why:** A dropdown is sufficient for version selection. Complex version browsers are downstream features.

## Caching Layers

**Cheshbon will never:**
- Add Redis or other caching
- Cache computed results
- Optimize with memoization beyond pure functions

**Why:** All computations are on-demand and deterministic. Caching adds complexity without evidence benefit.

## Silent Failures

**Cheshbon will never:**
- Silently ignore errors
- Continue after validation failures
- Skip verification steps
- Hide compatibility issues

**Why:** Explicit failures with detailed reports are essential for auditability. Silent failures break trust.

## Non-Deterministic Features

**Cheshbon will never:**
- Use random number generation (except for synthetic data with fixed seeds)
- Depend on system time for logic (only for timestamps)
- Use filesystem ordering assumptions
- Rely on dictionary iteration order (Python 3.7+ preserves insertion order, but we sort explicitly)

**Why:** Determinism is essential for reproducibility and testing.

## What This Means

These constraints ensure Cheshbon remains:
- **Focused**: Core mission is clear and bounded
- **Auditable**: Every decision is explicit and verifiable
- **Embeddable**: Can be integrated into larger systems
- **Testable**: Deterministic behavior enables comprehensive testing

Downstream products (hosted services, execution engines, CDISC emitters) can add these features **on top of** the core, but the core itself remains pure and minimal.
