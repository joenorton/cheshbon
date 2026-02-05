# Kernel Contract

## Guarantees

### Impact Computation
Given two versions of a `mapping_spec` (v1 and v2), the kernel **guarantees**:
- Computes the exact set of derived variables that are impacted by changes
- Explains why each variable is impacted (dependency path and reason code)
- Identifies the exact set of unaffected variables
- All computation is deterministic and reproducible

### Change Detection
The kernel **guarantees**:
- Detects all structural changes (input references, transform_id, type)
- Distinguishes structural changes from metadata-only changes (renames)
- Emits normalized change events (not raw JSON diffs)
- Tracks unresolved references (missing inputs)

### Binding Validation
When provided with raw schema and bindings, the kernel **guarantees**:
- Identifies missing bindings for required source IDs
- Detects invalid bindings (bound columns not in schema)
- Reports unmapped raw columns (informational)
- Distinguishes binding issues from spec changes

### Explanation Quality
The kernel **guarantees**:
- Dependency paths show full chains (not just nodes)
- Impact reasons are explicit (DIRECT_CHANGE, TRANSITIVE_DEPENDENCY, MISSING_INPUT, MISSING_BINDING, TRANSFORM_IMPL_CHANGED, MISSING_TRANSFORM_REF)
- Missing inputs/bindings/transforms are listed with canonical names from spec
- Structured explanation primitives are complete and renderable (rendering occurs outside the kernel)
- Transform implementation changes explain what changed (file, module, etc.)

### Determinism
The kernel **guarantees**:
- Same inputs produce same outputs (no randomness, no heuristics)
- No execution of transforms required
- No data access required
- Pure functions with no side effects

## Explicit Exclusions

### No Execution
The kernel **does not**:
- Execute any transforms or data operations
- Access or read actual data files
- Perform runtime validation of transform logic
- Generate executable code

### No Heuristics
The kernel **does not**:
- Guess or infer relationships between entities
- Match names using similarity algorithms
- Infer bindings from column names
- Make assumptions about transform semantics

### No Execution Engine Integration
The kernel **does not**:
- Integrate with pandas, SAS, or any execution runtime
- Schedule or orchestrate transform execution
- Manage execution state or caching
- Provide execution order recommendations

### No Data Access
The kernel **does not**:
- Read from databases or data warehouses
- Access file systems
- Query external APIs or services
- Perform data profiling or sampling

### No UI or Workflow
The kernel **does not**:
- Provide a graphical user interface
- Manage approval workflows
- Track review status or assignments
- Send notifications or alerts

### No Domain Semantics
The kernel **does not**:
- Understand SDTM, CDISC, or clinical data standards
- Validate domain-specific business rules
- Enforce regulatory compliance
- Interpret transform semantics beyond structural dependencies

### No Multi-Domain Support
The kernel **does not**:
- Handle multiple source tables or domains
- Cross-reference dependencies across domains
- Orchestrate multi-domain transformations
- Manage domain-level versioning

### No LLM or AI
The kernel **does not**:
- Use large language models
- Perform natural language processing
- Generate explanations using AI
- Infer intent from unstructured text

## Scope Boundary

The kernel operates **exclusively** on:
- In-memory `mapping_spec` representations (v1 and v2)
- Optional in-memory `raw_schema` and `bindings` representations
- Pure structural analysis

The kernel produces **exclusively**:
- Change event lists
- Impact analysis results (impacted/unaffected sets)
- Structured explanation primitives (rendered outside the kernel)
- Dependency paths and reason codes
- Machine-first reports (core/full/all-details) derived from the same kernel semantics

All-details reports assert only analytical impact semantics under this contract; they make no claims about code execution, data correctness, or regulatory acceptance.

## Failure Modes

The kernel **will fail explicitly** (not silently) if:
- Spec inputs are invalid JSON or violate schema
- Input references point to non-existent IDs
- Transform references point to non-existent transforms (if registry provided)
- Dependency cycles exist (detected but not resolved)
- Required inputs are missing or unreadable
- Transform parameters contain floats or non-JSON types (canonicalization errors)

The kernel **will not fail** on:
- Missing optional metadata (notes, review status)
- Unmapped raw columns (reported, not errors)
- Non-impacting changes (correctly identified as unaffected)

### Known Gap: Type Incompatibility Detection

The kernel **currently does not detect** type incompatibility between raw columns and source columns:
- If a raw column's type changes incompatibly with its bound source column type (e.g., `date` -> `string`), this goes **silent**
- Input exists, binding exists, but type mismatch is not detected or reported
- This is a **binding-layer validation gap** that should be addressed in a future version

## Completeness Statement

This contract defines the complete scope of the kernel. Anything not explicitly guaranteed above is **out of scope**. The kernel is complete for its stated purpose: proving that change impact is computable without execution.
