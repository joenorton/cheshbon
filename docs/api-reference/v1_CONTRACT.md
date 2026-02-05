# Cheshbon v1.0 Contract

**status:** frozen at v1.0
**scope:** open-source core
**audience:** implementers, auditors, integrators

this document defines the **stable, non-negotiable contract** for Cheshbon v1.0.
anything not specified here is explicitly **out of scope** or **non-guaranteed**.

after v1.0, breaking this contract requires a major version bump.

---

## 1. scope and non-goals

### in scope

* deterministic specification of mapping logic
* immutable change evidence with verifiable hash chains
* impact analysis (direct + transitive)
* registry drift detection as evidence
* read-only projections (graphs, dashboards, diffs)
* append-only human review artifacts

### explicitly out of scope (v1.x)

* data execution or row-level computation
* CDISC or any standardized output generation
* permissions, auth, workflow states, approvals
* heuristics or inferred semantics
* mutation of historical evidence
* collaboration beyond append-only acknowledgments
* background jobs, caches, or state machines

cheshbon is a **ledger and analyzer**, not a runtime.

---

## 2. stable identifiers and versioning

### spec versions

* specs are versioned as `vNNN` (e.g. `v003`)
* version ordering is numeric, not timestamp-based
* filenames are storage keys only; APIs never expose filenames

### change identifiers

* `change_id` is semantic and stable:
  `chg:vNNN` where `vNNN` is `to_spec_version`
* `change_id` is **derived**, never inferred or parsed from filenames
* filenames remain numeric: `changes/vNNN.json`

### review identifiers

* reviews are append-only and local to a given project context
* path: `reviews/<change_id>/rev.001.json`
* review ids are not portable and are **not exported**

### schema versioning

* artifacts may include `schema_version`
* absence of `schema_version` implies legacy `0.6`
* v1.0 writers always emit `schema_version: "0.7"`

---

## 3. canonical json and hashing

### canonical serialization

* canonical serialization is used **only when writing new artifacts**
* rules:

  * sorted object keys
  * stable list ordering where defined
  * UTF-8
  * no whitespace beyond separators

### content_hash

* computed over canonical JSON
* **excluded field:** `content_hash` only
* included fields:

  * `parent_hash`
  * `audit_narrative`
  * all evidence fields

tampering with any evidence field invalidates `content_hash`.

### hash chains

* each change stores:

  * `parent_change_id`
  * `parent_hash`
* verification recomputes hashes; no stored state is trusted

---

## 4. artifact schemas (stable)

### spec

required:

* `schema_version`
* `sources`
* `derived`
* `bindings` (by reference)

rules:

* `transform_id` is the only valid transform reference
* no execution semantics

### change

required:

* `change_id`
* `from_spec_version`
* `to_spec_version`
* `canonical_spec`
* `spec_diff`
* `drift`
* `registry_snapshot`
* `registry_drift`
* `audit_narrative`
* `content_hash`

immutability:

* write-once
* never recomputed
* never mutated

### raw_schema

* header-only
* deterministic `schema_hash`
* no data rows

### bindings

* deterministic mappings
* no execution logic


### compatibility report

* deterministic ordering
* explicit actions: `accept | migrate | reject`
* never mutates bytes

### all-details report (analysis evidence)

* machine-first JSON report (core-compatible + witnesses)
* analysis-only: no execution, data correctness, or regulatory claims
* deterministic ordering; canonical JSON for digests

### error envelope

* only used on failure
* fields:

  * `error_code`
  * `message`
  * `details` (structured)

---

## 5. determinism requirements

the following must be deterministic for identical inputs:

* spec diff results
* impacted sets
* dependency paths
* registry drift detection
* audit narrative text
* synthetic project generation (seeded)

rules:

* no iteration over raw dicts
* all lists sorted explicitly
* all ids compared lexicographically unless specified otherwise

---

## 6. bundle tooling (out of scope)

Bundle export/import and bundle verification are intentionally out of scope for the kernel. They belong to higher‑level tooling outside the kernel.

---

## 8. performance and scaling posture

* benchmarks are part of the test suite
* budgets are enforced
* sentinel suite is gated (run with `--run-perf`)
* perf sentinels exercise the **core compute path** (no explanation rendering)
* UI scale handling is projection-only:

  * focus mode
  * depth truncation
* backend semantics are never altered for performance

performance failures block release.

---

## 9. API stability policy

v1.0 guarantees stability for:

* artifact schemas defined above
* change, graph, export/import, verify, health, review endpoints
* error envelope shape

new endpoints may be added; existing shapes will not change in v1.x.

### Python API Modules

**Stable modules (v1.0 contract):**
* `cheshbon.api` - Core functions (`diff`, `validate`) and result types (`DiffResult`, `ValidationResult`)
* `cheshbon.contracts` - Compatibility models (`CompatibilityIssue`, `CompatibilityReport`)

**Root exports (convenience, not contract):**
* Functions and types are also exported from `cheshbon` root for convenience
* Root exports are **not part of the v1.0 contract** and may be removed or changed without a major version bump
* For stable code, import from `cheshbon.api` and `cheshbon.contracts` explicitly

Example:
```python
# Stable (recommended)
from cheshbon.api import diff, DiffResult
from cheshbon.contracts import CompatibilityIssue

# Convenience (may change)
from cheshbon import diff, DiffResult, CompatibilityIssue
```

---

## 10. post-1.0 evolution rules

allowed in v1.x:

* new projections
* new append-only artifacts
* new UI views
* stricter verification modes (opt-in)

require v2.0:

* evidence schema changes
* hashing rule changes
* execution semantics
* mutation of historical artifacts

---

**cheshbon v1.0 is defined by this contract.
if it isn’t here, it isn’t guaranteed.**

---
