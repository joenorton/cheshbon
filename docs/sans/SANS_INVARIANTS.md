# SANS ↔ Cheshbon Invariants

This document defines the immutable cryptographic and semantic rules governing the SANS bundle format and its ingestion into Cheshbon. These invariants ensure that Cheshbon remains a deterministic, audit-stable ledger.

---

## 1. Semantic Isolation of Identity (Logic)

*   **Invariant:** `transform_id` MUST be derivable solely from the content-addressed `spec` (containing `op` and `params`).
*   **No Implementation Leaks:** Inclusion of implementation-specific metadata (e.g., source code, file paths, engine version, or line numbers `loc`) in the `transform_id` is a violation of the contract.
*   **Decimal Stability:** Floating-point numbers are strictly FORBIDDEN in `params`. All decimals must be represented as strings (e.g., `"0.1"`) to prevent hash drift caused by varying CPU architectures or floating-point precision rules.

## 2. Wiring-only Step Identity (Application)

*   **Invariant:** `step_id` MUST be a deterministic hash of the `transform_id` and the ordered list of logical table names (`inputs` and `outputs`).
*   **DAG Stability:** `step_id` is an application-level identifier. It remains stable even if physical file paths change, provided the logical graph wiring remains identical.
*   **Order Sensitivity:** List ordering in `inputs` and `outputs` MUST be preserved exactly as provided in the SANS IR. Re-sorting these lists for "cleanliness" is forbidden, as input order may carry semantic meaning for the execution engine.

## 3. Execution Context Blindness (The Run)

*   **Invariant:** The `fingerprint` in `run.json` MUST be derivable solely from:
    1. The `plan.ir.json` SHA256.
    2. The ordered sequence of `(step_id, transform_id)`.
    3. The canonical hashes of input and output data files.
*   **Metadata Exclusion:** The `run_id` (UUID), `created_at` (timestamp), and system-specific paths MUST NOT contribute to the semantic fingerprint.
*   **Environment Agnosticism:** Two runs performed on different operating systems or in different directories MUST yield the same `fingerprint` if the logical plan and the data bits are identical.

## 4. Rejection over Inference

*   **Invariant:** Cheshbon is a **passive ledger**, not an active debugger. It MUST NOT attempt to "repair" a bundle (e.g., by inferring a missing transform specification from history).
*   **Failure over Guessing:** If a bundle is internally inconsistent (e.g., a hash mismatch or a missing `spec`), Cheshbon MUST reject the entire bundle.

## 5. Canonical Serialization

*   **Invariant:** All cryptographic signatures MUST be computed using the **Canonical JSON** standard:
    *   UTF-8 encoding.
    *   Keys sorted recursively (lexicographical).
    *   No extraneous whitespace (separators: `",", ":"`).
    *   Stable list ordering (preserve IR order).

---

## Violations and Audit Failure

Any modification to Cheshbon that violates these invariants renders the system semantically non-compliant and invalidates historical audit trails.
