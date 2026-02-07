# Impact Analysis Report

Generated: 2026-02-07T02:19:02.448519+00:00

## Schema Lock

- Lock used (A): True
- Lock used (B): True
- Schema contract hash A: `168b0abf79dad1e84101a5f7b8efce5ab7cd80f083171f918709e30ea59c24a8`
- Schema contract hash B: `168b0abf79dad1e84101a5f7b8efce5ab7cd80f083171f918709e30ea59c24a8`
- Contract changed: False
- Provenance changed (e.g. created_by); contract unchanged.

## [!] Run Status: IMPACTED

### Summary

- **Change Events**: 3
  - Spec events: 1
  - Registry events: 2
- **Impacted Nodes**: 2
- **Unaffected Nodes**: 39
- **Missing Bindings**: 0
- **Missing Transforms**: 0

## Spec Changes

### Derived Transform Ref Changed

- `v:__t6__.label`: `t:1296e8a6f545713327120baf647835174f864f46ffe0d8676240661a66e74112` -> `t:fbd20e998b400b8fac9b2d770aa99f35b480890e5023f0b16cf5bba40fdd0e1d`
  - old: `label = "HIGH"` (derive)
  - new: `label = "LOW"` (derive)
  - old transform: `compute label = "HIGH" (derive)`
  - new transform: `compute label = "LOW" (derive)`

## Registry Changes

### Transform Added

- `t:fbd20e998b400b8fac9b2d770aa99f35b480890e5023f0b16cf5bba40fdd0e1d`
  - spec: `compute label = "LOW" (derive)`

### Transform Removed

- `t:1296e8a6f545713327120baf647835174f864f46ffe0d8676240661a66e74112`
  - spec: `compute label = "HIGH" (derive)`

## Impacted Variables

| ID | Name | Reason | Dependency Path | Value Change |
|----|------|--------|-----------------|--------------|
| `v:__t6__.label` | __t6__.label | DIRECT_CHANGE | v:__t6__.label | "HIGH" -> "LOW" |
| `v:sorted_high.label` | sorted_high.label | TRANSITIVE_DEPENDENCY | v:__t6__.label -(flow)-> v:sorted_high.label |  |

## Next Actions

- **Review impacted variables** and update downstream dependencies

---

## Detailed Explanations

## Changes Detected

- Transform removed: `t:1296e8a6f545713327120baf647835174f864f46ffe0d8676240661a66e74112` (version: None)
- Transform added: `t:fbd20e998b400b8fac9b2d770aa99f35b480890e5023f0b16cf5bba40fdd0e1d` (version: None)

## Impact Analysis

### Impacted Variables (2)

- **__t6__.label** (ID: v:__t6__.label)
  - Dependency path: v:__t6__.label
  - Reason: DIRECT_CHANGE
- **sorted_high.label** (ID: v:sorted_high.label)
  - Dependency path: v:__t6__.label -> v:sorted_high.label
  - Reason: TRANSITIVE_DEPENDENCY

### Unaffected Variables (39)

- __datasource__lb.LBDTC (ID: v:__datasource__lb.LBDTC)
- __datasource__lb.LBORRES (ID: v:__datasource__lb.LBORRES)
- __datasource__lb.LBSTRESN (ID: v:__datasource__lb.LBSTRESN)
- __datasource__lb.LBSTRESU (ID: v:__datasource__lb.LBSTRESU)
- __datasource__lb.LBTESTCD (ID: v:__datasource__lb.LBTESTCD)
- __datasource__lb.USUBJID (ID: v:__datasource__lb.USUBJID)
- __datasource__lb.VISITNUM (ID: v:__datasource__lb.VISITNUM)
- __t2__.LBDTC (ID: v:__t2__.LBDTC)
- __t2__.LBORRES (ID: v:__t2__.LBORRES)
- __t2__.LBSTRESN (ID: v:__t2__.LBSTRESN)
- __t2__.LBSTRESU (ID: v:__t2__.LBSTRESU)
- __t2__.LBTESTCD (ID: v:__t2__.LBTESTCD)
- __t2__.USUBJID (ID: v:__t2__.USUBJID)
- __t2__.VISITNUM (ID: v:__t2__.VISITNUM)
- __t3__.LBDTC (ID: v:__t3__.LBDTC)
- __t3__.LBORRES (ID: v:__t3__.LBORRES)
- __t3__.LBSTRESN (ID: v:__t3__.LBSTRESN)
- __t3__.LBSTRESU (ID: v:__t3__.LBSTRESU)
- __t3__.LBTESTCD (ID: v:__t3__.LBTESTCD)
- __t3__.USUBJID (ID: v:__t3__.USUBJID)
- __t3__.VISITNUM (ID: v:__t3__.VISITNUM)
- __t4__.A1C (ID: v:__t4__.A1C)
- __t4__.LBDTC (ID: v:__t4__.LBDTC)
- __t4__.LBORRES (ID: v:__t4__.LBORRES)
- __t4__.LBSTRESU (ID: v:__t4__.LBSTRESU)
- __t4__.LBTESTCD (ID: v:__t4__.LBTESTCD)
- __t4__.USUBJID (ID: v:__t4__.USUBJID)
- __t4__.VISITNUM (ID: v:__t4__.VISITNUM)
- __t5__.A1C (ID: v:__t5__.A1C)
- __t5__.LBDTC (ID: v:__t5__.LBDTC)
- __t5__.USUBJID (ID: v:__t5__.USUBJID)
- __t5__.VISITNUM (ID: v:__t5__.VISITNUM)
- __t6__.A1C (ID: v:__t6__.A1C)
- __t6__.LBDTC (ID: v:__t6__.LBDTC)
- __t6__.USUBJID (ID: v:__t6__.USUBJID)
- __t6__.VISITNUM (ID: v:__t6__.VISITNUM)
- sorted_high.A1C (ID: v:sorted_high.A1C)
- sorted_high.USUBJID (ID: v:sorted_high.USUBJID)
- sorted_high.VISITNUM (ID: v:sorted_high.VISITNUM)
