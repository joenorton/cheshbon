# cheshbon.graph v0.2 (SANS bundle output)

## overview

`cheshbon` ingests a SANS bundle and emits a deterministic lineage graph.

* `format`: `"cheshbon.graph"`
* `version`: `"0.2"`

## top-level fields

* `format`: string
* `version`: string
* `nodes`: list of node objects
* `edges`: list of edge objects

## node object

* `id`: typed id
  * `artifact:<name>` for files and provenance artifacts
  * `table:<name>` for logical datasets
* `name`: logical name
* `evidence`: optional object
  * `path`: bundle-relative path (forward slashes)
  * `sha256`: hex digest of the file bytes (or canonical hash when applicable)
  * `row_count`: optional integer (tables only)
  * `columns`: optional list of column names (tables only)

## edge object

* `id`: `step_id`
* `step_id`: application id from `plan.ir.json`
* `transform_id`: semantic transform id from `plan.ir.json`
* `op`: operation name from `plan.ir.json` (e.g. `compute`, `filter`, `select`, `sort`)
* `inputs`: list of `table:*` node ids
* `outputs`: list of `table:*` node ids

## construction notes

* artifact nodes come from `report.inputs` and `report.artifacts`
* table nodes come from `plan.ir.json` tables and step inputs/outputs
* nodes are emitted in lexicographic id order; edges follow plan step order
