"""Cheshbon CLI: artifact-centric analysis commands."""

import argparse
import json
import sys
from importlib.metadata import version as get_version, PackageNotFoundError
from pathlib import Path
from typing import Optional


def main():
    """Main CLI entry point for cheshbon commands."""
    # Get version for --version argument (handle PackageNotFoundError)
    try:
        cheshbon_version = get_version("cheshbon")
    except PackageNotFoundError:
        cheshbon_version = "dev"
    
    parser = argparse.ArgumentParser(
        prog="cheshbon",
        description="Cheshbon: Deterministic impact analysis for clinical data mappings"
    )
    parser.add_argument("--version", action="version", version=f"cheshbon {cheshbon_version}")
    # Common arguments
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress all non-error output."
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # diff command
    diff_parser = subparsers.add_parser(
        "diff",
        help="Compute diff and impact analysis between two specs",
        parents=[parent_parser]
    )
    diff_parser.add_argument(
        "--from",
        dest="spec_v1",
        type=Path,
        required=True,
        help="Path to spec v1"
    )
    diff_parser.add_argument(
        "--to",
        dest="spec_v2",
        type=Path,
        required=True,
        help="Path to spec v2"
    )
    diff_parser.add_argument(
        "--registry",
        type=Path,
        default=None,
        help="Path to transform registry"
    )
    diff_parser.add_argument(
        "--registry-v1",
        type=Path,
        default=None,
        help="Path to registry v1 (for registry diff)"
    )
    diff_parser.add_argument(
        "--registry-v2",
        type=Path,
        default=None,
        help="Path to registry v2 (for registry diff)"
    )
    diff_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for reports (defaults to 'reports/')"
    )
    diff_parser.add_argument(
        "--report-mode",
        choices=["full", "core", "all-details", "off"],
        default="full",
        help="Report mode: full (markdown+json), core (summary json only), all-details (machine-first json), off (no report output)"
    )
    diff_parser.add_argument(
        "--bindings",
        type=Path,
        default=None,
        help="Path to bindings file (used for both v1 and v2)"
    )
    diff_parser.add_argument(
        "--from-bindings",
        dest="from_bindings",
        type=Path,
        default=None,
        help="Path to bindings file for from_spec"
    )
    diff_parser.add_argument(
        "--to-bindings",
        dest="to_bindings",
        type=Path,
        default=None,
        help="Path to bindings file for to_spec"
    )

    # graph-diff command
    graph_diff_parser = subparsers.add_parser(
        "graph-diff",
        help="Compute diff + impact between bundle graph.json artifacts",
        parents=[parent_parser]
    )
    graph_diff_parser.add_argument(
        "--bundle-a",
        type=Path,
        required=True,
        help="Path to bundle A directory"
    )
    graph_diff_parser.add_argument(
        "--bundle-b",
        type=Path,
        required=True,
        help="Path to bundle B directory"
    )
    graph_diff_parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output directory for graph_diff.json and impact.json"
    )

    # run-diff command
    run_diff_parser = subparsers.add_parser(
        "run-diff",
        help="Compute diff + impact between two SANS bundles (kernel pipeline)",
        parents=[parent_parser]
    )
    run_diff_parser.add_argument(
        "--bundle-a",
        type=Path,
        required=True,
        help="Path to bundle A directory"
    )
    run_diff_parser.add_argument(
        "--bundle-b",
        type=Path,
        required=True,
        help="Path to bundle B directory"
    )
    run_diff_parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output directory for impact.md and impact.json"
    )
    
    # verify command group
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verification commands"
    )
    verify_subparsers = verify_parser.add_subparsers(dest="verify_command", help="Available verify commands")

    # ingest command group
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingestion commands"
    )
    ingest_subparsers = ingest_parser.add_subparsers(dest="ingest_command", help="Available ingest commands")

    # ingest sans command
    ingest_sans_parser = ingest_subparsers.add_parser(
        "sans",
        help="Ingest SANS run bundle",
        parents=[parent_parser]
    )
    ingest_sans_parser.add_argument(
        "--bundle",
        type=Path,
        required=True,
        help="Path to SANS bundle directory"
    )
    ingest_sans_parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output directory for Cheshbon artifacts"
    )

    # verify report command (all-details verification)
    verify_report_parser = verify_subparsers.add_parser(
        "report",
        help="Verify all-details report against input artifacts",
        parents=[parent_parser]
    )
    # verify spec command (artifact validation)
    verify_spec_parser = verify_subparsers.add_parser(
        "spec",
        help="Verify spec artifact",
        parents=[parent_parser]
    )
    verify_spec_parser.add_argument(
        "spec_path",
        type=Path,
        help="Path to spec JSON"
    )
    verify_spec_parser.add_argument(
        "--registry",
        type=Path,
        default=None,
        help="Path to transform registry"
    )
    verify_spec_parser.add_argument(
        "--bindings",
        type=Path,
        default=None,
        help="Path to bindings file"
    )
    verify_spec_parser.add_argument(
        "--raw-schema",
        type=Path,
        default=None,
        help="Path to raw schema file"
    )
    verify_spec_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for report"
    )

    # verify registry command (artifact validation)
    verify_registry_parser = verify_subparsers.add_parser(
        "registry",
        help="Verify registry artifact",
        parents=[parent_parser]
    )
    verify_registry_parser.add_argument(
        "registry_path",
        type=Path,
        help="Path to registry JSON"
    )
    verify_registry_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for report"
    )

    # verify bindings command (artifact validation)
    verify_bindings_parser = verify_subparsers.add_parser(
        "bindings",
        help="Verify bindings artifact",
        parents=[parent_parser]
    )
    verify_bindings_parser.add_argument(
        "bindings_path",
        type=Path,
        help="Path to bindings JSON"
    )
    verify_bindings_parser.add_argument(
        "--spec",
        type=Path,
        default=None,
        help="Path to spec JSON (enables missing bindings checks)"
    )
    verify_bindings_parser.add_argument(
        "--raw-schema",
        type=Path,
        default=None,
        help="Path to raw schema file (enables raw column validation)"
    )
    verify_bindings_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for report"
    )

    # verify bundle command (SANS bundle verification)
    verify_bundle_parser = verify_subparsers.add_parser(
        "bundle",
        help="Verify SANS run bundle",
        parents=[parent_parser]
    )
    verify_bundle_parser.add_argument(
        "bundle_path",
        type=Path,
        help="Path to SANS bundle directory"
    )
    verify_bundle_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for report"
    )

    verify_report_parser.add_argument(
        "report_path",
        type=Path,
        help="Path to all-details report JSON"
    )
    verify_report_parser.add_argument(
        "--from",
        dest="spec_v1",
        type=Path,
        required=True,
        help="Path to spec v1"
    )
    verify_report_parser.add_argument(
        "--to",
        dest="spec_v2",
        type=Path,
        required=True,
        help="Path to spec v2"
    )
    verify_report_parser.add_argument(
        "--registry",
        type=Path,
        default=None,
        help="Path to transform registry (used for both v1 and v2)"
    )
    verify_report_parser.add_argument(
        "--registry-v1",
        type=Path,
        default=None,
        help="Path to registry v1"
    )
    verify_report_parser.add_argument(
        "--registry-v2",
        type=Path,
        default=None,
        help="Path to registry v2"
    )
    verify_report_parser.add_argument(
        "--bindings",
        type=Path,
        default=None,
        help="Path to bindings file (v2)"
    )
    verify_report_parser.add_argument(
        "--raw-schema",
        type=Path,
        default=None,
        help="Path to raw schema file (v2)"
    )
    verify_report_parser.add_argument(
        "--distance-check",
        choices=["sample", "strict"],
        default="sample",
        help="Distance verification mode for report verification"
    )
    verify_report_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for report (defaults to reports/)"
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)

    def _write_validation_result(result, output_dir: Optional[Path], filename: str) -> None:
        from ._internal.canonical_json import canonical_dumps

        result_dict = result.model_dump()
        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)
            report_out = output_dir / filename
            report_out.write_text(canonical_dumps(result_dict) + "\n", encoding="utf-8")
            if not args.quiet:
                print("[OK] Verification complete")
                print(f"  Report: {report_out}")
        else:
            if not args.quiet:
                status = "OK" if result.ok else "FAILED"
                print(f"[{status}] Verification complete")
        if not args.quiet:
            print(f"  Status: {'OK' if result.ok else 'FAILED'}")
            print(f"  Errors: {len(result.errors)}")
            print(f"  Warnings: {len(result.warnings)}")
        if not result.ok:
            sys.exit(1)
    
    if args.command == "diff":
        # Lazy import: only import run_diff (and kernel) when diff command is invoked
        from .diff import run_diff
        
        try:
            # Spec paths are required via argparse (required=True on --from and --to)
            spec_v1_path = Path(args.spec_v1).resolve()
            spec_v2_path = Path(args.spec_v2).resolve()
            
            # Validate bindings flags
            if args.bindings is not None and (args.from_bindings is not None or args.to_bindings is not None):
                print("Error: Cannot specify both --bindings and --from-bindings/--to-bindings. Use either a single --bindings file or both --from-bindings and --to-bindings.", file=sys.stderr)
                sys.exit(1)
            
            if (args.from_bindings is not None) != (args.to_bindings is not None):
                print("Error: Must provide both --from-bindings and --to-bindings, or neither.", file=sys.stderr)
                sys.exit(1)
            
            # Determine registry paths
            registry_v1_path = args.registry_v1
            registry_v2_path = args.registry_v2 or args.registry
            
            # Determine bindings paths (resolve if provided)
            bindings_path = args.bindings.resolve() if args.bindings else None
            from_bindings_path = args.from_bindings.resolve() if args.from_bindings else None
            to_bindings_path = args.to_bindings.resolve() if args.to_bindings else None
            
            output_dir: Optional[Path] = None
            if args.output_dir:
                output_dir = Path(args.output_dir).resolve()

            # If an output dir is specified, we want the content written to a file.
            # Otherwise, we want the content returned to us so we can print it to stdout.
            return_content = output_dir is None

            # Run diff
            exit_code, report_md, report_json = run_diff(
                spec_v1_path,
                spec_v2_path,
                output_dir=output_dir,
                registry_v1_path=registry_v1_path,
                registry_v2_path=registry_v2_path,
                bindings_path=bindings_path,
                from_bindings_path=from_bindings_path,
                to_bindings_path=to_bindings_path,
                return_content=return_content,
                report_mode=args.report_mode,
            )

            if return_content and not args.quiet and args.report_mode == "full":
                print(report_md)

            if not args.quiet:
                if args.report_mode == "off":
                    print("[OK] Diff analysis complete (report_mode=off)")
                else:
                    if output_dir:
                        print(f"[OK] Diff analysis complete")
                        if args.report_mode == "full":
                            print(f"  Markdown: {report_md}")
                        print(f"  JSON: {report_json}")
                        with open(report_json, 'r', encoding='utf-8') as f:
                            report = json.load(f)
                    else:
                        report = json.loads(report_json)

                    if report["run_status"] == "non_executable":
                        print(f"  Status: NON-EXECUTABLE (validation failed)")
                    elif report["run_status"] == "impacted":
                        print(f"  Status: IMPACTED ({report['summary']['impacted_count']} variables)")
                    else:
                        print(f"  Status: NO IMPACT")

            
            sys.exit(exit_code)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    elif args.command == "graph-diff":
        try:
            from .api import graph_diff_bundles
            from ._internal.canonical_json import canonical_dumps

            bundle_a = Path(args.bundle_a)
            bundle_b = Path(args.bundle_b)
            output_dir = Path(args.out)
            output_dir.mkdir(parents=True, exist_ok=True)

            diff, impact = graph_diff_bundles(bundle_a, bundle_b)

            diff_path = output_dir / "graph_diff.json"
            impact_path = output_dir / "impact.json"
            diff_path.write_text(canonical_dumps(diff.model_dump()) + "\n", encoding="utf-8")
            impact_path.write_text(canonical_dumps(impact.model_dump()) + "\n", encoding="utf-8")

            if not args.quiet:
                if diff.graph_a_sha256 and diff.graph_b_sha256 and diff.graph_a_sha256 == diff.graph_b_sha256:
                    print("graphs identical")
                print("[OK] Graph diff complete")
                print(f"  Diff: {diff_path}")
                print(f"  Impact: {impact_path}")
            sys.exit(0)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    elif args.command == "run-diff":
        try:
            from .adapters.sans_bundle import run_diff_from_bundles

            output_dir = Path(args.out)
            output_dir.mkdir(parents=True, exist_ok=True)

            md_content, json_content = run_diff_from_bundles(
                bundle_a=Path(args.bundle_a),
                bundle_b=Path(args.bundle_b),
            )

            md_path = output_dir / "impact.md"
            json_path = output_dir / "impact.json"
            md_path.write_text(md_content, encoding="utf-8")
            json_path.write_text(json_content, encoding="utf-8")

            if not args.quiet:
                print("[OK] Run diff complete")
                print(f"  Markdown: {md_path}")
                print(f"  JSON: {json_path}")
            sys.exit(0)
        except (FileNotFoundError, ValueError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    elif args.command == "verify" and args.verify_command == "report":
        try:
            from ._internal.report_doctor import run_doctor_report
            from ._internal.canonical_json import canonical_dumps

            report_path = Path(args.report_path).resolve()
            spec_v1_path = Path(args.spec_v1).resolve()
            spec_v2_path = Path(args.spec_v2).resolve()

            registry_v1_path = args.registry_v1 or args.registry
            registry_v2_path = args.registry_v2 or args.registry
            if (registry_v1_path is None) != (registry_v2_path is None):
                print("Error: Must provide both --registry-v1 and --registry-v2, or neither.", file=sys.stderr)
                sys.exit(1)

            bindings_path = args.bindings.resolve() if args.bindings else None
            raw_schema_path = args.raw_schema.resolve() if args.raw_schema else None

            if args.output_dir:
                output_dir = Path(args.output_dir).resolve()
            else:
                output_dir = Path.cwd() / "reports"
            output_dir.mkdir(parents=True, exist_ok=True)

            report = run_doctor_report(
                report_path=report_path,
                spec_v1_path=spec_v1_path,
                spec_v2_path=spec_v2_path,
                registry_v1_path=registry_v1_path,
                registry_v2_path=registry_v2_path,
                bindings_path=bindings_path,
                raw_schema_path=raw_schema_path,
                distance_check_mode=args.distance_check,
            )

            report_json = canonical_dumps(report)
            report_out = output_dir / "verify_report.json"
            report_out.write_text(report_json + "\n", encoding="utf-8")

            print("[OK] Report verification complete")
            print(f"  Report: {report_out}")
            print(f"  Status: {'OK' if report['ok'] else 'FAILED'}")
            print(f"  Clauses: {report['summary']['ok_clauses']}/{report['summary']['total_clauses']} passed")
            if not report['ok']:
                print(f"  Failed clauses: {', '.join(report['summary']['failed_clause_ids'])}")
                sys.exit(1)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    elif args.command == "verify" and args.verify_command == "spec":
        try:
            from ._internal.verify_artifacts import verify_spec

            spec_path = Path(args.spec_path).resolve()
            registry_path = args.registry.resolve() if args.registry else None
            bindings_path = args.bindings.resolve() if args.bindings else None
            raw_schema_path = args.raw_schema.resolve() if args.raw_schema else None

            output_dir = Path(args.output_dir).resolve() if args.output_dir else None

            result = verify_spec(
                spec=spec_path,
                registry=registry_path,
                bindings=bindings_path,
                raw_schema=raw_schema_path,
            )
            _write_validation_result(result, output_dir, "verify_spec.json")
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    elif args.command == "verify" and args.verify_command == "registry":
        try:
            from ._internal.verify_artifacts import verify_registry

            registry_path = Path(args.registry_path).resolve()
            output_dir = Path(args.output_dir).resolve() if args.output_dir else None

            result = verify_registry(registry=registry_path)
            _write_validation_result(result, output_dir, "verify_registry.json")
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    elif args.command == "verify" and args.verify_command == "bindings":
        try:
            from ._internal.verify_artifacts import verify_bindings

            bindings_path = Path(args.bindings_path).resolve()
            spec_path = args.spec.resolve() if args.spec else None
            raw_schema_path = args.raw_schema.resolve() if args.raw_schema else None
            output_dir = Path(args.output_dir).resolve() if args.output_dir else None

            result = verify_bindings(
                bindings=bindings_path,
                spec=spec_path,
                raw_schema=raw_schema_path,
            )
            _write_validation_result(result, output_dir, "verify_bindings.json")
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    elif args.command == "verify" and args.verify_command == "bundle":
        try:
            from .api import verify_sans_bundle

            bundle_path = Path(args.bundle_path).resolve()
            output_dir = Path(args.output_dir).resolve() if args.output_dir else None

            result = verify_sans_bundle(bundle_path)
            _write_validation_result(result, output_dir, "verify_bundle.json")
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    elif args.command == "ingest" and args.ingest_command == "sans":
        try:
            from .api import ingest_sans_bundle

            bundle_path = Path(args.bundle).resolve()
            output_dir = Path(args.out).resolve()

            ingest_sans_bundle(bundle_path, output_dir)
            if not args.quiet:
                print(f"[OK] Ingestion complete")
                print(f"  Artifacts: {output_dir}/cheshbon/")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    elif args.command == "verify":
        verify_parser.print_help()
        sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
