"""Regenerate the technical report PDF from bench/results/*.json.

    uv run --group bench python -m bench.make_report_pdf

Writes docs/stepledger-report.pdf. Every number comes from the results files, through the same
gathering code as RESULTS.md (bench/report.py). "What beats it" comes before the wins.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from bench.report import MiB, gather

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "stepledger-report.pdf"
INK, INK2, GRID = colors.HexColor("#0b0b0b"), colors.HexColor("#52514e"), colors.HexColor("#e1e0d9")

styles = getSampleStyleSheet()
H1 = ParagraphStyle("h1", parent=styles["Heading1"], textColor=INK, fontSize=20, spaceAfter=8)
H2 = ParagraphStyle("h2", parent=styles["Heading2"], textColor=INK, fontSize=13, spaceBefore=10)
BODY = ParagraphStyle("body", parent=styles["BodyText"], textColor=INK, fontSize=9.5, leading=13)
SMALL = ParagraphStyle("small", parent=BODY, textColor=INK2, fontSize=8.5, leading=11)
CELL = ParagraphStyle("cell", parent=BODY, fontSize=7.5, leading=9.5)


def tbl(head: list[str], rows: list[list[Any]], widths: list[float] | None = None) -> Table:
    data = [[Paragraph(f"<b>{h}</b>", CELL) for h in head]]
    data += [[Paragraph(str(c), CELL) for c in r] for r in rows]
    t = Table(data, colWidths=widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("LINEBELOW", (0, 0), (-1, 0), 0.8, INK),
                ("LINEBELOW", (0, 1), (-1, -1), 0.3, GRID),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return t


def bullets(items: list[str]) -> list[Any]:
    return [Paragraph(f"&bull; {i}", BODY) for i in items]


def build(d: dict[str, Any]) -> list[Any]:
    s = d["summary"]
    chaos = {c["config"]: c for c in d["chaos"]["configs"]}
    mat, cost = d["materialize"], {m["mode"]: m for m in d["cost"]["modes"]}
    ov, env = d["overhead"], d["cliff"]["environment"]
    story: list[Any] = [
        Paragraph("Stepledger: technical report", H1),
        Paragraph(
            "Every LangGraph node running on Temporal, recorded once per Activity execution "
            "in your own Postgres, at any state size.",
            BODY,
        ),
        Spacer(1, 4),
        Paragraph(
            f"Measured on {env['platform']}; Python {env['python']}, temporalio "
            f"{env['temporalio']}, langgraph {env['langgraph']}, {env['temporal_cli']}, "
            "Postgres 16. Every number is read from bench/results/*.json; each file records "
            "the command that produced it.",
            SMALL,
        ),
        Paragraph("What beats it", H2),
    ]
    beats = [a for a in d["attacks"]["attacks"] if a["holds"] is False]
    story.append(
        tbl(
            ["id", "attack", "measured", "expected"],
            [[a["id"], a["name"], a["rate"], a["expected"]] for a in beats],
            [12 * mm, 45 * mm, 60 * mm, 55 * mm],
        )
    )
    story += bullets(
        [
            f"With the LLM journal on, {cost['journal']['wasted_calls']} billed model calls were still "
            "wasted: the worker died between the provider call and the journal write.",
            'With on_ledger_error="warn", a database outage leaves rows missing until reconcile '
            "repairs them from history.",
            "History still grows, linearly; unbounded runs still need continue-as-new.",
        ]
    )
    story += [Paragraph("Demo 1: the cliff", H2)]
    story.append(
        tbl(
            ["config", "nodes", "SDK default", "check disabled", "largest payload", "history MiB"],
            [
                [
                    f"{r['config']} {r['label']}",
                    r["nodes"],
                    f"{r['outcome']} {r.get('stopped_at') or ''}",
                    next(
                        x
                        for x in d["cliff"]["rows"]
                        if x["config"] == r["config"] and x["payload_check"] == "disabled"
                    )["outcome"],
                    f"{r['largest_payload_bytes']:,}",
                    f"{r['history_size_bytes'] / MiB:.2f}",
                ]
                for r in d["cliff"]["rows"]
                if r["payload_check"] == "sdk_default"
            ],
            [46 * mm, 14 * mm, 38 * mm, 28 * mm, 24 * mm, 20 * mm],
        )
    )
    story += [
        Paragraph("Demo 2: pull the plug", H2),
        tbl(
            ["config", "runs", "faults", "dup rows", "divergent", "lost", "orphan", "dup effects"],
            [
                [
                    c["config"],
                    c["runs"],
                    c["fault_injections"],
                    c["duplicate_rows"],
                    c["divergent_rows"],
                    c["lost_rows"],
                    c["orphan_rows"],
                    c["duplicate_side_effects"],
                ]
                for c in d["chaos"]["configs"]
            ],
        ),
    ]
    story += bullets(
        [
            f"Stepledger: every declared fault fired once "
            f"({chaos['SL']['fault_injections']}), {chaos['SL']['worker_restarts']} "
            "worker restarts, every run completed, each row checked against history."
        ]
    )
    story += [PageBreak(), Paragraph("Demo 3: the quiet quadratic", H2)]
    for name in ("history", "storage"):
        story.append(
            Image(
                str(ROOT / "bench" / "plots" / f"{name}-light.png"),
                width=175 * mm,
                height=175 * mm * 624 / 1760,
            )
        )
        story.append(Spacer(1, 4))
    story += bullets(
        [
            f"At 80 nodes x 100 KiB: {s['growth_store_mb_80']['B3_100']} MB as one object per payload, "
            f"{s['growth_store_mb_80']['B4_100']} MB as dedup chunks "
            f"({s['store_ratio_whole_blob_over_dedup']['100KiB_80nodes']}x).",
        ]
    )
    story += [Paragraph("Demo 4: the view equals the truth", H2)]
    story += bullets(
        [
            f"{mat['runs']} seeded runs: equal {mat['equal']}, declared gap "
            f"{mat['gap']}, unequal {mat['unequal']}. Cached continue-as-new runs with "
            f"chain=True: {mat['chain']['equal']} EXACT."
        ]
    )
    story += [
        Paragraph("Demo 5: the retry bill", H2),
        tbl(
            ["mode", "calls billed", "USD billed", "wasted calls", "wasted USD", "replays"],
            [
                [
                    m,
                    cost[m]["model_calls_billed"],
                    cost[m]["usd_billed"],
                    cost[m]["wasted_calls"],
                    cost[m]["wasted_usd"],
                    cost[m]["journal_replays"],
                ]
                for m in ("no-journal", "journal")
            ],
        ),
    ]
    story += [Paragraph("Overhead", H2)]
    story += bullets(
        [
            f"Ledger write p50 {ov['ledger_write_ms_p50']} ms, p95 "
            f"{ov['ledger_write_ms_p95']} ms; {ov['wall_overhead_per_node_ms']} ms of "
            "wall clock per node; marginal header bytes per node in history "
            f"{d['history_overhead']['marginal_header_bytes_per_node']} between 10, 20, 40 and 80 nodes."
        ]
    )
    story += [Paragraph("What holds under attack", H2)]
    holds = [a for a in d["attacks"]["attacks"] if a["holds"]]
    story.append(
        tbl(
            ["id", "attack", "measured"],
            [[a["id"], a["name"], a["rate"]] for a in holds],
            [12 * mm, 70 * mm, 90 * mm],
        )
    )
    return story


def main() -> None:
    d = gather()
    doc = SimpleDocTemplate(
        str(OUT),
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title="Stepledger report",
        author="Poojan Patel",
    )
    doc.build(build(d))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
