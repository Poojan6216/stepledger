"""Plots for the README and report, from bench/results/*.json. Light and dark variants.

    uv run --group bench python -m bench.plots.make_plots

Writes bench/plots/{history,storage}-{light,dark}.png. Colors: the two leading slots of the
validated reference palette (blue, orange), stepped per mode and checked with the dataviz
validator for CVD separation and contrast in both modes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parent / "results"
MiB = 2**20

THEMES: dict[str, dict[str, str]] = {
    "light": {
        "surface": "#fcfcfb",
        "ink": "#0b0b0b",
        "ink2": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
        "blue": "#2a78d6",
        "orange": "#eb6834",
    },
    "dark": {
        "surface": "#1a1a19",
        "ink": "#ffffff",
        "ink2": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
        "blue": "#3987e5",
        "orange": "#d95926",
    },
}
KBS = [20, 60, 100]


def rows(name: str) -> list[dict[str, Any]]:
    return list(json.loads((RESULTS / f"{name}.json").read_text())["rows"])


def style_axes(ax: Any, t: dict[str, str]) -> None:
    ax.set_facecolor(t["surface"])
    ax.grid(axis="y", color=t["grid"], linewidth=1, linestyle="-")
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(t["axis"])
    ax.tick_params(colors=t["muted"], labelsize=9, length=0)
    ax.xaxis.label.set_color(t["ink2"])
    ax.yaxis.label.set_color(t["ink2"])


def line(
    ax: Any, xs: list[float], ys: list[float], color: str, t: dict[str, str], label: str
) -> None:
    ax.plot(
        xs,
        ys,
        color=color,
        linewidth=2,
        solid_capstyle="round",
        solid_joinstyle="round",
        label=label,
        zorder=3,
    )
    ax.plot(
        xs,
        ys,
        linestyle="none",
        marker="o",
        markersize=7,
        color=color,
        markeredgecolor=t["surface"],
        markeredgewidth=2,
        zorder=4,
    )


def end_label(ax: Any, x: float, y: float, text: str, t: dict[str, str], dy: float = 0) -> None:
    ax.annotate(
        text,
        (x, y),
        xytext=(6, dy),
        textcoords="offset points",
        va="center",
        fontsize=9,
        color=t["ink"],
    )


def history_figure(theme: str) -> Path:
    t = THEMES[theme]
    plain = {(r["kb_per_node"], r["nodes"]): r for r in rows("growth") if r["config"] == "B2"}
    naive = {(r["kb_per_node"], r["nodes"]): r for r in rows("growth_problem")}
    stored = {(r["kb_per_node"], r["nodes"]): r for r in rows("growth") if r["config"] == "B4"}
    gap = max(
        abs(plain[k]["history_size_bytes"] - naive[k]["history_size_bytes"])
        / naive[k]["history_size_bytes"]
        for k in plain
        if k in naive
    )
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.9), sharey=True, facecolor=t["surface"])
    for ax, kb in zip(axes, KBS, strict=True):
        style_axes(ax, t)
        done = sorted(
            (n, r["history_size_bytes"] / MiB)
            for (k, n), r in plain.items()
            if k == kb and r["status"] == "COMPLETED"
        )
        stops = {
            (r["stopped_at_node"], r["history_size_bytes"] / MiB, r["wall"])
            for (k, _), r in plain.items()
            if k == kb and r["status"] != "COMPLETED"
        }
        xs, ys = [p[0] for p in done], [p[1] for p in done]
        first_stop = sorted(stops)[:1]
        for x, y, _ in first_stop:
            xs.append(x)
            ys.append(y)
        line(ax, xs, ys, t["orange"], t, "No External Storage (B1 naive, B2 ledger only)")
        for x, y, wall in first_stop:
            ax.plot(
                [x],
                [y],
                marker="X",
                markersize=10,
                color=t["orange"],
                markeredgecolor=t["surface"],
                markeredgewidth=1.5,
                zorder=5,
            )
            why = "history over 50 MiB" if wall and "history" in wall else "a node input over 2 MiB"
            label = f"stopped at node {x}\n{why}"
            if y < 40:
                ax.annotate(
                    label,
                    (x, y),
                    xytext=(-10, 8),
                    textcoords="offset points",
                    ha="right",
                    va="bottom",
                    fontsize=8.5,
                    color=t["ink2"],
                )
            else:  # near the limit line: set the label in open space with a leader line
                ax.annotate(
                    label,
                    (x, y),
                    xytext=(91, y - 20),
                    textcoords="data",
                    ha="right",
                    va="top",
                    fontsize=8.5,
                    color=t["ink2"],
                    arrowprops={
                        "arrowstyle": "-",
                        "color": t["muted"],
                        "lw": 1,
                        "shrinkA": 2,
                        "shrinkB": 6,
                    },
                )
        sx = sorted(n for (k, n) in stored if k == kb)
        sy = [stored[(kb, n)]["history_size_bytes"] / MiB for n in sx]
        line(ax, sx, sy, t["blue"], t, "External Storage (B3 whole-blob, B4 dedup)")
        end_label(ax, sx[-1], sy[-1], f"{sy[-1]:.1f} MiB", t, dy=8)
        ax.axhline(50, color=t["muted"], linewidth=1)
        ax.annotate(
            "history limit 50 MiB",
            (2, 50),
            xytext=(0, 3),
            textcoords="offset points",
            fontsize=8,
            color=t["muted"],
        )
        ax.set_title(f"{kb} KiB of new output per node", fontsize=10, color=t["ink"], loc="left")
        ax.set_xlim(0, 92)
        ax.set_ylim(0, 58)
        ax.set_xlabel("nodes in the run")
    axes[0].set_ylabel("history size (MiB)")
    fig.suptitle(
        "Workflow history size per run",
        x=0.01,
        ha="left",
        fontsize=13,
        color=t["ink"],
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.91,
        "Without External Storage, history grows with the square of the run and the run "
        "stops at a wall; with it, history stays small and linear.\n"
        f"B1 and B2 match within {gap * 100:.1f}%; B3 and B4 histories are identical.",
        fontsize=9,
        color=t["ink2"],
        linespacing=1.5,
        va="top",
    )
    handles, labels = axes[0].get_legend_handles_labels()
    leg = fig.legend(
        handles,
        labels,
        loc="lower left",
        ncol=2,
        frameon=False,
        fontsize=9,
        bbox_to_anchor=(0.01, -0.02),
    )
    for text in leg.get_texts():
        text.set_color(t["ink"])
    fig.tight_layout(rect=(0, 0.06, 1, 0.84))
    out = HERE / f"history-{theme}.png"
    fig.savefig(out, dpi=160, facecolor=t["surface"])
    plt.close(fig)
    return out


def storage_figure(theme: str) -> Path:
    t = THEMES[theme]
    g = rows("growth")
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.9), facecolor=t["surface"])
    for ax, kb in zip(axes, KBS, strict=True):
        style_axes(ax, t)
        for cfg, key, color, label in (
            ("B3", "store_whole_blob_bytes", t["orange"], "One object per payload (B3, like S3)"),
            ("B4", "store_unique_chunk_bytes", t["blue"], "Stepledger dedup chunks (B4)"),
        ):
            pts = sorted(
                (r["nodes"], r[key] / 1e6)
                for r in g
                if r["config"] == cfg and r["kb_per_node"] == kb
            )
            xs, ys = [p[0] for p in pts], [p[1] for p in pts]
            line(ax, xs, ys, color, t, label)
            end_label(
                ax, xs[-1], ys[-1], f"{ys[-1]:.1f} MB" if ys[-1] < 100 else f"{ys[-1]:.0f} MB", t
            )
        ax.set_title(f"{kb} KiB of new output per node", fontsize=10, color=t["ink"], loc="left")
        ax.set_xlim(0, 96)
        ax.set_ylim(bottom=0)
        ax.set_xlabel("nodes in the run")
    axes[0].set_ylabel("stored per run (MB)")
    fig.suptitle(
        "External store bytes per run",
        x=0.01,
        ha="left",
        fontsize=13,
        color=t["ink"],
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.905,
        "Whole-payload objects grow with the square of the run; content-defined "
        "chunks stored once grow linearly. Each panel has its own scale.",
        fontsize=9,
        color=t["ink2"],
    )
    handles, labels = axes[0].get_legend_handles_labels()
    leg = fig.legend(
        handles,
        labels,
        loc="lower left",
        ncol=2,
        frameon=False,
        fontsize=9,
        bbox_to_anchor=(0.01, -0.02),
    )
    for text in leg.get_texts():
        text.set_color(t["ink"])
    fig.tight_layout(rect=(0, 0.06, 1, 0.88))
    out = HERE / f"storage-{theme}.png"
    fig.savefig(out, dpi=160, facecolor=t["surface"])
    plt.close(fig)
    return out


def main() -> None:
    plt.rcParams["font.family"] = ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"]
    for theme in THEMES:
        print(history_figure(theme))
        print(storage_figure(theme))


if __name__ == "__main__":
    main()
