"""Renders the JSON written by bench_throughput.py --json as a standalone SVG bar chart.

The SVG carries its own light and dark styles, so it renders in a README in both
GitHub themes. Repeated cases are averaged.

    python benchmarks/plot.py results.json benchmarks/results/throughput.svg
"""

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

LIBRARIES = ("aroyo", "pyarrow", "obstore")
PHASES = ("write", "read")

LIGHT = {"aroyo": "#2a78d6", "pyarrow": "#eb6834", "obstore": "#1baf7a"}
DARK = {"aroyo": "#3987e5", "pyarrow": "#d95926", "obstore": "#199e70"}

WIDTH = 760
BAR = 18
GAP = 2
GROUP_PAD = 26
LEFT = 70
RIGHT = 90
TOP = 64
BOTTOM = 56


@dataclass(frozen=True)
class Bar:
    phase: str
    library: str
    mib_per_second: float


def library(name: str) -> str:
    return name.split(" ")[0]


def phase(name: str) -> str:
    return name.split(" ")[1]


def bars(report: dict) -> list[Bar]:
    totals: dict[tuple[str, str], list[float]] = defaultdict(list)
    for result in report["results"]:
        totals[(phase(result["name"]), library(result["name"]))].append(result["mib_per_second"])
    return [
        Bar(item, lib, sum(totals[(item, lib)]) / len(totals[(item, lib)]))
        for item in PHASES
        for lib in LIBRARIES
        if totals.get((item, lib))
    ]


def nice_step(maximum: float) -> float:
    rough = maximum / 4
    magnitude = 10 ** len(str(int(rough))) / 10
    for factor in (1, 2, 2.5, 5, 10):
        if factor * magnitude >= rough:
            return factor * magnitude
    return 10 * magnitude


def render(report: dict) -> str:
    data = bars(report)
    libraries = [lib for lib in LIBRARIES if any(bar.library == lib for bar in data)]
    group_height = len(libraries) * (BAR + GAP) - GAP
    plot_height = len(PHASES) * group_height + (len(PHASES) - 1) * GROUP_PAD
    height = TOP + plot_height + BOTTOM
    plot_width = WIDTH - LEFT - RIGHT
    maximum = max(bar.mib_per_second for bar in data)
    step = nice_step(maximum)
    axis_max = step * (int(maximum / step) + 1)
    scale = plot_width / axis_max
    payload_mib = report["payload_bytes"] / (1024 * 1024)

    lines: list[str] = []
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" '
        f'viewBox="0 0 {WIDTH} {height}" font-family="-apple-system, BlinkMacSystemFont, '
        f'Segoe UI, Helvetica, Arial, sans-serif" font-size="12">'
    )
    series_light = "".join(f".s-{lib}{{fill:{LIGHT[lib]}}}" for lib in libraries)
    series_dark = "".join(f".s-{lib}{{fill:{DARK[lib]}}}" for lib in libraries)
    lines.append(
        "<style>"
        ".surface{fill:#ffffff}.ink{fill:#1f2328}.muted{fill:#59636e}.grid{stroke:#d1d9e0;stroke-width:1}"
        f"{series_light}"
        "@media (prefers-color-scheme: dark){"
        ".surface{fill:#0d1117}.ink{fill:#f0f6fc}.muted{fill:#9198a1}.grid{stroke:#3d444d}"
        f"{series_dark}"
        "}"
        "</style>"
    )
    lines.append(f'<rect class="surface" width="{WIDTH}" height="{height}" rx="8"/>')
    lines.append(
        f'<text class="ink" x="{LEFT}" y="24" font-size="15" font-weight="600">'
        f"Throughput, MiB/s of Arrow data (higher is better)</text>"
    )
    lines.append(
        f'<text class="muted" x="{LEFT}" y="42">{report["rows"]:,} rows, {payload_mib:,.0f} MiB per file, '
        f'{report["cpus"]} cpus, {report["machine"]}, pyarrow {report["pyarrow"]}, aroyo {report["aroyo"]}</text>'
    )

    tick = 0.0
    while tick <= axis_max + 1e-9:
        x = LEFT + tick * scale
        lines.append(f'<line class="grid" x1="{x:.1f}" y1="{TOP}" x2="{x:.1f}" y2="{TOP + plot_height}"/>')
        lines.append(
            f'<text class="muted" x="{x:.1f}" y="{TOP + plot_height + 18}" text-anchor="middle" '
            f'font-variant-numeric="tabular-nums">{tick:,.0f}</text>'
        )
        tick += step

    for group, item in enumerate(PHASES):
        y0 = TOP + group * (group_height + GROUP_PAD)
        lines.append(
            f'<text class="ink" x="{LEFT - 12}" y="{y0 + group_height / 2 + 4:.1f}" text-anchor="end" '
            f'font-weight="600">{item}</text>'
        )
        for row, lib in enumerate(libraries):
            bar = next((candidate for candidate in data if candidate.phase == item and candidate.library == lib), None)
            if not bar:
                continue
            y = y0 + row * (BAR + GAP)
            width = bar.mib_per_second * scale
            lines.append(
                f'<path class="s-{lib}" d="M{LEFT},{y} h{width - 4:.1f} a4,4 0 0 1 4,4 v{BAR - 8} '
                f'a4,4 0 0 1 -4,4 h-{width - 4:.1f} z"/>'
            )
            lines.append(
                f'<text class="ink" x="{LEFT + width + 8:.1f}" y="{y + BAR / 2 + 4:.1f}" '
                f'font-variant-numeric="tabular-nums">{bar.mib_per_second:,.0f}</text>'
            )

    legend_y = TOP + plot_height + 42
    x = LEFT
    for lib in libraries:
        lines.append(f'<rect class="s-{lib}" x="{x}" y="{legend_y - 9}" width="12" height="12" rx="2"/>')
        lines.append(f'<text class="ink" x="{x + 18}" y="{legend_y + 1}">{lib}</text>')
        x += 18 + 8 * len(lib) + 28
    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="JSON written by bench_throughput.py --json")
    parser.add_argument("target", type=Path, help="SVG to write")
    args = parser.parse_args()
    report = json.loads(args.source.read_text())
    args.target.parent.mkdir(parents=True, exist_ok=True)
    args.target.write_text(render(report))


if __name__ == "__main__":
    main()
