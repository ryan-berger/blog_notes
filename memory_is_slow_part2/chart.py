#!/usr/bin/env python3
import argparse
import json
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np


@dataclass
class ChartSpec:
    # Required
    title: str
    x_label: Optional[str]
    y_label: str
    categories: List[str]                 # x-axis groups
    series: Dict[str, List[float]]        # {series_name: [values per category]}
    # Optional
    highlight_series: Optional[str] = None  # series name to color red
    outfile: Optional[str] = None           # if omitted, derived from title
    width: float = 0.2
    group_gap: float = 1.0
    rotate_xticks: int = 25

    def validate(self):
        if not self.categories:
            raise ValueError("`categories` must be a non-empty list.")
        n = len(self.categories)
        for k, v in self.series.items():
            if len(v) != n:
                raise ValueError(f"Series '{k}' has {len(v)} values but there are {n} categories.")
        if self.highlight_series and self.highlight_series not in self.series:
            raise ValueError(f"`highlight_series` '{self.highlight_series}' not found in `series`.")


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    text = re.sub(r"^-+|-+$", "", text)
    return text or "chart"


def plot_sketch_bar(spec: ChartSpec, dark_mode: bool):
    spec.validate()

    # Styles
    if dark_mode:
        bg_color = '#1e1e1e'
        text_color = 'white'
        bar_face = '#555555'
        edge_color = 'white'
    else:
        bg_color = 'white'
        text_color = 'black'
        bar_face = 'lightgrey'
        edge_color = 'black'

    series_names = list(spec.series.keys())
    x = np.arange(len(spec.categories))
    width = spec.width
    group_gap = spec.group_gap
    hatches = ['/', '.', 'x', 'o', '\\', '-', '+', '*']  # cycles if > len

    with plt.xkcd():
        fig, ax = plt.subplots(figsize=(8, 5))

        # backgrounds
        fig.patch.set_facecolor(bg_color)
        ax.set_facecolor(bg_color)

        # bars
        for i, name in enumerate(series_names):
            offsets = x * group_gap + (i - (len(series_names) - 1) / 2) * width * 1.1
            vals = spec.series[name]
            if spec.highlight_series and name == spec.highlight_series:
                ax.bar(offsets, vals, width,
                       color='red',
                       edgecolor=edge_color,
                       linewidth=1.5,
                       label=name)
            else:
                ax.bar(offsets, vals, width,
                       facecolor=bar_face,
                       edgecolor=edge_color,
                       hatch=hatches[i % len(hatches)],
                       linewidth=1,
                       label=name)

        # labels & title
        if len(spec.categories) == 1 and spec.categories[0] == "":
            # synthesized single category: hide x-axis ticks entirely
            ax.set_xticks([])
            ax.tick_params(axis='x', bottom=False, labelbottom=False)
        else:
            ax.set_xticks(x * group_gap)
            ax.set_xticklabels(spec.categories, rotation=spec.rotate_xticks, ha='right', color=text_color)

        if spec.x_label:
            ax.set_xlabel(spec.x_label, color=text_color)
        ax.set_ylabel(spec.y_label, color=text_color)
        ax.set_title(spec.title, color=text_color)

        legend = ax.legend(title='Series', facecolor=bg_color, edgecolor=edge_color)
        plt.setp(legend.get_texts(), color=text_color)
        if legend.get_title():
            plt.setp(legend.get_title(), color=text_color)

        # spine & ticks
        ax.tick_params(colors=text_color)
        for spine in ax.spines.values():
            spine.set_edgecolor(text_color)

        # remove xkcd text halo
        import matplotlib as mpl
        for text in fig.findobj(match=mpl.text.Text):
            text.set_path_effects([])

        plt.tight_layout()

        # save
        out = spec.outfile or f"{slugify(spec.title)}.png"
        plt.savefig(out, dpi=300)
        # Don’t block if many charts; users can open files
        plt.close(fig)


def load_chart_specs(path: str) -> List[ChartSpec]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, list):
        raise ValueError("Charts JSON must be a list of dicts.")

    specs: List[ChartSpec] = []
    for i, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"Item {i} is not a dict.")
        spec = ChartSpec(
            title=item.get("title", f"Chart {i+1}"),
            x_label=item.get("x_label"),
            y_label=item["y_label"],
            categories=item["categories"],
            series=item["series"],
            highlight_series=item.get("highlight_series"),
            outfile=item.get("outfile"),
            width=item.get("width", 0.2),
            group_gap=item.get("group_gap", 1.0),
            rotate_xticks=item.get("rotate_xticks", 25),
        )
        spec.validate()
        specs.append(spec)
    return specs


def main():
    p = argparse.ArgumentParser(description="Render xkcd-style grouped bar charts")
    p.add_argument('--dark', action='store_true', help="Use dark mode styling")
    p.add_argument('--charts-json', type=str, required=True,
                   help="Path to a JSON file containing a list of chart dicts")
    args = p.parse_args()

    specs = load_chart_specs(args.charts_json)
    for spec in specs:
        plot_sketch_bar(spec, dark_mode=args.dark)


if __name__ == '__main__':
    main()
