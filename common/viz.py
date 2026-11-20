"""Style commun des graphiques (notebooks matplotlib) — palette de référence validée.

Règles appliquées (voir README, section Visualisation) :
  * catégoriel : couleurs dans un ORDRE FIXE, 3 séries au plus par graphique (au-delà : petits multiples
    ou une seule couleur + étiquettes) ;
  * magnitude (classements) : une seule couleur ; heatmaps : dégradé bleu clair → foncé ;
  * écart à une référence (anomalies) : bleu ↔ rouge autour d'un gris neutre ;
  * traits fins, grille discrète, un seul axe Y, texte en encre neutre (jamais la couleur de la série).

Responsable : commun.
"""
from __future__ import annotations

import matplotlib as mpl
from matplotlib.colors import LinearSegmentedColormap

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE, ORANGE, AQUA = SERIES[0], SERIES[1], SERIES[2]
INK, INK_2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE, NEUTRAL = "#e1e0d9", "#c3c2b7", "#fcfcfb", "#f0efec"
WARM, COOL = "#e34948", "#2a78d6"  # pôles du divergent (chaud / froid)

SEQUENTIAL = LinearSegmentedColormap.from_list(
    "urbanhub_blue", ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"])
DIVERGING = LinearSegmentedColormap.from_list("urbanhub_div", [COOL, NEUTRAL, WARM])


def apply_style() -> None:
    mpl.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "figure.dpi": 110, "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"], "font.size": 10,
        "text.color": INK, "axes.labelcolor": INK_2, "axes.titlecolor": INK, "axes.titlesize": 12,
        "axes.titleweight": "semibold", "axes.titlelocation": "left",
        "axes.edgecolor": AXIS, "axes.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
        "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK_2, "ytick.labelcolor": INK_2,
        "lines.linewidth": 2.0, "lines.markersize": 6,
        "axes.prop_cycle": mpl.cycler(color=SERIES),
        "legend.frameon": False, "legend.labelcolor": INK_2,
    })
