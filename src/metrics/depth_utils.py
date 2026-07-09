"""
Shared depth-analysis helpers used across metrics and data-handling
scripts, so the exclusion list, depth-from-filename parsing, and
depth-binning logic are defined once instead of drifting independently
across files.
"""

import re

import pandas as pd

# Noise/non-organism classes excluded before depth analysis. Reconciled
# from data-handling/depth_profile.py and metrics/depth_profile_predictions.py,
# which had independently drifted lists.
EXCLUDED_CLASSES = [
    "noise", "bubble", "football", "aggregate", "Unknown", "artifact",
    "phaeocystis", "crustacean", "chaetognath", "centric_diatom", "bloom",
]


def extract_depth_from_path(path):
    """
    Extract a depth value (in meters) embedded in a file path, e.g.
    ".../233.15m_0_6057.png" -> 233.15.

    Returns None if no depth suffix is found.
    """
    match = re.search(r"(\d+\.\d+)m", path)
    return float(match.group(1)) if match else None


def bin_by_depth(df, depth_col="depth", bin_width=10, range_col="Depth Range"):
    """
    Add a categorical depth-bin column to df, binning depth_col into
    bin_width-sized ranges starting at 0.

    Returns the same DataFrame with range_col added.
    """
    bins = list(range(0, int(df[depth_col].max()) + bin_width, bin_width))
    labels = [f"{bins[i]}-{bins[i + 1]}" for i in range(len(bins) - 1)]
    df[range_col] = pd.cut(df[depth_col], bins=bins, labels=labels, include_lowest=True)
    return df
