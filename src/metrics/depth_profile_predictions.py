"""
Script: Depth Distribution Normalization and Plotting
Author: Fernanda Lecaros
Purpose:
    - Load a CSV with image paths and predicted labels.
    - Exclude unwanted classes.
    - Extract depth from the filename.
    - Bin depths into 10 m intervals.
    - Normalize counts by a provided reference dictionary.
    - Plot proportion of each class across depth ranges.
"""

import pandas as pd
import matplotlib.pyplot as plt
import re
import os

# ------------------------- CONFIG -------------------------
csv_path = "/Users/fernandalecaros/Downloads/images_csv"  # Replace with actual CSV path
output_dir = "./plots"  # Directory to save plots
excluded_classes = [
    'noise', 'bubble', 'football', "aggregate", "Unknown", "artifact",
    "phaeocystis", "crustacean", "chaetognath", "centric_diatom", "bloom"
]

depth_range_counts_by10 = {
    "0-10": 3, "10-20": 30, "20-30": 15, "30-40": 12, "40-50": 24,
    "50-60": 15, "60-70": 15, "70-80": 34, "80-90": 15, "90-100": 17,
    "100-110": 32, "110-120": 15, "120-130": 34, "130-140": 15, "140-150": 15,
    "150-160": 35, "160-170": 15, "170-180": 34, "180-190": 15, "190-200": 34,
    "200-210": 15, "210-220": 30, "220-230": 19, "230-240": 30, "240-250": 15,
    "250-260": 7, "260-270": 509, "270-280": 2765, "280-290": 1609, "290-300": 3067,
    "300-310": 1170, "310-320": 960, "320-330": 173, "330-340": 195, "340-350": 472,
    "350-360": 480
}

os.makedirs(output_dir, exist_ok=True)

# ------------------------- LOAD AND CLEAN DATA -------------------------
df = pd.read_csv(csv_path, sep=",")
df = df[~df['predicted_label'].isin(excluded_classes)]

# Extract depth from filename
def extract_depth(path):
    match = re.search(r'(\d+\.\d+)m', path)
    return float(match.group(1)) if match else None

df['depth'] = df['image_path'].apply(extract_depth)
df = df.dropna(subset=['depth'])

# ------------------------- BIN DEPTHS -------------------------
bins = list(range(0, int(df['depth'].max()) + 10, 10))
labels = [f"{bins[i]}-{bins[i+1]}" for i in range(len(bins) - 1)]
df['Depth Range'] = pd.cut(df['depth'], bins=bins, labels=labels, include_lowest=True)

print("Depth Range labels in the data:")
print(df['Depth Range'].unique())

# ------------------------- GROUP AND NORMALIZE -------------------------
grouped = df.groupby(['Depth Range', 'predicted_label']).size().unstack(fill_value=0)

# Map depth range counts from reference dictionary
depth_counts_mapped = grouped.index.to_series().map(depth_range_counts_by10)

# Check for missing keys
if depth_counts_mapped.isnull().any():
    missing = depth_counts_mapped[depth_counts_mapped.isnull()].index.tolist()
    print(f"Warning: Missing keys in depth mapping for: {missing}")
    # Optionally fill missing values with 1 to avoid division by NaN
    depth_counts_mapped = depth_counts_mapped.fillna(1)

normalized = grouped.div(depth_counts_mapped, axis=0)
print("Normalized data preview:")
print(normalized.head())

# ------------------------- PLOT -------------------------
for label in normalized.columns:
    if normalized[label].notnull().any():
        plt.figure(figsize=(10, 6))
        normalized[label].plot(kind='barh', color='skyblue')
        plt.title(f"Proportion of {label} by Depth Range")
        plt.xlabel("Proportion")
        plt.ylabel("Depth Range (m)")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plot_path = os.path.join(output_dir, f"{label}_depth_range.png")
        plt.savefig(plot_path)
        plt.show()
        print(f"Plot saved to {plot_path}")
    else:
        print(f"No data to plot for {label}")