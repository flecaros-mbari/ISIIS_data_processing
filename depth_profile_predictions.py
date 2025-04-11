import pandas as pd
import matplotlib.pyplot as plt
import re

# Read the CSV file
df = pd.read_csv("/Users/fernandalecaros/Downloads/images_csv", sep=",")

# Exclude specific classes
excluded_classes = ['noise', 'bubble', 'football', "Unknown", "artifact", "phaeocystis", "crustacean", "chaetognath", "centric_diatom"]
df = df[~df['predicted_label'].isin(excluded_classes)]

# Extract depth from the image path
df['depth'] = df['image_path'].apply(lambda x: re.search(r'(\d+\.\d+)m', x).group(1) if re.search(r'(\d+\.\d+)m', x) else None)

# Convert depth to numeric (float), and drop NaNs if any
df['depth'] = pd.to_numeric(df['depth'], errors='coerce')
df = df.dropna(subset=['depth'])

# Define depth bins (0-10, 10-20, etc.) and labels
bins = list(range(0, int(df['depth'].max()) + 10, 10))
labels = [f"{bins[i]}-{bins[i+1]}" for i in range(len(bins) - 1)]

# Create a new column for depth bins
df['Depth Range'] = pd.cut(df['depth'], bins=bins, labels=labels, include_lowest=True)

# Print the Depth Range labels to ensure they match our dictionary keys
print("Generated Depth Range Labels:")
print(df['Depth Range'].unique())

# Group by Depth Range and predicted label, then count occurrences
grouped = df.groupby(['Depth Range', 'predicted_label']).size().unstack(fill_value=0)

# Check dictionary keys against DataFrame labels
depth_range_counts_by10 = {
    "0-10": 3,
    "10-20": 30,
    "20-30": 15,
    "30-40": 12,
    "40-50": 24,
    "50-60": 15,
    "60-70": 15,
    "70-80": 34,
    "80-90": 15,
    "90-100": 17,
    "100-110": 32,
    "110-120": 15,
    "120-130": 34,
    "130-140": 15,
    "140-150": 15,
    "150-160": 35,
    "160-170": 15,
    "170-180": 34,
    "180-190": 15,
    "190-200": 34,
    "200-210": 15,
    "210-220": 30,
    "220-230": 19,
    "230-240": 30,
    "240-250": 15,
    "250-260": 7,
    "260-270": 509,
    "270-280": 2765,
    "280-290": 1609,
    "290-300": 3067,
    "300-310": 1170,
    "310-320": 960,
    "320-330": 173,
    "330-340": 195,
    "340-350": 472,
    "350-360": 480
}

# Map depth ranges to counts and normalize
depth_counts_mapped = grouped.index.to_series().map(depth_range_counts_by10)

# Print to verify mapping, check for missing keys
print("Mapped depth counts for each range (check for NaNs):")
print(depth_counts_mapped)

# Normalize by the constant count for each depth range
normalized = grouped.div(depth_counts_mapped, axis=0)

# Print normalized data
print("Normalized data:")
print(normalized)

# Plot if data exists
for label in normalized.columns:
    if normalized[label].notnull().any():
        plt.figure(figsize=(10, 6))
        normalized[label].plot(kind='barh', color='skyblue')
        plt.title(f"Proportion of {label} by Depth Range")
        plt.xlabel("Proportion")
        plt.ylabel("Depth Range (m)")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(f"{label}_depth_range.png")
        plt.show()
    else:
        print(f"No data to plot for {label}")
