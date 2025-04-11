import pandas as pd
import matplotlib.pyplot as plt


# Read the TSV file
df = pd.read_csv("/Users/fernandalecaros/Downloads/isiis_labels.tsv", sep="\t")

# Exclude specific classes
excluded_classes = ['noise', 'bubble', 'football', "aggreagate", "Unknown", "artifact", "phaeocystis", "crustacean", "chaetognath", "centric_diatom", "bloom"]
df = df[~df['Label'].isin(excluded_classes)]

# Define depth bins (0-100, 100-200, etc.) and label them
bins = list(range(0, int(df['depth'].max()) + 100, 100))
labels = [f"{bins[i]}-{bins[i+1]}" for i in range(len(bins) - 1)]

# Create a new column in the dataframe for the depth bins
df['Depth Range'] = pd.cut(df['depth'], bins=bins, labels=labels, include_lowest=True)

# Group by Depth Range and Label, and count occurrences
grouped = df.groupby(['Depth Range', 'Label']).size().unstack(fill_value=0)

# Define number of rows and columns for subplots based on number of labels
n_labels = len(grouped.columns)
n_cols = 3  # Number of columns (adjustable)
n_rows = (n_labels + n_cols - 1) // n_cols  # Calculate rows needed

# Create a large figure with multiple subplots (mini-plots)
fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, n_rows * 5))

# Flatten axes array for easy iteration (handles cases where n_rows or n_cols > 1)
axes = axes.flatten()

# Loop through each class and create a mini plot for each one
for i, label in enumerate(grouped.columns):
    ax = axes[i]
    # Plot data for the current class as a horizontal bar plot
    grouped[label].plot(kind='barh', ax=ax, color='skyblue')

    # Set the title and axis labels for each subplot
    ax.set_title(f"{label}")
    ax.set_xlabel("Count")
    ax.set_ylabel("Depth Range (m)")

    # Invert the Y-axis to imitate ocean depth (zero at the top)
    ax.invert_yaxis()

# Turn off any empty subplots if the number of labels is less than the grid size
for j in range(i + 1, len(axes)):
    fig.delaxes(axes[j])

# Adjust layout to prevent overlap
plt.tight_layout()

# Display the plot
plt.show()
