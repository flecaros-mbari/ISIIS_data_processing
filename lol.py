import os
import re

# Folder path
folder_path = '/Volumes/CFElab/Data_analysis/ISIIS/detections20240821/vizresults/'

# Get the list of files that end with 'm.jpg'
files = [f for f in os.listdir(folder_path) if f.endswith('m.jpg')]

# Regular expression to extract the depth from the file name
depth_pattern = re.compile(r'(\d+\.\d+)m\.jpg')

# List to store extracted depths
depths = []

# Extract depths from the file names
for file in files:
    match = depth_pattern.search(file)
    if match:
        depth = float(match.group(1))  # Convert depth to float
        depths.append(depth)

# Sort the depths
depths.sort()

# Count how many files fall into each range of 10
group_counts = {}

for depth in depths:
    group = int(depth // 10) * 10  # Range of 10 (0-10, 10-20, etc.)
    if group not in group_counts:
        group_counts[group] = 0
    group_counts[group] += 1

# Display the results
for group, count in sorted(group_counts.items()):
    print(f"{group}-{group+9}: {count}")
