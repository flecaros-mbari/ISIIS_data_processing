"""
Script Description:
===================
This script processes a collection of CSV files containing particle detection or crop information. 
It performs the following operations on each CSV:

1. Removes unnecessary columns (e.g., 'crop_name').
2. Optionally updates image and crop file paths to new directories, keeping filenames intact.
3. Creates a unique identifier for each row called 'elemental_id', based on image and crop filenames.
4. Copies the 'class' column to a new column 'predicted_class' for downstream compatibility.
5. Saves the modified CSV back to the same location.

This script is useful for cleaning and standardizing CSVs before further analysis, model predictions, 
or database ingestion.

Dependencies:
- pandas
- glob
- pathlib
- tqdm
"""

import pandas as pd
import glob
from pathlib import Path
from tqdm import tqdm

# ==== CONFIGURATION ====
# Pattern to find all CSV files to process
csv_pattern = "/mnt/CFElab/Data_analysis/ISIIS/20240206_RachelCarson_detections/det_filtered/csv/*.csv"

# Optional: set new directories to update image and crop paths
# If None, paths will not be changed
new_image_dir = None
new_crop_dir = None

# ==== FIND ALL CSV FILES MATCHING THE PATTERN ====
# This will generate a list of all CSV file paths matching the specified pattern
path_list = glob.glob(csv_pattern)

# ==== FUNCTION TO CREATE A UNIQUE ELEMENTAL ID FOR EACH ROW ====
def make_elemental_id(row):
    """
    Create a unique identifier for each row based on the image and crop filenames.
    
    Logic:
    - If both image_path and crop_path exist, combine their base filenames (without extension)
    - Replace spaces with underscores
    - If only one exists, use that one
    - If neither exists, return 'unknown'
    """
    img_stem = Path(str(row["image_path"])).stem if pd.notna(row.get("image_path")) else ""
    crop_stem = Path(str(row["crop_path"])).stem if pd.notna(row.get("crop_path")) else ""
    
    if img_stem and crop_stem:
        return f"{img_stem}_{crop_stem}".replace(" ", "_")
    elif img_stem:
        return img_stem
    elif crop_stem:
        return crop_stem
    else:
        return "unknown"

# ==== PROCESS EACH CSV FILE ====
for csv_path in tqdm(path_list):
    # Read the CSV into a DataFrame
    df = pd.read_csv(csv_path)

    # --- DROP UNUSED COLUMN ---
    # If 'crop_name' exists, remove it
    if "crop_name" in df.columns:
        df = df.drop(columns=["crop_name"])

    # --- UPDATE IMAGE PATHS IF NEW DIRECTORY IS PROVIDED ---
    if "image_path" in df.columns and new_image_dir is not None:
        # Replace the existing image path with the new directory, keeping the original filename
        df["image_path"] = df["image_path"].apply(
            lambda x: str(Path(new_image_dir) / Path(x).name) if pd.notna(x) else x
        )

    # --- UPDATE CROP PATHS IF NEW DIRECTORY IS PROVIDED ---
    if "crop_path" in df.columns and new_crop_dir is not None:
        # Replace the existing crop path with the new directory, keeping the original filename
        df["crop_path"] = df["crop_path"].apply(
            lambda x: str(Path(new_crop_dir) / Path(x).name) if pd.notna(x) else x
        )

    # --- CREATE ELEMENTAL ID ---
    # Safely generate a unique identifier per row
    df["elemental_id"] = df.apply(make_elemental_id, axis=1)

    # --- COPY CLASS COLUMN TO PREDICTED_CLASS ---
    # For compatibility with downstream processing
    if "class" in df.columns:
        df["predicted_class"] = df["class"]

    # --- SAVE THE UPDATED CSV BACK TO THE SAME PATH ---
    df.to_csv(csv_path, index=False)

    # Print progress for user visibility
    print(f"Updated {csv_path}")