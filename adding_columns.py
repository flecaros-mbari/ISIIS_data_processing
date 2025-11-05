import pandas as pd
import glob
from pathlib import Path
from tqdm import tqdm 

# ==== CONFIGURATION ====
csv_pattern = "/mnt/CFElab/Data_analysis/ISIIS/20240206_RachelCarson_detections/det_filtered/csv/*.csv"  # pattern to find CSV files
new_image_dir = None  # set to None if no change
new_crop_dir = None # "/mnt/CFElab/Data_analysis/ISIIS/20240206_RachelCarson_detections/det_filtered/crops"  # set to None if no change

# find all matching csv files
path_list = glob.glob(csv_pattern)

def make_elemental_id(row):
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

for csv_path in tqdm(path_list):
    df = pd.read_csv(csv_path)

    # ✅ drop crop_name if it exists
    if "crop_name" in df.columns:
        df = df.drop(columns=["crop_name"])

    # ✅ update paths if new directories are provided
    if "image_path" in df.columns and new_image_dir is not None:
        df["image_path"] = df["image_path"].apply(lambda x: str(Path(new_image_dir) / Path(x).name) if pd.notna(x) else x)

    if "crop_path" in df.columns and new_crop_dir is not None:
        df["crop_path"] = df["crop_path"].apply(lambda x: str(Path(new_crop_dir) / Path(x).name) if pd.notna(x) else x)

    # ✅ create elemental_id safely
    df["elemental_id"] = df.apply(make_elemental_id, axis=1)

    # ✅ copy class column into predicted_class
    if "class" in df.columns:
        df["predicted_class"] = df["class"]

    # ✅ save back to same csv
    df.to_csv(csv_path, index=False)
    print(f"Updated {csv_path}")
