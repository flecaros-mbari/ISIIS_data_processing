"""
Script Description:
===================
This script processes regions of interest (ROIs) from images based on CSV files containing ROI information.
It can either crop ROIs from the original images or organize existing crop files into class-based folders.

Key Features:
1. Reads all CSV files in a specified directory.
2. Filters ROIs based on minimum area and optional depth requirements.
3. Creates a unique folder structure for each class and stores the ROIs there.
4. Supports copying existing crops or cropping new ROIs from images.
5. Processes ROIs in parallel using ThreadPoolExecutor for speed.

Dependencies:
- pandas
- PIL (Pillow)
- tqdm
- argparse
- shutil
"""

import os
import pandas as pd
from PIL import Image
import shutil
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import argparse

from roi_crop import crop_roi

# ==== ARGUMENT PARSING ====
def parse_args():
    """
    Define command-line arguments for the script.
    --csv-dir      : Directory containing CSV files with ROI information.
    --crops        : If set, assumes crops already exist and should be copied instead of cropped.
    --min-area     : Minimum area required for ROI to be processed (default 210 pixels^2).
    --require-depth: Only process ROIs whose image filenames contain 'm'.
    """
    parser = argparse.ArgumentParser(
        description="Crop ROIs or organize existing crops into class folders."
    )
    parser.add_argument(
        "--csv-dir",
        required=True,
        help="Directory containing the CSV files with ROI information."
    )
    parser.add_argument(
        "--crops",
        action="store_true",
        help="If set, assumes that the crops already exist and should be copied instead of cropping from images."
    )
    parser.add_argument(
        "--min-area",
        type=float,
        default=210,
        help="Minimum area required to process the ROI (default: 210)."
    )
    parser.add_argument(
        "--require-depth",
        action="store_true",
        help="Only process ROIs whose image filenames contain 'm'."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each ROI as it is copied/cropped and each one that is skipped."
    )
    return parser.parse_args()

# ==== ROI PROCESSING FUNCTION ====
def process_roi(row, crops, roi_base_dir, min_area, require_m, verbose=False):
    """
    Process a single ROI row from the CSV.

    If 'crops' is True:
        - Copies existing crop to the appropriate class folder.
    Else:
        - Crops ROI from the original image using coordinates and saves to class folder.

    Filters:
        - Skips ROIs smaller than min_area
        - Optionally skips ROIs if 'require_m' is True and filename doesn't contain 'm'
    """
    try:
        image_path = row["image_path"]
        if float(row["area"]) < min_area:
            if verbose:
                print(f"Skipping {image_path}: area below {min_area}")
            return
        if require_m and "m" not in os.path.basename(image_path):
            if verbose:
                print(f"Skipping {image_path}: missing depth suffix")
            return

        # Determine folder for the class
        class_name = row["class"]
        class_dir = os.path.join(roi_base_dir, class_name)
        os.makedirs(class_dir, exist_ok=True)

        if crops:
            # Copy existing crop file
            src = row["crop_path"]
            img_name = os.path.basename(src)
            dst = os.path.join(class_dir, img_name)
            shutil.copy2(src, dst)
            if verbose:
                print(f"Copied {src} -> {dst}")
        else:
            # Crop ROI from the original image
            x1 = int(row["image_width"]) * float(row["x"])
            y1 = int(row["image_height"]) * float(row["y"])
            x2 = int(row["image_width"]) * float(row["xx"])
            y2 = int(row["image_height"]) * float(row["xy"])

            with Image.open(image_path) as img:
                img = img.convert("RGB")
                roi = crop_roi(img, x1, y1, x2, y2)
                if roi is None:
                    print(f"Invalid coordinates in {image_path}, skipping...")
                    return
                roi_filename = os.path.join(
                    class_dir,
                    f"{os.path.basename(image_path)}_{int(x1)}_{int(y1)}_{int(x2)}_{int(y2)}.jpg"
                )
                roi.save(roi_filename)
                if verbose:
                    print(f"Saved ROI {roi_filename}")

    except Exception as e:
        print(f"Error with {row.get('image_path', 'unknown')}: {e}")

# ==== MAIN FUNCTION ====
def main():
    """
    Main workflow:
    1. Parse arguments
    2. Iterate through CSV files
    3. Process each ROI in parallel
    4. Save cropped images or copy existing crops to class folders
    """
    args = parse_args()

    csv_folder = args.csv_dir
    crops = args.crops
    min_area = args.min_area
    require_m = args.require_depth
    verbose = args.verbose

    # List CSV files in the folder
    csv_files = [f for f in os.listdir(csv_folder) if f.endswith(".csv")]

    for csv_file in tqdm(csv_files, desc="Processing CSVs"):
        csv_path = os.path.join(csv_folder, csv_file)

        # Create base folder for storing ROIs
        roi_base_dir = os.path.join(csv_folder, "rois")
        os.makedirs(roi_base_dir, exist_ok=True)

        # Read CSV into a DataFrame
        df = pd.read_csv(csv_path)

        # Keep only relevant columns
        keep_cols = [
            "image_path",
            "class",
            "score",
            "area",
            "saliency",
            "x",
            "y",
            "xx",
            "xy",
            "w",
            "h",
            "cluster",
            "image_width",
            "image_height",
        ]
        if crops:
            keep_cols.append("crop_path")
        df = df[keep_cols]

        # Process ROIs in parallel for speed
        with ThreadPoolExecutor() as executor:
            list(
                tqdm(
                    executor.map(
                        lambda r: process_roi(
                            r, crops, roi_base_dir, min_area, require_m, verbose
                        ),
                        df.to_dict(orient="records")
                    ),
                    total=len(df),
                    desc=f"Processing ROIs ({csv_file})"
                )
            )

    print("ROI processing completed.")

# ==== RUN SCRIPT ====
if __name__ == "__main__":
    main()