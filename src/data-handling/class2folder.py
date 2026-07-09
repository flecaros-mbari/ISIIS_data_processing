"""
Script: Organize particle crop images by predicted class

Description:
This script reads a CSV file containing particle information, including:
    - The predicted class of each particle (column: 'class')
    - The file path to the cropped image (column: 'crop_path')

It then:
    1. Fixes file paths to match the local machine mount point.
    2. Verifies that required columns exist in the CSV.
    3. Creates an output directory structure where each class has its own folder.
    4. Copies each crop image into the folder corresponding to its class.

The result is a dataset organized as:

    output_base/
        class_1/
            image1.png
            image2.png
        class_2/
            image3.png
            ...

Important:
- Images are copied (not moved).
- If two images share the same filename within a class, they may overwrite each other.
- The script assumes the CSV contains 'class' and 'crop_path' columns.
"""

# ==============================
# Import required libraries
# ==============================
import argparse
import os          # For file and directory operations
import shutil      # For copying files
import pandas as pd  # For reading and handling CSV data


def parse_args():
    parser = argparse.ArgumentParser(
        description="Copy class-predicted crop images into per-class folders."
    )
    parser.add_argument(
        "--folder",
        default="STT24-1",
        help="Dataset folder name under /Volumes/CFElab/Projects/STT_Traps/.",
    )
    parser.add_argument(
        "--csv-path",
        default=None,
        help="CSV with 'class' and 'crop_path' columns (default derived from --folder).",
    )
    parser.add_argument(
        "--output-base",
        default=None,
        help="Base output directory for class-organized folders (default derived from --folder).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    folder = args.folder

    # Path to the CSV file containing particle metadata
    csv_path = args.csv_path or (
        f"/Volumes/CFElab/Projects/STT_Traps/{folder}/{folder}_Measured_particles_all-magnifications.csv"
    )

    # Base output directory where class-organized folders will be created
    output_base = args.output_base or (
        f"/Volumes/CFElab/Projects/STT_Traps/{folder}/{folder}_facebook_prediction_v1/"
    )

    # Read CSV into a pandas DataFrame
    df = pd.read_csv(csv_path)

    # Replace '/mnt/' with '/Volumes/' because:
    # - The CSV was likely generated on a server using '/mnt/'
    # - You are running this locally where the drive is mounted as '/Volumes/'
    df["crop_path"] = df["crop_path"].str.replace("/mnt/", "/Volumes/", regex=False)

    # Ensure the CSV contains the necessary columns
    required_columns = {"class", "crop_path"}
    if not required_columns.issubset(df.columns):
        raise ValueError(
            "The CSV must contain the columns 'class' and 'crop_path'."
        )

    # Iterate through each row of the DataFrame
    for _, row in df.iterrows():

        # Get class name (convert to string in case it's numeric)
        cls = str(row["class"])

        # Get image path
        img_path = row["crop_path"]

        # Create class-specific folder, e.g. output_base/aggregate/
        dst_dir = os.path.join(output_base, cls)
        os.makedirs(dst_dir, exist_ok=True)

        # Build full destination path
        filename = os.path.basename(img_path)
        dst_path = os.path.join(dst_dir, filename)

        # Copy image
        if os.path.exists(img_path):
            # Copy file while preserving metadata (timestamps, etc.)
            shutil.copy2(img_path, dst_path)
            print(f"Copied: {img_path} -> {dst_path}")
        else:
            # Warn if the image file does not exist
            print(f"Image does not exist: {img_path}")

    print("Done")


if __name__ == "__main__":
    main()