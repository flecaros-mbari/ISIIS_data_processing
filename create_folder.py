import os
import pandas as pd
import torch
import torchvision.transforms as T
from PIL import Image
import shutil
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import argparse

def parse_args():
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
    return parser.parse_args()

def process_roi(row, crops, roi_base_dir, device, min_area, require_m):
    try:
        image_path = row["image_path"]
        if float(row["area"]) < min_area:
            return
        if require_m and "m" not in os.path.basename(image_path):
            return

        class_name = row["class"]
        class_dir = os.path.join(roi_base_dir, class_name)
        os.makedirs(class_dir, exist_ok=True)

        if crops:
            # Copy existing crop
            src = row["crop_path"]
            img_name = os.path.basename(src)
            dst = os.path.join(class_dir, img_name)
            shutil.copy2(src, dst)
        else:
            # Compute ROI coordinates
            x1 = int(int(row["image_width"]) * float(row["x"]))
            y1 = int(int(row["image_height"]) * float(row["y"]))
            x2 = int(int(row["image_width"]) * float(row["xx"]))
            y2 = int(int(row["image_height"]) * float(row["xy"]))

            if x2 <= x1 or y2 <= y1:
                print(f"Invalid coordinates in {image_path}, skipping...")
                return

            with Image.open(image_path) as img:
                img = img.convert("RGB")
                transform = T.ToTensor()
                img_tensor = transform(img).to(device)
                roi_tensor = img_tensor[:, y1:y2, x1:x2]
                roi = T.ToPILImage()(roi_tensor.cpu())
                roi_filename = os.path.join(
                    class_dir, f"{os.path.basename(image_path)}_{x1}_{y1}_{x2}_{y2}.jpg"
                )
                roi.save(roi_filename)

    except Exception as e:
        print(f"Error with {row.get('image_path', 'unknown')}: {e}")

def main():
    args = parse_args()

    csv_folder = args.csv_dir
    crops = args.crops
    min_area = args.min_area
    require_m = args.require_depth

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    csv_files = [f for f in os.listdir(csv_folder) if f.endswith(".csv")]

    for csv_file in tqdm(csv_files, desc="Processing CSVs"):
        csv_path = os.path.join(csv_folder, csv_file)

        # Create the rois folder next to the csv file
        roi_base_dir = os.path.join(csv_folder, "rois")
        os.makedirs(roi_base_dir, exist_ok=True)

        df = pd.read_csv(csv_path)

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

        with ThreadPoolExecutor() as executor:
            list(
                tqdm(
                    executor.map(
                        lambda r: process_roi(r, crops, roi_base_dir, device, min_area, require_m),
                        df.to_dict(orient="records")
                    ),
                    total=len(df),
                    desc=f"Processing ROIs ({csv_file})"
                )
            )

    print("ROI processing completed.")

if __name__ == "__main__":
    main()
