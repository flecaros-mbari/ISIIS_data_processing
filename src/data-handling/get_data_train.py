import argparse
import os
import pandas as pd
import glob
from PIL import Image
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

from roi_crop import crop_roi

def find_image_by_timestamp(root_folder, media_name, timestamp):
    """Search for the image in directories that contain 'RachelCarson' and match the timestamp."""
    print(f"Searching for {media_name} with timestamp {timestamp} in RachelCarson folders...")

    # Search only in directories that contain the timestamp
    for dirpath, dirnames, filenames in os.walk(root_folder):
        if 'RachelCarson' in dirpath and timestamp in dirpath:
            if media_name in filenames:
                print(f"Found {media_name} in {dirpath}")
                return os.path.join(dirpath, media_name)
    
    print(f"{media_name} not found in RachelCarson folders with timestamp {timestamp}.")
    return None

def process_roi(row, image_folder, output_folder, idx, tator, ind):
    """Process a single ROI, crop it from the image, and save it in the appropriate class folder."""
    if tator:
        label = row['Label']
        depth = row['depth']
        media_name = row['media_name']
        x = row['x']
        y = row['y']
        width = row['width']
        height = row['height']
        timestamp = row['iso_datetime'].strftime('%Y-%m-%d')  # Extract the date portion for timestamp matching
            
        print(f"Processing ROI: {label}, Depth: {depth}, Media: {media_name}, Timestamp: {timestamp}")

        class_folder = os.path.join(output_folder, label)
        os.makedirs(class_folder, exist_ok=True)

        # Search for the image using the timestamp for faster lookup
        image_path = find_image_by_timestamp(image_folder, media_name, timestamp)
    else:
    
        label = row['class']
        depth_match = re.search(r'_(\d+(\.\d+)?)m\.jpg$', row["image_path"])  # Extracts depth
        if depth_match:
            depth = depth_match.group(1)  # This will contain the numeric depth value
            print(f"Extracted depth: {depth}")
        else:
            depth = None # Default if no depth is found in the filename
            print("Depth not found in image path.")

        media_name = row['image_path']
        x = row['x']
        y = row['y']
        width = row['w']
        height = row['h']
            
        print(f"Processing ROI: {label}, Depth: {depth}, Media: {media_name}")

        class_folder = os.path.join(output_folder, label)
        os.makedirs(class_folder, exist_ok=True)
        image_path =  media_name

    if image_path is None:
        print(f"Warning: Image {media_name} not found.")
        return
    
    if not os.path.exists(image_path):
        print(f"Warning: Image {image_path} does not exist.")
        return
    
    with Image.open(image_path) as img:
        img_width, img_height = img.size
        print(f"Opened image {media_name} with size: {img_width}x{img_height}")

        left = x * img_width
        top = y * img_height
        right = left + width * img_width
        bottom = top + height * img_height

        roi = crop_roi(img, left, top, right, bottom)
        if roi is None:
            print(f"Invalid coordinates in {image_path}, skipping...")
            return
        print(f"Cropped ROI from {int(left)},{int(top)} to {int(right)},{int(bottom)}")

        if not tator:
            roi_filename = f"{depth}m_{idx}_{ind}.png"
        else:
            roi_filename = f"{depth}m_{idx}.png"

        roi_output_path = os.path.join(class_folder, roi_filename)
        roi.save(roi_output_path)
        print(f"Saved ROI: {roi_output_path}")

def filter_and_save_rois(df, image_folder, output_folder, max_workers=4, tator = True, ind = 0):
    """Filters ROIs by label, crops the ROI from the images, and saves them in class-named folders."""

    if tator:
        print("Converting iso_datetime to datetime format...")
        df['iso_datetime'] = pd.to_datetime(df['iso_datetime'])

        print("Filtering DataFrame by label and date range...")
        start_date = '2023-07-12'
        end_date = '2023-12-31'
        filtered_df = df[(df['Label'] != 'Unknown') & 
                        (df['iso_datetime'] >= start_date) & 
                        (df['iso_datetime'] <= end_date)]
        
        print(f"Filtered down to {len(filtered_df)} ROIs.")
    else:
        filtered_df = df


    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        print("Starting to process ROIs...")
        for idx, row in tqdm(filtered_df.iterrows(), total=len(filtered_df), desc="Processing ROIs"):
            futures.append(executor.submit(process_roi, row, image_folder, output_folder, idx, tator, ind))
        
        print("Waiting for threads to complete...")
        for future in tqdm(futures, desc="Waiting for threads"):
            future.result()
    
    print("All ROIs have been processed.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Crop ROIs from labeled data into class-named training folders."
    )
    parser.add_argument(
        "--tator",
        action="store_true",
        help="Use the Tator TSV export schema instead of the detection-CSV schema.",
    )
    parser.add_argument(
        "--tsv-path",
        default="isiis_labels.tsv",
        help="Tator TSV export path (used when --tator is set).",
    )
    parser.add_argument(
        "--image-folder",
        default=None,
        help="Directory of source images (default depends on --tator).",
    )
    parser.add_argument(
        "--output-folder",
        default="classes/",
        help="Directory to write class-named crop folders into.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Number of worker threads for ROI cropping.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    tator = args.tator

    if tator:
        image_folder = args.image_folder or "/Volumes/CFElab/Data_archive/Images/ISIIS/COOK/Videos2framesdepth/"
        df = pd.read_csv(args.tsv_path, sep='\t')
        filter_and_save_rois(df, image_folder, args.output_folder, max_workers=args.max_workers, tator=tator)
    else:
        image_folder = args.image_folder or "/Volumes/CFElab/Data_analysis/ISIIS/detections20240821/det_filtered/csv/"
        files = glob.glob(image_folder + "*.csv")
        ind = 0
        for file in files:
            depth = re.search(r'_(\d+(\.\d+)?)m\.csv$', file)  # Match a number (integer or float) before 'm'
            if depth is not None:
                file_df = pd.read_csv(file, sep=',')
                filter_and_save_rois(file_df, image_folder, args.output_folder, max_workers=args.max_workers, tator=tator, ind=ind)
                ind += 1


if __name__ == "__main__":
    main()

