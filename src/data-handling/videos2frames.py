import cv2
import os
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import argparse

def extract_frames(video_path, output_dir, frame_rate):
    """Extract frames from a video at the desired frame rate."""
    video_name = os.path.splitext(os.path.basename(video_path))[0]
    os.makedirs(output_dir, exist_ok=True)
    print(f"Processing video: {video_name}")
    print(f"Output directory: {output_dir}")

    cap = cv2.VideoCapture(video_path)
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    if fps == 0:
        print(f"Warning: FPS is 0 for video {video_path}")
        return

    frame_interval = max(1, fps // frame_rate)
    count = 0
    saved_frames = 0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    with tqdm(total=total_frames, desc=f"Extracting frames from {video_name}", unit="frame") as pbar:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            if count % frame_interval == 0:
                frame_name = f"{video_name}_{saved_frames:04d}.jpg"
                frame_path = os.path.join(output_dir, frame_name)
                cv2.imwrite(frame_path, frame)
                saved_frames += 1
            count += 1
            pbar.update(1)

    cap.release()
    print(f"Extraction completed for {video_name}. {saved_frames} frames saved.")

def process_video_file(params):
    video_path, base_output_dir, input_dir, frames_per_second = params
    relative_path = os.path.relpath(video_path, input_dir)
    sub_dirs = relative_path.split(os.sep)
    output_dir = os.path.join(base_output_dir, *sub_dirs[:-1])
    os.makedirs(output_dir, exist_ok=True)
    extract_frames(video_path, output_dir, frames_per_second)

def process_videos(input_dir, output_dir, frames_per_second):
    video_files = []
    print("Scanning for video files...")
    for root, dirs, files in tqdm(os.walk(input_dir), desc="Walking through directories"):
        for filename in files:
            if filename.endswith(".mp4"):
                video_path = os.path.join(root, filename)
                video_files.append((video_path, output_dir, input_dir, frames_per_second))

    print(f"Found {len(video_files)} video files to process.")
    num_cores = max(1, cpu_count() - 1)
    print(f"Using {num_cores} cores for parallel processing.")

    with Pool(num_cores) as pool:
        list(tqdm(pool.imap(process_video_file, video_files), total=len(video_files), desc="Processing videos"))

    print("Conversion and frame extraction completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract frames from videos at a specified frame rate.")
    parser.add_argument('--input', required=True, help='Path to the input directory containing videos')
    parser.add_argument('--output', required=True, help='Path to the output directory for extracted frames')
    parser.add_argument('--fps', type=int, default=1, help='Number of frames to extract per second')

    args = parser.parse_args()

    print(f"Input directory: {args.input}")
    print(f"Output directory: {args.output}")
    print(f"Frames per second: {args.fps}")

    process_videos(args.input, args.output, args.fps)
