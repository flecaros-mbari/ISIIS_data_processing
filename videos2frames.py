import cv2
import os
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

def extract_frames(video_path, output_dir, frame_rate):
    """This function is to extract frames of a video in avi format with 
    a desired frame rate

    Args:
        video_path (str): path to the videos
        output_dir (str): path to the folder that you want to save the frames   
        frame_rate (int): frame rate to obtain the frames
    """    

    # Getting the videos
    video_name = os.path.splitext(os.path.basename(video_path))[0]

    # Create the new folder for the frames
    os.makedirs(output_dir, exist_ok=True)

    # Debugging
    print(f"Processing video: {video_name}")
    print(f"Output directory: {output_dir}")

    # Read the videos 
    cap = cv2.VideoCapture(video_path)
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    frame_interval = fps // frame_rate

    
    count = 0
    saved_frames = 0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # Looping into the videos to get the frames
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
    """This function creates the new path, reads the videos and call 
    the function to get the frames

    Args:
        params (str): path to the videos to get the frames
    """    

    # Get the path, relative path, frame rate and name of the video
    video_path, base_output_dir, frame_rate, input_dir = params

    # Generate the output directory for frames
    relative_path = os.path.relpath(video_path, input_dir)
    sub_dirs = relative_path.split(os.sep)

    #if any('rachelcarson' in dir.lower() for dir in sub_dirs):
    output_dir = os.path.join(base_output_dir, *sub_dirs[:-1])
    
    # Create the folder
    os.makedirs(output_dir, exist_ok=True)

    # Extract the frames
    extract_frames(video_path, output_dir, frame_rate)

def process_videos(input_dir, output_dir):
    """This function loops into all the videos to transform them to frames

    Args:
        input_dir (str): path which contains all the videos in avi
        output_dir (str): path to the new videos in frames
    """    

    # List with the video files
    video_files = []

    # Looping into the folders to get the videos to tranform
    print("Scanning for video files...")
    for root, dirs, files in tqdm(os.walk(input_dir), desc="Walking through directories"):
        for filename in files:
            if filename.endswith(".avi"):
                video_path = os.path.join(root, filename)
                video_files.append((video_path, output_dir, input_dir))
    print(f"Found {len(video_files)} video files to process.")

    # Using the cores of the computer
    num_cores = cpu_count() - 1
    print(f"Using {num_cores} cores for parallel processing.")

    # Tranforming the videos
    print("Starting conversion and frame extraction...")
    with Pool(num_cores) as pool:
        list(tqdm(pool.imap(process_video_file, video_files), total=len(video_files), desc="Processing videos"))
    print("Conversion and frame extraction completed.")

if __name__ == "__main__":
    input_directory = "/Volumes/CFElab/Data_archive/Images/ISIIS/RAW/20240821_RachelCarson/"
    output_directory = "/Volumes/CFElab/Data_archive/Images/ISIIS/COOK/Videos2framesnew/"
    frames_per_second = 1

    print(f"Input directory: {input_directory}")
    print(f"Output directory: {output_directory}")
    print(f"Frames per second: {frames_per_second}")

    process_videos(input_directory, output_directory, frames_per_second)
