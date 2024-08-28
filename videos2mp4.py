import cv2
import os
import ffmpeg
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

def convert_avi_to_mp4(video_path, output_dir):
    """This function transforms avi videos into mp4 format

    Args:
        video_path (str): Path to the videos in avi
        output_dir (str): Path to the new videos in mp4
    """    

    video_name = os.path.splitext(os.path.basename(video_path))[0]
    output_mp4 = os.path.join(output_dir, f"{video_name}.mp4")

    # Skip conversion if the MP4 file already exists
    if os.path.exists(output_mp4):
        return output_mp4

    # Convert AVI to MP4
    ffmpeg.input(video_path).output(output_mp4, vcodec='libx264').run()
    return output_mp4

def process_video_file(params):
    """This function creates the path to the new videos in mp4 and
    call the function to tranform the videos.

    Args:
        params (str): path of the video to transform
    """    

    # Get the path, relative path and name of the video
    video_path, base_output_dir, input_dir = params

    # Generate the output directory for the MP4 files
    relative_path = os.path.relpath(video_path, input_dir)
    sub_dirs = relative_path.split(os.sep)
    output_dir = os.path.join(base_output_dir, *sub_dirs[:-1])
    
    # Create the new path
    os.makedirs(output_dir, exist_ok=True)

    # Convert AVI to MP4 and save it in the output directory
    mp4_video_path = convert_avi_to_mp4(video_path, output_dir)

def process_videos(input_dir, output_dir):
    """This function loops into all the videos to transform

    Args:
        input_dir (str): path which contains all the videos in avi
        output_dir (str): path to the new videos in mp4
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
    input_directory = "//Volumes/CFElab/Data_archive/Images/ISIIS/RAW/"
    output_directory = "/Volumes/CFElab/Data_archive/Images/ISIIS/COOK/VideosMP4/"

    print(f"Input directory: {input_directory}")
    print(f"Output directory: {output_directory}")

    process_videos(input_directory, output_directory)
