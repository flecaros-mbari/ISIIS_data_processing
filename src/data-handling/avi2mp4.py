import os
import subprocess
from pathlib import Path
import argparse
from tqdm import tqdm

def transcode_file(input_file: Path, output_file: Path, gop: int, verbose: bool = False):
    """Run ffmpeg to transcode the video."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if verbose:
        print(f"Transcoding: {input_file} → {output_file}")
    command = [
        "ffmpeg", "-y", "-i", str(input_file),
        "-g", str(gop), "-keyint_min", str(gop), "-sc_threshold", "0",
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-movflags", "faststart+frag_keyframe+empty_moov+default_base_moof",
        str(output_file)
    ]
    subprocess.run(command)

def main():
    parser = argparse.ArgumentParser(description="Recursively transcode AVI files to fast start MP4 with keyframes compatible with Tator. Retains directory structure.")
    parser.add_argument("--input", required=True, help="Input directory to search for MP4 files")
    parser.add_argument("--output", required=True, help="Output directory to store transcoded MP4s")
    parser.add_argument("--gop", type=int, default=30, help="GOP size (default: 30)")
    parser.add_argument("--verbose", action="store_true", help="Print each file being transcoded.")

    args = parser.parse_args()
    input_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()
    gop = args.gop
    verbose = args.verbose

    # Getting all the files
    input_files = [f for f in input_dir.rglob("*.avi") if f.stat().st_size > 0]

    if not input_files:
        print("No AVI files found or all are empty.")
        return

    print(f"Found {len(input_files)} AVI files to transcode.")

    # Progress with tqdm
    for input_file in tqdm(input_files, desc="Transcoding videos"):
        relative_path = input_file.relative_to(input_dir)
        output_file = output_dir / relative_path.with_suffix(".mp4")
        transcode_file(input_file, output_file, gop, verbose)

if __name__ == "__main__":
    main()
