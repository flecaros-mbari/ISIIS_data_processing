"""
Script: Gel Trap Particle Volume and Flux Calculation
Author: Fernanda Lecaros
Purpose:
    - Process microscopy images of gel trap particles.
    - Calculate particle volume using distance transform.
    - Extract metadata (elapsed time, magnification) from Excel.
    - Compute carbon flux per particle and save results to Excel.
"""

import argparse
import os

from volume_core import (
    process_image_volume,
    get_folder_name,
    extract_fragments,
    extract_elapsed_time_from_excel,
    shape_to_flux,
    FLUX_PARAMS,
    NON_FLUX_CLASSES,
    save_to_excel,
)


def process_directory(directory_path, metadata_excel):
    """Walk directory_path for *.tiff "JC" images and compute per-particle flux."""
    files_to_process = [
        os.path.join(dirpath, file)
        for dirpath, _, files in os.walk(directory_path)
        for file in files if file.endswith(".tiff") and "JC" in file
    ]

    processed_data = []

    for file_path in files_to_process:
        volume = process_image_volume(file_path)
        particle_class = get_folder_name(file_path)
        first_frag, second_frag = extract_fragments(file_path)
        elapsed_time, magnification = extract_elapsed_time_from_excel(first_frag, second_frag, metadata_excel)

        if particle_class in NON_FLUX_CLASSES:
            continue
        if particle_class not in FLUX_PARAMS:
            print("UNKNOWN PARTICLE TYPE", particle_class)
            continue

        a, b = FLUX_PARAMS[particle_class]
        flux = shape_to_flux(a, b, volume, magnification, elapsed_time, 1)

        processed_data.append({
            "Image Path": file_path,
            "Class": particle_class,
            "Volume (pixels^3)": volume,
            "A Parameter": a,
            "B Parameter": b,
            "Flux (mol C m-2 d-1)": flux,
            "Elapsed Time (days)": elapsed_time,
            "Magnification": magnification,
            "First Fragment": first_frag,
            "Second Fragment": second_frag
        })

    return processed_data


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute gel-trap particle volumes and carbon flux from microscopy images."
    )
    parser.add_argument(
        "--directory",
        default="/Volumes/CFElab/Data_archive/Images/geltrap_microscopy/EXPORTS2/Classified_particles_fromModel_and_validated/",
        help="Directory of classified particle images to process.",
    )
    parser.add_argument(
        "--metadata-excel",
        default="JC_trap_summary.xlsx",
        help="Excel file with per-trap elapsed time and magnification metadata.",
    )
    parser.add_argument(
        "--output-excel",
        default="processed_image_data.xlsx",
        help="Path to write the processed particle data.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    processed_data = process_directory(args.directory, args.metadata_excel)
    save_to_excel(processed_data, args.output_excel)

    total_flux = sum(
        item["Flux (mol C m-2 d-1)"] for item in processed_data if item["Flux (mol C m-2 d-1)"] is not None
    )
    print(f"Total Flux: {total_flux} mol C m-2 d-1")


if __name__ == "__main__":
    main()
