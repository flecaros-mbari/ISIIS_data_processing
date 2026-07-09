"""
Script Description:
===================
This script calculates the volumes of particle shapes from microscopy images and estimates
carbon flux based on particle class, volume, magnification, and elapsed time.
It also provides tools to optimize flux parameters (`a` and `b`) for different particle classes.

Key Features:
1. Converts grayscale images to binary masks using thresholding and morphological operations.
2. Calculates particle volumes using a distance transform-based method (`distmap_volume`).
3. Processes entire directories of images in batch.
4. Extracts metadata (elapsed time, magnification) from Excel or filename fragments.
5. Computes total carbon flux based on particle volumes and class-specific parameters.
6. Optimizes flux parameters (`a`, `b`) to match a target total flux using `scipy.optimize.minimize`.
7. Provides optional visualization for distance transforms, 3D plots, and thresholded images.

Dependencies:
- numpy
- pandas
- scipy
- scikit-image (skimage)
- matplotlib
"""

import argparse

from volume_core import (
    process_directory_images,
    flux_equations,
    optimize_flux_parameters,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Optimize per-class carbon-flux shape parameters (a, b) to match a target total flux."
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
        "--target-flux",
        type=float,
        default=0.008,
        help="Target total flux (mol C m-2 d-1) to fit parameters against.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    files, volume_results, clas, first, second, frames = process_directory_images(
        args.directory, args.metadata_excel
    )

    optimized_params = optimize_flux_parameters(
        clas, volume_results, first, second, frames, target_flux=args.target_flux
    )
    print("Optimized parameters per class:", optimized_params)

    total_flux = flux_equations(clas, volume_results, first, second, frames, optimized_params)
    print(f"Optimized Total Flux: {total_flux} mol C m-2 d-1")


if __name__ == "__main__":
    main()
