"""
Script: Gel Trap Particle Volume and Flux Calculation
Author: Fernanda Lecaros
Purpose: 
    - Process microscopy images of gel trap particles.
    - Calculate particle volume using distance transform.
    - Extract metadata (elapsed time, magnification) from Excel.
    - Compute carbon flux per particle and save results to Excel.
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from skimage.io import imread
from skimage import filters
from skimage.morphology import binary_opening, disk
from scipy.ndimage import distance_transform_edt, binary_fill_holes


# ------------------------- IMAGE PROCESSING -------------------------
def distmap_volume(boundary_image, show_distance=False, show_3D=False):
    """
    Calculate particle volume from a binary mask using distance transform.
    
    Returns both final volume and sum of distances for intermediate checks.
    """
    dist = distance_transform_edt(boundary_image) + 1
    filled_image = binary_fill_holes(boundary_image)
    dist[~filled_image] = np.nan

    if show_distance:
        plt.figure(figsize=(10, 8))
        plt.subplot(2, 2, 1)
        plt.imshow(boundary_image, cmap='gray')
        plt.title("Binary Boundary Image")
        plt.axis('off')
        
        plt.subplot(2, 2, 2)
        plt.imshow(distance_transform_edt(boundary_image), cmap='cool')
        plt.colorbar(label='Distance')
        plt.title("Initial Distance Transform")
        plt.axis('off')
        
        plt.subplot(2, 2, 3)
        plt.imshow(filled_image, cmap='gray')
        plt.title("Filled Image")
        plt.axis('off')
        
        plt.subplot(2, 2, 4)
        plt.imshow(dist, cmap='cool')
        plt.colorbar(label='Distance')
        plt.title("Adjusted Distance Transform")
        plt.axis('off')
        plt.tight_layout()
        plt.show()

    if show_3D:
        rows, cols = dist.shape
        X, Y = np.meshgrid(np.arange(cols), np.arange(rows))
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111, projection='3d')
        surf = ax.plot_surface(X, Y, dist, cmap='viridis', edgecolor='none')
        ax.plot_surface(
            X, Y, -dist,
            facecolors=plt.cm.viridis((dist - np.nanmin(dist)) / (np.nanmax(dist) - np.nanmin(dist))),
            edgecolor='none'
        )
        fig.colorbar(surf, ax=ax, label="Distance")
        ax.set_title("3D Distance Transform with Mirrored Surface")
        ax.set_xlabel("X")
        ax.set_ylabel("Y")
        ax.set_zlabel("Distance")
        maximum = max(rows, cols, np.nanmax(dist))
        ax.set_xlim(0, maximum)
        ax.set_ylim(0, maximum)
        ax.set_zlim(-maximum, maximum)
        plt.show()

    x = 4 * np.nansum(dist) - 2
    c1 = (x**2) / (x**2 + 2 * x + 0.5)
    c2 = np.pi / 2
    volume_final = c1 * c2 * 2 * np.nansum(dist)
    return volume_final, 2 * np.nansum(dist)


def segmenting(image):
    """Threshold and denoise an image to create a binary mask."""
    threshold = filters.threshold_li(image)
    mask = image > threshold
    mask = binary_opening(mask, disk(3))
    return mask


def process_image_volume(image_path, show_threshold=False):
    """Compute particle volume from image path."""
    image = imread(image_path, as_gray=True)
    binary_mask = segmenting(image)
    volume_final, volume_sum = distmap_volume(binary_mask, show_distance=False, show_3D=False)

    if show_threshold:
        plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        plt.imshow(image, cmap='gray')
        plt.title("Original Image")
        plt.axis('off')

        plt.subplot(1, 2, 2)
        plt.imshow(binary_mask, cmap='gray')
        plt.title("Binary Image")
        plt.axis('off')
        plt.tight_layout()
        plt.show()

    return volume_final, volume_sum


def get_folder_name(image_path):
    """Return the folder name containing the image."""
    return os.path.basename(os.path.dirname(image_path))


# ------------------------- METADATA HANDLING -------------------------
def extract_fragments(file_path):
    """Split filename into fragments for Excel lookup."""
    parts = os.path.basename(file_path).split('_')
    return parts[0], parts[1] if len(parts) > 1 else None


def extract_elapsed_time_from_excel(first_fragment, second_fragment, excel_file):
    """Extract elapsed time and magnification from Excel metadata."""
    df = pd.read_excel(excel_file)
    filtered_row = df[df['Trap'] == first_fragment]

    elapsed_time_value = filtered_row['Elapsed_time'].iloc[0] if not filtered_row.empty else None
    second_fragment_value = (
        filtered_row[second_fragment].iloc[0] if second_fragment in filtered_row.columns and not filtered_row.empty else None
    )
    return elapsed_time_value, second_fragment_value


# ------------------------- FLUX CALCULATION -------------------------
def shape_to_flux(a, b, v, magnification, time, frames):
    """Convert particle volume to carbon flux."""
    area_image = 2048 * 2048

    if isinstance(time, list): time = time[0]

    # Pixels per micron
    if magnification == "7x":
        area = 0.1268
    elif magnification == "115x":
        area = 2.179
    elif magnification == "32x":
        area = 0.59
    else:
        print("No magnification found", magnification)
        return 0

    carbon = (a * (1 / area) ** 3 * v ** b) / 12.011
    flux = carbon / (area_image * (1 / area) ** 2 * time * frames)
    return flux * 1e12  # Convert to mol C m−2 d−1


def flux_equations(classes, volumes, elapsed_times, magnifications, frames):
    """Calculate total flux for a set of particles."""
    total_flux = 0
    for p_class, v, time, mag, frame in zip(classes, volumes, elapsed_times, magnifications, frames):
        a, b = 0, 0
        if p_class == 'aggregate': a, b = 0.113e-9, 0.81
        elif p_class == 'mini_pellet': a, b = 0.113e-9, 1
        elif p_class == 'rhizaria': a, b = 0.004e-9, 0.939
        elif p_class == 'phytoplankton': a, b = 0.288e-9, 0.811
        elif p_class in ('long_pellet', 'short_pellet', 'salp_pellet'): a, b = 0.113e-9, 1
        elif p_class in ('unidentifiable', 'fiber', 'swimmer'):
            continue
        else:
            print('UNKNOWN PARTICLE TYPE')
            continue
        total_flux += shape_to_flux(a, b, v, mag, time, frame)
    return total_flux


# ------------------------- FILE OUTPUT -------------------------
def save_to_excel(data, output_path):
    """Save processed particle data to Excel."""
    df = pd.DataFrame(data)
    df.to_excel(output_path, index=False)
    print(f"Results saved to {output_path}")


# ------------------------- MAIN PROCESS -------------------------
if __name__ == "__main__":
    directory_path = "/Volumes/CFElab/Data_archive/Images/geltrap_microscopy/EXPORTS2/Classified_particles_fromModel_and_validated/"
    metadata_excel = "JC_trap_summary.xlsx"
    output_excel = "processed_image_data.xlsx"

    files_to_process = [
        os.path.join(dirpath, file)
        for dirpath, _, files in os.walk(directory_path)
        for file in files if file.endswith(".tiff") and "JC" in file
    ]

    processed_data = []

    for file_path in files_to_process:
        volume_final, volume_initial = process_image_volume(file_path)
        particle_class = get_folder_name(file_path)
        first_frag, second_frag = extract_fragments(file_path)
        elapsed_time, magnification = extract_elapsed_time_from_excel(first_frag, second_frag, metadata_excel)

        # Determine particle parameters
        a, b = 0, 0
        if particle_class == 'aggregate': a, b = 0.113e-9, 0.81
        elif particle_class == 'mini_pellet': a, b = 0.113e-9, 1
        elif particle_class == 'rhizaria': a, b = 0.004e-9, 0.939
        elif particle_class == 'phytoplankton': a, b = 0.288e-9, 0.811
        elif particle_class in ('long_pellet', 'short_pellet', 'salp_pellet'): a, b = 0.113e-9, 1
        elif particle_class in ('unidentifiable', 'fiber', 'swimmer'):
            continue
        else:
            particle_class = "Unknown"
            print("UNKNOWN PARTICLE TYPE")
            continue

        flux = shape_to_flux(a, b, volume_initial, magnification, elapsed_time, 1)

        processed_data.append({
            "Image Path": file_path,
            "Class": particle_class,
            "Volume (pixels^3)": volume_final,
            "Initial Volume (pixels^3)": volume_initial,
            "A Parameter": a,
            "B Parameter": b,
            "Flux (mol C m−2 d-1)": flux,
            "Elapsed Time (days)": elapsed_time,
            "Magnification": magnification,
            "First Fragment": first_frag,
            "Second Fragment": second_frag
        })

    save_to_excel(processed_data, output_excel)

    # Optional: Total flux calculation
    total_flux = sum(item["Flux (mol C m−2 d-1)"] for item in processed_data if item["Flux (mol C m−2 d-1)"] is not None)
    print(f"Total Flux: {total_flux} mol C m−2 d-1")