"""
Shared gel-trap particle volume and carbon-flux estimation logic.

Used by volumes.py (bulk processing/export) and optimization.py
(flux-parameter fitting) so the underlying image-processing and flux
math is defined once instead of duplicated across both scripts.
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from skimage.io import imread
from skimage import filters
from skimage.morphology import binary_opening, disk
from scipy.ndimage import distance_transform_edt, binary_fill_holes
from scipy.optimize import minimize


# Canonical carbon-flux shape parameters (a, b) per particle class.
FLUX_PARAMS = {
    "aggregate": (0.113e-9, 0.81),
    "mini_pellet": (0.113e-9, 1),
    "rhizaria": (0.004e-9, 0.939),
    "phytoplankton": (0.288e-9, 0.811),
    "long_pellet": (0.113e-9, 1),
    "short_pellet": (0.113e-9, 1),
    "salp_pellet": (0.113e-9, 1),
}

# Particle classes that are intentionally excluded from flux calculations.
NON_FLUX_CLASSES = ("unidentifiable", "fiber", "swimmer")


# ------------------------- IMAGE PROCESSING -------------------------
def distmap_volume(boundary_image, show_distance=False, show_3D=False):
    """
    Calculate particle volume from a binary mask using distance transform.

    Returns the final estimated particle volume in pixels^3.
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
    volume = c1 * c2 * 2 * np.nansum(dist)
    return volume


def segmenting(image):
    """Threshold and denoise an image to create a binary mask."""
    threshold = filters.threshold_li(image)
    mask = image > threshold
    mask = binary_opening(mask, disk(3))
    return mask


def process_image_volume(image_path, show_threshold=False):
    """Read an image, segment it, and compute its particle volume."""
    image = imread(image_path, as_gray=True)
    binary_mask = segmenting(image)
    volume = distmap_volume(binary_mask, show_distance=False, show_3D=False)

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

    return volume


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


def process_directory_images(directory_path, metadata_file):
    """
    Process all *.tiff "JC" images in a directory to compute volumes and
    extract metadata.

    Returns:
        tuple: (image_paths, volumes, classes, elapsed_times, magnifications, frames)
    """
    files = [
        os.path.join(dirpath, f)
        for dirpath, _, filelist in os.walk(directory_path)
        for f in filelist if f.endswith(".tiff") and "JC" in f
    ]
    volumes = [process_image_volume(f) for f in files]
    classes = [get_folder_name(f) for f in files]
    fragments = [extract_fragments(f) for f in files]
    times, magnifications = zip(*[
        extract_elapsed_time_from_excel(f1, f2, metadata_file) for f1, f2 in fragments
    ]) if fragments else ((), ())
    frames = [1] * len(files)  # assuming 1 frame if unknown
    return files, volumes, classes, list(times), list(magnifications), frames


# ------------------------- FLUX CALCULATION -------------------------
def shape_to_flux(a, b, v, magnification, time, frames):
    """Convert particle volume to carbon flux."""
    area_image = 2048 * 2048

    if isinstance(time, list):
        time = time[0]

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
    return flux * 1e12  # Convert to mol C m-2 d-1


def flux_equations(classes, volumes, elapsed_times, magnifications, frames, params=FLUX_PARAMS):
    """Calculate total flux for a set of particles using class-specific parameters."""
    total_flux = 0
    for p_class, v, time, mag, frame in zip(classes, volumes, elapsed_times, magnifications, frames):
        if p_class in NON_FLUX_CLASSES:
            continue
        if p_class not in params:
            print("UNKNOWN PARTICLE TYPE", p_class)
            continue
        a, b = params[p_class]
        total_flux += shape_to_flux(a, b, v, mag, time, frame)
    return total_flux


def optimization_function(params, classes, volumes, elapsed_times, magnifications, frames, target_flux):
    """Objective function: absolute difference between calculated and target flux."""
    total_flux = flux_equations(classes, volumes, elapsed_times, magnifications, frames, params)
    return abs(total_flux - target_flux)


def optimize_flux_parameters(classes, volumes, elapsed_times, magnifications, frames, target_flux=8):
    """Optimize `a` and `b` parameters per class to match a target flux using L-BFGS-B."""
    param_names = list(FLUX_PARAMS.keys())
    initial_values = np.array([p for name in param_names for p in FLUX_PARAMS[name]])

    def flattened_func(flat_params):
        params = {param_names[i]: (flat_params[2 * i], flat_params[2 * i + 1]) for i in range(len(param_names))}
        return optimization_function(params, classes, volumes, elapsed_times, magnifications, frames, target_flux)

    result = minimize(flattened_func, initial_values, method='L-BFGS-B')
    return {param_names[i]: (result.x[2 * i], result.x[2 * i + 1]) for i in range(len(param_names))}


# ------------------------- FILE OUTPUT -------------------------
def save_to_excel(data, output_path):
    """Save processed particle data to Excel."""
    df = pd.DataFrame(data)
    df.to_excel(output_path, index=False)
    print(f"Results saved to {output_path}")
