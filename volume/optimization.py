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
- logging
- multiprocessing
"""

import os
import numpy as np
import pandas as pd
from scipy.ndimage import distance_transform_edt, binary_fill_holes
from scipy.optimize import minimize
from skimage.io import imread
from skimage import filters
from skimage.morphology import binary_opening, disk
import matplotlib.pyplot as plt

# ============================
# DISTANCE TRANSFORM VOLUME
# ============================
def distmap_volume(boundary_image, show_distance=False, show_3D=False):
    """
    Compute the volume of a particle shape using a distance transform.

    Args:
        boundary_image (ndarray): Binary mask of particle (True inside object).
        show_distance (bool): If True, display intermediate distance transform images.
        show_3D (bool): If True, display a 3D plot of the distance transform.

    Returns:
        float: Volume of the particle in pixels^3.
    """
    # Compute distance transform (avoiding zeros)
    dist = distance_transform_edt(boundary_image) + 1
    # Fill holes to get solid object
    image_fill = binary_fill_holes(boundary_image)
    # Mask outside areas
    dist[~image_fill] = np.nan

    # Optional visualizations
    if show_distance:
        plt.figure(figsize=(10, 8))
        plt.subplot(2,2,1); plt.imshow(boundary_image, cmap='gray'); plt.title("Binary Boundary"); plt.axis('off')
        plt.subplot(2,2,2); plt.imshow(distance_transform_edt(boundary_image), cmap='cool'); plt.colorbar(label='Distance'); plt.axis('off')
        plt.subplot(2,2,3); plt.imshow(image_fill, cmap='gray'); plt.title("Filled Image"); plt.axis('off')
        plt.subplot(2,2,4); plt.imshow(dist, cmap='cool'); plt.colorbar(label='Distance'); plt.axis('off')
        plt.tight_layout(); plt.show()

    if show_3D:
        # 3D mirrored surface visualization
        rows, cols = dist.shape
        X, Y = np.meshgrid(np.arange(cols), np.arange(rows))
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111, projection='3d')
        surface = ax.plot_surface(X, Y, dist, cmap='viridis', edgecolor='none')
        ax.plot_surface(X, Y, -dist, facecolors=plt.cm.viridis((dist - np.nanmin(dist)) / (np.nanmax(dist) - np.nanmin(dist))), edgecolor='none')
        fig.colorbar(surface, ax=ax, label="Distance Value")
        ax.set_title("3D Distance Transform with Mirrored Surface")
        ax.set_xlabel("X"); ax.set_ylabel("Y"); ax.set_zlabel("Distance")
        max_dim = max(cols, rows, np.nanmax(dist))
        ax.set_xlim(0, max_dim); ax.set_ylim(0, max_dim); ax.set_zlim(-max_dim, max_dim)
        plt.show()

    # Compute volume using distance transform equations
    x = 4 * np.nansum(dist) - 2
    c1 = (x**2) / (x**2 + 2*x + 0.5)
    c2 = np.pi / 2
    volume = c1 * c2 * 2 * np.nansum(dist)
    return volume

# ============================
# SEGMENTATION
# ============================
def segmenting(image):
    """
    Convert a grayscale image to a binary mask using thresholding and morphological operations.

    Args:
        image (ndarray): Input grayscale image.

    Returns:
        ndarray: Binary mask with objects as True.
    """
    threshold = filters.threshold_li(image)
    binary_mask = image > threshold
    binary_mask = binary_opening(binary_mask, disk(3))  # Remove small noise
    return binary_mask

# ============================
# IMAGE VOLUME PROCESSING
# ============================
def process_image_volume(image_path, show_threshold=False):
    """
    Read an image, segment it, and calculate volume.

    Args:
        image_path (str): Path to image.
        show_threshold (bool): Show original and binary images.

    Returns:
        float: Volume of the particle in the image.
    """
    image = imread(image_path, as_gray=True)
    binary_image = segmenting(image)
    volume = distmap_volume(binary_image)

    if show_threshold:
        plt.figure(figsize=(12,6))
        plt.subplot(1,2,1); plt.imshow(image, cmap='gray'); plt.title("Original Image"); plt.axis('off')
        plt.subplot(1,2,2); plt.imshow(binary_image, cmap='gray'); plt.title("Binary Image"); plt.axis('off')
        plt.tight_layout(); plt.show()

    return volume

# ============================
# METADATA EXTRACTION
# ============================
def get_folder_name(image_path):
    """Get the parent folder name for a file path."""
    return os.path.basename(os.path.dirname(image_path))

def extract_fragments(file_path):
    """Extract fragments from file name for metadata purposes."""
    parts = os.path.basename(file_path).split('_')
    first_fragment = parts[0]
    second_fragment = parts[1] if len(parts) > 1 else None
    return first_fragment, second_fragment

def extract_elapsed_time_from_excel(first_fragment, second_fragment, excel_file):
    """
    Extract elapsed time and magnification from Excel metadata for a given trap.

    Returns:
        tuple: (elapsed_time, magnification value)
    """
    df = pd.read_excel(excel_file)
    filtered = df[df['Trap'] == first_fragment]
    elapsed_time = filtered['Elapsed_time'].iloc[0] if not filtered.empty else None
    second_val = filtered[second_fragment].iloc[0] if second_fragment in filtered.columns and not filtered.empty else None
    return elapsed_time, second_val

# ============================
# DIRECTORY PROCESSING
# ============================
def process_directory_images(directory_path, metadata_file):
    """
    Process all images in a directory to calculate volumes and extract metadata.

    Returns:
        tuple: volumes, classes, elapsed times, magnifications, frame counts
    """
    files = [
        os.path.join(dirpath, f)
        for dirpath, _, filelist in os.walk(directory_path)
        for f in filelist if f.endswith(".tiff") and "JC" in f
    ]
    volumes = [process_image_volume(f) for f in files]
    classes = [get_folder_name(f) for f in files]
    fragments = [extract_fragments(f) for f in files]
    times, magnifications = zip(*[extract_elapsed_time_from_excel(f1, f2, metadata_file) for f1, f2 in fragments])
    return volumes, classes, list(times), list(magnifications), [1]*len(files)  # assuming 1 frame if unknown

# ============================
# FLUX CALCULATIONS
# ============================
def shape_to_flux(a, b, v, magnification, time, frames):
    """
    Convert particle volume to carbon flux using class-specific parameters and magnification.
    """
    area_image = 2048*2048
    if magnification == "7x": area = 0.1268
    elif magnification == "115x": area = 2.179
    elif magnification == "32x": area = 0.59
    else: return 0
    carbon = (a*(1/area)**3 * v**b)/12.011
    flux = carbon / (area_image*(1/area)**2*time*frames)
    return flux * 1e12

def flux_equations(clas, volume, first, second, frames, params):
    """
    Calculate total flux for a list of particles using optimized parameters.
    """
    total_flux = 0
    for p_class, v, f, m, fr in zip(clas, volume, first, second, frames):
        if p_class in params:
            a, b = params[p_class]
            total_flux += shape_to_flux(a, b, v, m, f, fr)
    return total_flux

# ============================
# OPTIMIZATION
# ============================
def optimization_function(params, clas, volume, first, second, frames, target_flux):
    """
    Objective function: difference between calculated flux and target flux.
    """
    total_flux = 0
    param_names = list(params.keys())
    for p_class, v, f, m, fr in zip(clas, volume, first, second, frames):
        if p_class in params:
            a, b = params[p_class]
            total_flux += shape_to_flux(a, b, v, m, f, fr)
    return abs(total_flux - target_flux)

def optimize_flux_parameters(clas, volume, first, second, frames, target_flux=8):
    """
    Optimize `a` and `b` parameters per class to match target flux using L-BFGS-B.
    """
    initial_params = {
        'aggregate': [0.113e-9,0.81],
        'mini_pellet':[0.113e-9,1],
        'rhizaria':[0.004e-9,0.939],
        'phytoplankton':[0.288e-9,0.811],
        'long_pellet':[0.113e-9,1],
        'short_pellet':[0.113e-9,1],
        'salp_pellet':[0.113e-9,1]
    }
    initial_values = np.array([p for v in initial_params.values() for p in v])
    param_names = list(initial_params.keys())

    def flattened_func(flat_params):
        params = {param_names[i]: [flat_params[2*i], flat_params[2*i+1]] for i in range(len(param_names))}
        return optimization_function(params, clas, volume, first, second, frames, target_flux)

    result = minimize(flattened_func, initial_values, method='L-BFGS-B')
    optimized_params = {param_names[i]: [result.x[2*i], result.x[2*i+1]] for i in range(len(param_names))}
    return optimized_params

# ============================
# MAIN EXECUTION EXAMPLE
# ============================
if __name__ == "__main__":
    directory_path = "/Volumes/CFElab/Data_archive/Images/geltrap_microscopy/EXPORTS2/Classified_particles_fromModel_and_validated/"
    volume_results, clas, first, second, frames = process_directory_images(directory_path, "JC_trap_summary.xlsx")
    
    # Optimize parameters
    optimized_params = optimize_flux_parameters(clas, volume_results, first, second, frames, target_flux=0.008)
    print("Optimized parameters per class:", optimized_params)
    
    # Compute total flux using optimized parameters
    total_flux = flux_equations(clas, volume_results, first, second, frames, optimized_params)
    print(f"Optimized Total Flux: {total_flux} mol C m−2 d-1")