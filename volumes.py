import os
from glob import glob
from multiprocessing import Pool
import numpy as np
import logging
from skimage.io import imread
import pandas as pd
from skimage.measure import find_contours
from scipy.ndimage import label, distance_transform_edt, binary_fill_holes
import matplotlib.pyplot as plt
from skimage import filters
from skimage.morphology import binary_opening, disk

def distmap_volume(boundary_image, show_distance = False, show_3D = False):
    """
    Calculate the volume of a shape defined by its boundary in a binary image using the distance transform method.

    Args:
        boundary_image (ndarray): Binary image with the shape boundary, where pixels inside the object are `True` 
                                  and background pixels are `False`.
        show (bool): If True, displays the process with four subplots.

    Returns:
        float: Calculated volume of the shape based on distance transform values.
    """
    dist = distance_transform_edt(boundary_image) + 1  # Distance transform, avoiding zero values
    image_fill = binary_fill_holes(boundary_image)      # Fill holes to get a solid object shape
    dist[~image_fill] = np.nan                          # Mask outside areas in distance transform

    # Showing results
    if show_distance:
        plt.figure(figsize=(10, 8))
        
        plt.subplot(2, 2, 1)
        plt.title("Binary Boundary Image")
        plt.imshow(boundary_image, cmap='gray')
        plt.axis('off')
        
        plt.subplot(2, 2, 2)
        plt.title("Initial Distance Transform")
        plt.imshow(distance_transform_edt(boundary_image), cmap='cool')
        plt.colorbar(label='Distance')
        plt.axis('off')
        
        plt.subplot(2, 2, 3)
        plt.title("Filled Image")
        plt.imshow(image_fill, cmap='gray')
        plt.axis('off')
        
        plt.subplot(2, 2, 4)
        plt.title("Adjusted Distance Transform")
        plt.imshow(dist, cmap='cool')
        plt.colorbar(label='Distance')
        plt.axis('off')
        
        plt.tight_layout()
        plt.show()

    if show_3D:    
        """Plots the distance transform in 3D with a mirrored reflection."""
        rows, cols = dist.shape
        X, Y = np.meshgrid(np.arange(cols), np.arange(rows))
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111, projection='3d')
        surface = ax.plot_surface(X, Y, dist, cmap='viridis', edgecolor='none')
        ax.plot_surface(X, Y, -dist, facecolors=plt.cm.viridis((dist - np.min(dist)) / (np.max(dist) - np.min(dist))), edgecolor='none')
        fig.colorbar(surface, ax=ax, label="Distance Value")
        ax.set_title("3D Distance Transform Plot with Mirrored Surface")
        ax.set_xlabel("X coordinate")
        ax.set_ylabel("Y coordinate")
        ax.set_zlabel("Distance")
        maximum = max(cols, rows, np.nanmax(dist))
        ax.set_xlim(0, maximum)
        ax.set_ylim(0, maximum)
        ax.set_zlim(-maximum, maximum)
        plt.show()
    

    # Computing the volume 
    x = 4 * np.nansum(dist) - 2 # Equations from paper
    c1 = (x**2) / (x**2 + 2 * x + 1/2)
    c2 = np.pi / 2

    # Final volume in pixels**3
    volume_final = c1 * c2 * 2 * np.nansum(dist)
    
    return volume_final, 2 * np.nansum(dist)

def segmenting(image):
    """
    Segments the input image using thresholding and morphological operations.

    Args:
        image (ndarray): Input grayscale image.

    Returns:
        ndarray: Binary mask where segmented objects are True, background is False.
    """
    li = filters.threshold_li(image)
    binary_mask = image > li  # Apply thresholding
    binary_mask = binary_opening(binary_mask, disk(3))  # Morphological opening to remove noise
    return binary_mask

def process_image_volume(image_path, show_threshold=False):
    """
    Reads an image, converts it to binary, and computes the shape volume using `distmap_volume`.

    Args:
        image_path (str): Path to the input grayscale image file.
        show (bool): If True, displays the original and binary images.

    Returns:
        float: Calculated volume of the shape in the image.
    """
    # Reading image
    image = imread(image_path, as_gray=True)

    # Making the binary map
    binary_image = segmenting(image)

    # Volume 
    volume_final, volume = distmap_volume(binary_image)

    if show_threshold:
        plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        plt.imshow(image, cmap='gray')
        plt.title("Original Image")
        plt.axis('off')
        
        plt.subplot(1, 2, 2)
        plt.imshow(binary_image, cmap='gray')
        plt.title("Binary Image")
        plt.axis('off')
        
        plt.tight_layout()
        plt.show()

    return volume_final, volume

def get_folder_name(image_path):
    """Gets the folder name containing the image."""
    return os.path.basename(os.path.dirname(image_path))

def process_directory_images(directory_path, metadata):
    """
    Processes all image files in a given directory to calculate their volumes.

    Args:
        directory_path (str): Path to the directory containing images.

    Returns:
        tuple: Lists of volumes, class names, elapsed times, magnifications, and total frames.
    """
    # Walk through the directory and filter files with the desired conditions
    files_to_process = [
        os.path.join(dirpath, file)
        for dirpath, _, files in os.walk(directory_path)
        for file in files if file.endswith(".tiff") and "JC" in file
    ]
    
    # Process each image file and extract the required information
    volume_results = [process_image_volume(file)[0] for file in files_to_process]
    volume_initial = [process_image_volume(file)[1] for file in files_to_process]
    clas = [get_folder_name(file) for file in files_to_process]
    fragments = [extract_fragments(file) for file in files_to_process]
    
    # Extract elapsed times and frames from Excel for each fragment
    times, total_frames = zip(*[
        extract_elapsed_time_from_excel(first_fragment, second_fragment, metadata)
        for first_fragment, second_fragment in fragments
    ])
    
    # Extract magnification from the second fragment
    magnification = [second_fragment for _, second_fragment in fragments]

    return volume_results, volume_initial, clas, list(times), magnification, list(total_frames)


def extract_fragments(file_path):
    """Extracts specific parts of the file name as fragments."""
    parts = os.path.basename(file_path).split('_')
    first_fragment = parts[0]
    second_fragment = parts[1] if len(parts) > 1 else None
    return first_fragment, second_fragment

def extract_elapsed_time_from_excel(first_fragment, second_fragment, excel_file):
    """
    Extracts elapsed time for a specific trap from an Excel file, along with the value from the column specified by second_fragment.
    
    Parameters:
        first_fragment (str or int): The Trap value to filter the row.
        second_fragment (str): The column name to extract the value for.
        excel_file (str): Path to the Excel file.

    Returns:
        tuple: The elapsed time value and the value from the specified column for the specified trap, or None if not found.
    """
    # Load the Excel file into a DataFrame
    df = pd.read_excel(excel_file)
    
    # Filter the row where the 'Trap' column matches the first_fragment value
    filtered_row = df[df['Trap'] == first_fragment]
    
    # Extract the 'Elapsed_time' value for the filtered row, if it exists
    elapsed_time_value = filtered_row['Elapsed_time'].iloc[0] if not filtered_row.empty else None
    
    # Extract the value from the column specified by second_fragment, if it exists
    second_fragment_value = (
        filtered_row[second_fragment].iloc[0] if second_fragment in filtered_row.columns and not filtered_row.empty else None
    )
    return elapsed_time_value, second_fragment_value


def flux_equations(clas, volume, first_fragment, second_fragment, frames):
    """
    Calculates total flux based on particle class, volume, and time information.

    Args:
        clas (list): List of particle classes.
        volume (list): List of calculated volumes.
        first_fragment (list): List of elapsed time values.
        second_fragment (list): List of magnifications.

    Returns:
        float: Total flux calculated.
    """
    total_flux = 0

    for p_class, v, first, second, frames in zip(clas, volume, first_fragment, second_fragment, frames):
        a, b = 0, 0
        if p_class == 'aggregate': a, b = 0.113e-9, 0.81
        elif p_class == 'mini_pellet': a, b = 0.113e-9, 1
        elif p_class == 'rhizaria': a, b = 0.004e-9, 0.939
        elif p_class == 'phytoplankton': a, b = 0.288e-9, 0.811
        elif p_class in ('long_pellet', 'short_pellet', 'salp_pellet'): a, b = 0.113e-9, 1
        elif p_class in ('unidentifiable', 'fiber', 'swimmer'): continue
        else:
            print('UNKNOWN PARTICLE TYPE')
            continue
        flux = shape_to_flux(a, b, v, second, first, frames)

        total_flux += flux

    return total_flux

def shape_to_flux(a, b, v, magnification, time, frames):
    """Converts shape volume to flux value based on magnification and elapsed time."""

    area_image = 2048 * 2048 # Size of the images

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

    carbon = (a * (1/area)**3 * v**b) / 12.011 # from mg to mol
    flux = carbon / (area_image *  (1/area)**2 * time * frames)
    
    return flux * 10e12 # From um2 to m2 (microns**2 to m**2)

def save_to_excel(data, output_path):
    """
    Save processed data to an Excel file.

    Args:
        data (list of dict): List of dictionaries containing image parameters.
        output_path (str): Path to save the Excel file.
    """
    df = pd.DataFrame(data)
    df.to_excel(output_path, index=False)
    print(f"Results saved to {output_path}")
if __name__ == "__main__":
    directory_path = "/Volumes/CFElab/Data_archive/Images/geltrap_microscopy/EXPORTS2/Classified_particles_fromModel_and_validated/"
    metadata_excel = "JC_trap_summary.xlsx"
    output_excel = "processed_image_data.xlsx"

    # Process images and collect data
    files_to_process = [
        os.path.join(dirpath, file)
        for dirpath, _, files in os.walk(directory_path)
        for file in files if file.endswith(".tiff") and "JC" in file
    ]

    processed_data = []  # List to hold all results

    for file_path in files_to_process:
        # Process image
        volume_final, volume = process_image_volume(file_path)
        clas = get_folder_name(file_path)
        first_fragment, second_fragment = extract_fragments(file_path)

        # Extract elapsed time and magnification from metadata
        elapsed_time, magnification = extract_elapsed_time_from_excel(first_fragment, second_fragment, metadata_excel)

        # Calculate flux if applicable
        a, b = 0, 0
        if clas == 'aggregate': a, b = 0.113e-9, 0.81
        elif clas == 'mini_pellet': a, b = 0.113e-9, 1
        elif clas == 'rhizaria': a, b = 0.004e-9, 0.939
        elif clas == 'phytoplankton': a, b = 0.288e-9, 0.811
        elif clas in ('long_pellet', 'short_pellet', 'salp_pellet'): a, b = 0.113e-9, 1
        elif clas in ('unidentifiable', 'fiber', 'swimmer'): continue
        else:
            clas = "Unknown"
            print('UNKNOWN PARTICLE TYPE')
            continue

        flux = shape_to_flux(a, b, volume, second_fragment, elapsed_time, 1)

        # Collect data
        processed_data.append({
            "Image Path": file_path,
            "Class": clas,
            "Volume (pixels^3)": volume_final,
            "Initial Volume (pixels^3)": volume,
            "A Parameter": a,
            "B Parameter": b,
            "Flux (mol C m−2 d-1)": flux,
            "Elapsed Time (days)": elapsed_time,
            "Magnification": magnification,
            "First Fragment": first_fragment,
            "Second Fragment": second_fragment
        })

    # Save all results to Excel
    save_to_excel(processed_data, output_excel)

    # Optional: Calculate total flux
    total_flux = sum(item["Flux (mol C m−2 d-1)"] for item in processed_data if item["Flux (mol C m−2 d-1)"] is not None)
    print(f"Total Flux: {total_flux} mol C m−2 d-1")

#if __name__ == "__main__":

#    directory_path = "/Volumes/CFElab/Data_archive/Images/geltrap_microscopy/EXPORTS2/Classified_particles_fromModel_and_validated/"
#    volume_results, clas, first, second, frames = process_directory_images(directory_path, "JC_trap_summary.xlsx")
#    flux = flux_equations(clas, volume_results, first, second, frames)
#    print(f"Total Flux: {flux} mol C m−2 d-1")
