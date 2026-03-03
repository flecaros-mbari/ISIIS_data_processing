"""
Script: Image Selection, Copying, and Review
Author: Fernanda Lecaros
Purpose:
    - Recursively select images from a source directory with specific naming criteria.
    - Copy a desired number of images to a destination folder.
    - Use multiprocessing to load images quickly.
    - Display images for inspection using pygame and mark images for deletion.
"""

import os
import random
import shutil
import pygame
from pygame.locals import *
from tqdm import tqdm
import multiprocessing
from PIL import Image
import io


# ------------------------- IMAGE HANDLING -------------------------
def get_all_images(directory):
    """
    Recursively collect image files with specific naming:
        - Starts with 'CFE'
        - Ends with 'm' before the extension
    
    Args:
        directory (str): Directory to search for images.

    Returns:
        list: Full paths of matching images.
    """
    image_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff')
    all_images = []

    for root, _, files in os.walk(directory):
        for file in files:
            if (file.lower().endswith(image_extensions)
                    and file.startswith('CFE')
                    and file.endswith('m' + file[-4:])):
                all_images.append(os.path.join(root, file))

    return all_images


def copy_images(source_dir, destination_dir, num_images, check_point):
    """
    Copy a specified number of images from source to destination, avoiding duplicates.

    Args:
        source_dir (str): Folder containing source images.
        destination_dir (str): Folder to store copied images.
        num_images (int): Number of images to copy.
        check_point (str): Folder for checking existing copies.

    Returns:
        int: Number of successfully copied images.
    """
    all_images = get_all_images(source_dir)
    total_images = len(all_images)

    if total_images < num_images:
        print(f"Only {total_images} images found, copying all available.")
        num_images = total_images

    selected_images = random.sample(all_images, num_images)

    os.makedirs(destination_dir, exist_ok=True)
    copied_count = 0

    for image in tqdm(selected_images, desc="Copying images"):
        try:
            image_name = os.path.basename(image)
            dest_path = os.path.join(destination_dir, image_name)
            check_path = os.path.join(check_point, image_name)

            if os.path.exists(check_path):
                print(f"File {image_name} already exists in {check_path}. Skipping...")
                continue

            shutil.copy(image, dest_path)
            copied_count += 1

        except PermissionError as e:
            print(f"Permission denied: {e}. Skipping {image}")
        except Exception as e:
            print(f"Error copying {image}: {e}")

    print(f"Copied {copied_count} images to {destination_dir}")
    return copied_count


def read_image(path):
    """Read raw image data from file."""
    try:
        with open(path, 'rb') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return None


def upload_images_multiprocessing(directory):
    """
    Load images from a directory in parallel using multiprocessing and convert to pygame surfaces.

    Args:
        directory (str): Folder containing images.

    Returns:
        tuple: (list of pygame surfaces, list of image paths)
    """
    image_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff')
    paths = [os.path.join(root, file)
             for root, _, files in os.walk(directory)
             for file in files
             if file.lower().endswith(image_extensions)
             and file.startswith('CFE')
             and file.endswith('m' + file[-4:])]

    # Multiprocessing to read image data
    with multiprocessing.Pool() as pool:
        images_data = pool.map(read_image, paths)

    images_data = [img for img in images_data if img is not None]
    images = []

    for img_data in images_data:
        try:
            img = Image.open(io.BytesIO(img_data))
            mode, size = img.mode, img.size
            data = img.tobytes()
            surface = pygame.image.fromstring(data, size, mode)

            # Scale to 50%
            new_size = (surface.get_width() // 2, surface.get_height() // 2)
            surface = pygame.transform.scale(surface, new_size)

            images.append(surface)
        except Exception as e:
            print(f"Error converting image data: {e}")

    return images, paths


# ------------------------- IMAGE DISPLAY -------------------------
def show_images(directory):
    """
    Display images for review using pygame, allow marking for deletion.

    Args:
        directory (str): Folder containing images to display.
    """
    pygame.init()
    images, paths = upload_images_multiprocessing(directory)
    num_images = len(images)
    index = 0
    images_to_delete = []

    if num_images == 0:
        print(f"No valid images in {directory}.")
        pygame.quit()
        return

    screen = pygame.display.set_mode(images[0].get_rect().size)
    pygame.display.set_caption('Image Viewer')
    screen.blit(images[index], (0, 0))
    pygame.display.flip()

    running = True
    while running:
        for event in pygame.event.get():
            if event.type == QUIT:
                running = False
            elif event.type == KEYDOWN:
                if event.key == K_LEFT:
                    index = (index - 1) % num_images
                elif event.key == K_RIGHT:
                    index = (index + 1) % num_images
                elif event.key == K_UP:
                    if index not in images_to_delete:
                        images_to_delete.append(index)
                        print(f"Marked for deletion: {paths[index]}")

        screen.fill((0, 0, 0))
        if num_images > 0:
            screen.blit(images[index], (0, 0))
        pygame.display.flip()

    pygame.quit()

    # Delete images
    for idx in images_to_delete:
        try:
            os.remove(paths[idx])
            print(f"Deleted image: {paths[idx]}")
        except Exception as e:
            print(f"Error deleting {paths[idx]}: {e}")


# ------------------------- CHECK AND FILL -------------------------
def check_and_fill_images(source_dir, dest_dir, desired_count, check_point):
    """
    Ensure destination directory has desired number of images, copy more if needed, and show for review.

    Args:
        source_dir (str): Folder containing original images.
        dest_dir (str): Folder to fill with images.
        desired_count (int): Number of images to maintain.
        check_point (str): Folder to avoid duplicates.
    """
    current_images = get_all_images(dest_dir)
    current_count = len(current_images)

    if current_count >= desired_count:
        print(f"Already have {current_count} images in {dest_dir}.")
        show_images(dest_dir)
        return

    print(f"Currently have {current_count} images. Adding {desired_count - current_count} more...")
    copy_images(source_dir, dest_dir, desired_count - current_count, check_point)

    # Recursive check
    check_and_fill_images(source_dir, dest_dir, desired_count, check_point)


# ------------------------- MAIN -------------------------
if __name__ == "__main__":
    check_point = "/Volumes/CFElab/Data_archive/Images/ISIIS/COOK/Videos2framesdepth/5000_depth/"
    source_directory = "/Volumes/CFElab/Data_archive/Images/ISIIS/COOK/Videos2framesdepth/"
    destination_directory = os.path.join(source_directory, "25000_depth")
    number_of_images = 25000

    check_and_fill_images(source_directory, destination_directory, number_of_images, check_point)