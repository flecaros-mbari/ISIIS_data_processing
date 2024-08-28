import os
import random
import shutil
import pygame
from pygame.locals import *
from tqdm import tqdm
import multiprocessing
from PIL import Image
import io


def get_all_images(directory):
    """Recursively get all image files from the given directory. 
    They must have an image extention, start with CFE and ends with m (depth)

    Args:
        directory (str): path to the images

    Returns:
       all_images: images that we will analyze
    """

    # Images extension 
    image_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff')
    all_images = []
    
    # Choosing the images that I will analyze
    for root, _, files in os.walk(directory):
        for file in files:

            # They must start with CFE and end with m (we are oly interested in the images that has depth)
            if file.lower().endswith(image_extensions) and file.startswith('CFE') and file.endswith('m' + file[-4:]):

                # Adding the images to the list of the images that I will analyze
                all_images.append(os.path.join(root, file))
    
    return all_images

def copy_images(source_dir, destination_dir, num_images):
    """_summary_

    Args:
        source_dir (str): folder which containes the images
        destination_dir (str): new folder with the amount of images that i choose
        num_images (int): the amount of images that i want to analyze

    Returns:
        copied_count: number of images in the new folder
    """

    # Getting all the images that are available
    all_images = get_all_images(source_dir)

    # Getting the total of images
    total_images = len(all_images)
    
    # Verifying that I have enought images, otherwise I will use what I have
    if total_images < num_images:
        print(f"Only {total_images} images found, but {num_images} were requested. Copying all available images.")
        num_images = total_images
    
    # Selecting randomly the images based on the number that I specified
    selected_images = random.sample(all_images, num_images)
    
    # Making sure that the path exists, if not i will create it
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
    
    # Copying images in the new folder
    copied_count = 0
    for image in tqdm(selected_images, desc="Copying images"):
        try:

            # Copying the images
            image_filename = os.path.basename(image)
            destination_path = os.path.join(destination_dir, image_filename)
            
            # Check if file already exists in destination
            if os.path.exists(destination_path):
                print(f"File {image_filename} already exists in {destination_dir}. Skipping...")
                continue
            
            shutil.copy(image, destination_path)
            copied_count += 1

        except PermissionError as e:
            print(f"Permission denied: {e}. Skipping this file.")
        except Exception as e:
            print(f"Error copying {image}: {e}")
    
    print(f"Copied {copied_count} images to {destination_dir}")
    return copied_count

def read_image(path):
    """Read an image from the given path and return its raw data.

    Args:
        path (str): the path to read the image
    """

    try:

        # Reading the image
        with open(path, 'rb') as f:
            return f.read()
        
    except Exception as e:
        print(f"Error reading image {path}: {e}")
        return None

def upload_images_multiprocessing(directory):
    """Load images from the given directory using multiprocessing.

    Args:
        directory (str): path to the images

    Returns:
        images: list of images that i will show
        path: list with the images paths
    """

    # Read the images with the extensions 
    image_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff')
    paths = []

    # Collect image paths
    for root, _, files in os.walk(directory):
        for file in files:
            # Images containing CFE and m in their names
            if file.lower().endswith(image_extensions) and file.startswith('CFE') and file.endswith('m' + file[-4:]):
                path = os.path.join(root, file)
                paths.append(path)

    # Use multiprocessing to read images
    pool = multiprocessing.Pool()
    images_data = pool.map(read_image, paths)
    pool.close()
    pool.join()

    # Filter out None values (failed to read images)
    images_data = [img_data for img_data in images_data if img_data is not None]

    # Convert raw data to pygame surfaces
    images = []
    for img_data in images_data:
        try:
            # Open the images in pygame
            img = Image.open(io.BytesIO(img_data))
            mode = img.mode
            size = img.size
            data = img.tobytes()
            image = pygame.image.fromstring(data, size, mode)

            # Scale image to 50% of original size
            new_size = (image.get_width() // 2, image.get_height() // 2)
            image = pygame.transform.scale(image, new_size)

            # Images that I will show
            images.append(image)
        except Exception as e:
            print(f"Error converting image data: {e}")

    return images, paths

def show_images(directory):
    """Function that shows the images to review them with pygame

    Args:
        directory (str): path to the images
    """

    pygame.init()

    # Load images from directory using multiprocessing
    images, image_paths = upload_images_multiprocessing(directory)
    num_images = len(images)
    index_actual_image = 0

    # List to keep track of images to delete
    images_to_delete = []

    # Exit if there are no valid images to display
    if num_images == 0:
        print(f"No valid images in {directory}. Exiting...")
        pygame.quit()
        return

    # Set up the screen
    screen = pygame.display.set_mode(images[0].get_rect().size)
    pygame.display.set_caption('Image Viewer')
    screen.blit(images[index_actual_image], (0, 0))
    pygame.display.flip()

    # Main loop
    running = True
    while running:
        # Get keyboard and window events
        for event in pygame.event.get(): 
            # Exit the loop if the window is closed 
            if event.type == QUIT:
                running = False  
            elif event.type == KEYDOWN:
                # Left arrow key to continue seeing the images
                if event.key == K_LEFT:  
                    index_actual_image = (index_actual_image - 1) % num_images
                    print(f"Showing previous image. New index: {index_actual_image}")
                 # Right arrow key to see the images that I just past
                elif event.key == K_RIGHT: 
                    index_actual_image = (index_actual_image + 1) % num_images
                    print(f"Showing next image. New index: {index_actual_image}")
                # Up arrow key to delete the image
                elif event.key == K_UP:  
                    if index_actual_image not in images_to_delete:
                        images_to_delete.append(index_actual_image)
                        print(f"Image marked for deletion: {index_actual_image}")

        # Update screen with the new image
        # Clear the screen with black
        screen.fill((0, 0, 0))  
        if num_images > 0:

            # Show current image
            screen.blit(images[index_actual_image], (0, 0)) 

        # Update the screen
        pygame.display.flip()  

    pygame.quit()

    # Delete images marked for deletion
    for index in images_to_delete:
        try:
            current_image_path = image_paths[index]
            os.remove(current_image_path)
            print(f"Image deleted: {current_image_path}")
        except Exception as e:
            print(f"Error deleting image {current_image_path}: {e}")

def check_and_fill_images(source_directory, destination_directory, number_of_images):
    """Function to review the amoount of images and fill if its necessary 

    Args:
        source_directory (str):  path of the images
        destination_directory (str): folder were I'm storing them
        number_of_images (int): number of images that I want in the new folder
    """

    # Check if destination directory already has desired number of images
    current_images = get_all_images(destination_directory)
    current_count = len(current_images)
    
    if current_count >= number_of_images:
        print(f"Already have {current_count} images in {destination_directory}.")
        show_images(destination_directory)
        return
    
    print(f"Currently have {current_count} images. Adding more images to reach {number_of_images}...")
    remaining_images = number_of_images - current_count
    
    # Copy additional images until desired count is reached
    copied_count = copy_images(source_directory, destination_directory, remaining_images)
    
    # If images were copied, show them for inspection
    if copied_count > 0:
        print(f"Now showing images in {destination_directory} for inspection:")
        show_images(destination_directory)
    
    # Recursive call if number of images is still not reached
    check_and_fill_images(source_directory, destination_directory, number_of_images)

if __name__ == "__main__":
    source_directory = "/Volumes/CFElab-1/Data_archive/Images/ISIIS/COOK/Videos2framesdepth/"
    number_of_images = 5000
    destination_directory = f"/Volumes/CFElab-1/Data_archive/Images/ISIIS/COOK/Videos2framesdepth/{number_of_images}_depth"
    
    # Start the process of checking and filling images
    check_and_fill_images(source_directory, destination_directory, number_of_images)
