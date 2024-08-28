import os
import pandas as pd

def change_image_paths(csv_folder, new_image_path):
    """This function tranform the path of a csv into another one, is useful for the detection process of the images
    This function asumes that the column image_path is the name of the path of the image

    Args:
        csv_folder (str): the folder where are the csv files
        new_image_path (str): the name of the new path
    """

    try:
        # Iterate over each file in the CSV folder
        for filename in os.listdir(csv_folder):
            # For the csv files of the folder
            if filename.endswith(".csv"):

                # Get the peth
                file_path = os.path.join(csv_folder, filename)

                # Load CSV file
                df = pd.read_csv(file_path)

                # Change image paths in the 'image_path' column
                df['image_path'] = df['image_path'].apply(lambda x: os.path.join(new_image_path, os.path.basename(x)))

                # Save the modified DataFrame back to the CSV file
                df.to_csv(file_path, index=False)

                # Printing for debugging 
                print(f"Image paths in '{filename}' changed successfully.")

    except Exception as e:
        print("An error occurred:", e)

# Example usage:
# Csv folder which contains the csv files
csv_folder = "/Users/fernandalecaros/Documents/Data/1500det/det_filtered/csv/"

# New path for the image_path in the csv
new_image_path = "/Users/fernandalecaros/Documents/Data/1500/"


# Using the function
change_image_paths(csv_folder, new_image_path)
