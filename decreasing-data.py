import os
import shutil

# Source folder containing CSV files
source_folder = '/Users/fernandalecaros/Documents/Data/1500det/det_filtered/csv'

# Destination folder to save selected CSV files
destination_folder = '/Users/fernandalecaros/Documents/Data/1500det/det_filtered-reduced/csv'

# Ensure destination folder exists, create it if not
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

# Get a list of all files in the source folder
csv_files = [file for file in os.listdir(source_folder) if file.endswith('.csv')]

# Iterate through CSV files and copy every other file to the destination folder
for i, file_name in enumerate(csv_files):
    if i % 10 == 0:  # Select every other file
        source_file_path = os.path.join(source_folder, file_name)
        destination_file_path = os.path.join(destination_folder, file_name)
        shutil.copyfile(source_file_path, destination_file_path)
        print(f"File '{file_name}' copied to '{destination_folder}'")

print("Copying completed.")

# Count the number of CSV files in the destination folder
copied_csv_files = [file for file in os.listdir(destination_folder) if file.endswith('.csv')]
print(f"Number of CSV files in the destination folder: {len(copied_csv_files)}")
