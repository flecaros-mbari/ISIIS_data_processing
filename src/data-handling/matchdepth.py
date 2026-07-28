import argparse
import pandas as pd
import re
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import os
import shutil
import numpy as np
from tqdm import tqdm

def parse_log_date(year, yearday, time_string, timezone):
    """Parses the log date into a datetime object.

    Args:
        year (str): year of the data
        yearday (str): yearday of the data
        time_string (str): time of the data
        timezone (str): timezone of the data

    Returns:
        parsed_datetime: timestamp
    """    
    try:
        # Get the variables
        date = f'{year}-{yearday.zfill(3)}'
        time_string = time_string.strip()
        datetime_str = f'{date} {time_string} {timezone}'
        parsed_datetime = datetime.strptime(datetime_str, '%Y-%j %H:%M:%S %z')

        # return the timestamp
        return parsed_datetime
    
    except ValueError as e:
        print(f"Error to parse: {e}")
        return None

def filter_time_and_pressure_data(df, filter_columns = False):
    """Filter the DataFrame to show only some columns.

    Args:
        df(dataframe): dataframe that I want to filter
        filter_columns(bool): if its true we will use only some columns, 
                                otherwise we will filter values

    Returns:
        filtered_df: filtered dataframe
    """    

    # List of columns that I want (only if filter_columns is True)
    time_columns = [
        'timestamp',   # UTC
        'loghost_system_utc',  # original UTC column
        'year',
        'yearday',
        'time',
        'timezone',
        'parsed_datetime'
    ]
    
    pressure_columns = [
        'rov_ctd_pressure',
        'rov_pressure'
    ]

    # Filter for the columns of preassure (we are intereset in the ctd preassure)
    if filter_columns:
        filtered_df = df[time_columns + pressure_columns]
    else:
        filtered_df = df

    # Filter values of preassure
    filtered_df = filtered_df[
        ~filtered_df['rov_ctd_pressure'].isin(['NO_PUB', 'NO_PROV']) &
        ~filtered_df['rov_pressure'].isin(['NO_PUB', 'NO_PROV'])
    ]

    # Now that the invalid strings are removed, we can safely convert to float and filter by value
    filtered_df = filtered_df[filtered_df['rov_pressure'].astype(float) > 0]

    # Return the filtered dataframe
    return filtered_df

def add_seconds(dt, seconds):
    """Function to add fractional seconds

    Args:
        dt(timestamp): time that i want to add some seconds
        seconds(int): seconds that i want to add

    Returns:
        dt + timedelta(seconds=seconds): the addition fo the seconds to the timestamp
    """
    return dt + timedelta(seconds=seconds)

def find_matching_timestamps(df1, df2, defase= 0, threshold=8, raw = False):
    """Function to match the timestamps considering a defase of the clocks
    and a minimum threshold in seconds to consider the two timestamps as a match.

    Args:
        df1 (timestamp): first timestamp to analyze
        df2 (timestmap): second timestamp to analyze
        defase (int, optional): defase between clocks. Defaults to 0.
        threshold (int, optional): minimum seconds to consider the timstamps as a match. Defaults to 8.

    Returns:
        results: list with the matches
    """
    
    # Tranforming everything into timestamp
    df1['timestamp'] = pd.to_datetime(df1['timestamp'], errors='coerce')
    df2['iso_datetime'] = pd.to_datetime(df2['iso_datetime'], errors='coerce')

    # Filter out CTD rows with non-positive depth (some depths in the ctd file are negative)
    if raw:
        depth_values = pd.to_numeric(df1['rov_ctd_pressure'], errors='coerce')
    elif 'depth' in df1.columns:
        depth_values = pd.to_numeric(df1['depth'], errors='coerce')
    else:
        depth_values = pd.to_numeric(df1['DepSM'], errors='coerce')

    # Just depths > 0
    df1 = df1[depth_values > 0].reset_index(drop=True)

    # Timestmaps in UTC
    df1['timestamp'] = df1['timestamp'].apply(lambda x: x if x.tzinfo else pytz.utc.localize(x))
    df2['iso_datetime'] = df2['iso_datetime'].apply(lambda x: x if x.tzinfo else pytz.utc.localize(x))
    
    # Aplying the defase in the images (their clock runs ahead of the ctd in the rov)
    df2['adjusted_iso_datetime'] = df2['iso_datetime'].apply(lambda x: add_seconds(x, defase))
    
    # Expanding dimensions to calculate the diferente with matrices 
    timestamps_img_exp = np.expand_dims(df2['adjusted_iso_datetime'].values, axis=1)  # (m,) -> (m, 1)
    timestamps_rovctd_ts_exp = np.expand_dims(df1['timestamp'].values, axis=0)  # (n,) -> (1, n)
    
    # Absolute diference between the timestamps
    diff_matrix = np.abs((timestamps_rovctd_ts_exp - timestamps_img_exp).astype('timedelta64[s]').astype(float))

    # Get the index of the minimum value for every picture
    min_indices = np.argmin(diff_matrix, axis=1)
    
    # Get the value
    min_values = np.min(diff_matrix, axis=1)
    
    # List of results
    results = []

    # Apply the threshold to consider the match
    for i, min_idx in enumerate(tqdm(min_indices, desc="Matching timestamps")):
        if min_values[i] <= threshold:
            ts_rovctd = df2.iloc[i]['iso_datetime']

            # The name of the pressure dependes of the data
            if raw:
                depth_rovctd = df1.iloc[min_idx]['rov_ctd_pressure'].strip()
            elif 'depth' in df1.columns:
                depth_rovctd = df1.iloc[min_idx]['depth']
            else:
                depth_rovctd = df1.iloc[min_idx]['DepSM']
            ts_img = df1.iloc[min_idx]['timestamp']
            img_path = df2.iloc[i]['path']

            # Add the matches to the results
            results.append((ts_img, depth_rovctd, ts_rovctd, img_path))
    
    # Return the results of the matches
    return results

# Create a 'timestamp' column by combining 'rovCtdDtg' and 'usec'
def create_timestamp(row):
    """Create a 'timestamp' column by combining 'rovCtdDtg' and 'usec'
    Args:
        row (dataframe): data frame 

    Returns:
        dt: the data frame with the new row
    """
    
    # Convert 'rovCtdDtg' to a datetime object in GMT
    dt = datetime.strptime(row['rovCtdDtg'], '%m/%d/%Y %H:%M:%S')
    # Localize to GMT
    dt = pytz.timezone('GMT').localize(dt)

    # Return the dataframe
    return dt

def parse_log_file_to_dataframe(file_path):
    """Parses the log file into a DataFrame. Remember that this function is only when the 
    txt is RAW, they have not tranform it. 

    Args:
        file_path (str): path to the file that I want to transform

    Returns:
        df: the reults of tranforming the txt into a dataframe
    """    
   
    try:
        # Try to open the file with  utf-16
        with open(file_path, 'r', encoding='utf-16') as file:
            lines = file.readlines()
    except UnicodeError:
        # If fails try with latin-1
        with open(file_path, 'r', encoding='latin-1') as file:
            lines = file.readlines()

    # Get the names of the columns starting with #LOG
    column_names = []
    for line in lines:
        if line.startswith('#LOG'):
            parts = line.split()
            column_name = parts[1].lower().replace('.', '_')
            column_names.append(column_name)

    # Add columns for year, yearday, time, timezone 
    column_names += ["year", "yearday", "time", "timezone"]

    # Filter the columns that not start with #
    log_entries = [line.strip() for line in lines if not line.startswith('#')]

    # Separate the data into columns
    parsed_entries = []
    errors = []

    # Transform the data
    for entry in log_entries:
        data = entry.split(', ')
        if len(data) >= 20:
            try:
                # Extract and parse the date
                year = data[16]
                yearday = data[17]
                time_string = data[18]
                timezone = data[19]
                parsed_datetime = parse_log_date(year, yearday, time_string, timezone)
                
                if parsed_datetime:
                    # Add the parsed_datetime
                    data.append(str(parsed_datetime))
                else:
                    data.append(np.nan)
                
                parsed_entries.append(data)
            except IndexError:
                errors.append(entry)
        else:
            errors.append(entry)

    # Print lines with errors
    if errors:
        print("Lines with errors:")
        for error in errors:
            print(error)

    # Get sure that the rows has teh perfect number of columns
    for i, data in enumerate(parsed_entries):
        if len(data) < len(column_names) + 1:
            # Complete with nan
            parsed_entries[i] = data + [np.nan] * (len(column_names) + 1 - len(data))

    # Create Dataframe
    df = pd.DataFrame(parsed_entries, columns=column_names + ["parsed_datetime"])

    # Tranform to the correct data type
    df['loghost_system_utc'] = pd.to_numeric(df['loghost_system_utc'], errors='coerce')
    df['timestamp'] = pd.to_datetime(df['loghost_system_utc'], unit='s', errors='coerce')
    df['rov_position_lat'] = pd.to_numeric(df['rov_position_lat'], errors='coerce')
    df['rov_position_lon'] = pd.to_numeric(df['rov_position_lon'], errors='coerce')
    df['depth'] = pd.to_numeric(df['rov_ctd_pressure'], errors='coerce')

    # Return Dataframe
    return df

def main():
    parser = argparse.ArgumentParser(
        description="Match image timestamps with ROV CTD data and optionally rename files."
    )
    parser.add_argument(
        "--ctd-path",
        type=str,
        required=True,
        help="Path to the ROV CTD log file (CSV or raw text)."
    )
    parser.add_argument(
        "--images-dir",
        type=str,
        required=True,
        help="Directory containing image files to process."
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=69,
        help="Defase (offset) in seconds to adjust timestamps (default: 69)."
    )
    parser.add_argument(
        "--fps",
        type=int,
        default=15,
        help="Images per second taken form the video"
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Indicates that the log file is raw text format, not preprocessed CSV."
    )
    parser.add_argument(
        "--rename",
        action="store_true",
        help="Rename images based on matched depth."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed matching and renaming information."
    )

    args = parser.parse_args()

    # Unpack arguments
    file_path = args.ctd_path
    images_dir_path = args.images_dir
    defase = args.delay
    raw = args.raw
    rename = args.rename
    verbose = args.verbose
    fps = args.fps

    # Read or parse log file
    if not raw:
        df = pd.read_csv(
                    file_path,
                    sep =",",
                    encoding="latin-1"  
)
        # df = pd.read_csv(file_path)
        # df['timestamp'] = df.apply(create_timestamp, axis=1)
    else:
        df = parse_log_file_to_dataframe(file_path)
        df = filter_time_and_pressure_data(df)

    # Regular expression to extract data from filenames
    pattern = re.compile(r"CFE_(.*?)-(\d+)-(\d{4}-\d{2}-\d{2} \d{2}-\d{2}-\d{2}\.\d{3})_(\d{4})")

    iso_datetime = {}
    instrument_type = {}
    path = {}
    index = 0

    # Iterate over images
    print("Scanning for image files...")
    jpg_files = [
        (root, file)
        for root, dirs, files in os.walk(images_dir_path)
        for file in files
        if file.endswith('.jpg')
    ]
    print(f"Found {len(jpg_files)} JPG files to match.")

    for root, file in tqdm(jpg_files, desc="Parsing image timestamps"):
        matches = re.findall(pattern, file)
        if matches:
            instrument, _, datetime_str, frame_num = matches[0]
            datetime_str = datetime_str.replace('-', ':') + "Z"

            dt = datetime.strptime(datetime_str, "%Y:%m:%d %H:%M:%S.%fZ")
            dt = add_seconds(dt, float(frame_num)/fps)
            # dt = pytz.timezone('America/Los_Angeles').localize(dt)
            # dt_utc = dt.astimezone(pytz.utc)

            iso_datetime[index] = dt # dt_utc
            instrument_type[index] = instrument
            path[index] = os.path.join(root, file)
            index += 1

    iso_datetime_df = pd.DataFrame({
        'iso_datetime': pd.Series(iso_datetime),
        'instrument_type': pd.Series(instrument_type),
        'path': pd.Series(path)
    })

    # Match timestamps
    matching_timestamps = find_matching_timestamps(
        df,
        iso_datetime_df,
        defase=-defase,
        raw = raw
    )

    # Print matches if verbose
    if verbose:
        print("Matching timestamps:")
        for ts1, depth, ts2, p in matching_timestamps:
            print(f"ROV: {ts1} - Instrument: {ts2} at depth {depth} with path {p}")
        print(f"Summary: {len(matching_timestamps)} matches out of {len(iso_datetime_df)} images")

    # Rename files if requested
    if rename:
        print("Renaming matched files...")
        for ts1, depth, ts2, p in tqdm(matching_timestamps, desc="Renaming files"):
            try:
                original_name, extension = os.path.splitext(os.path.basename(p))
                if re.search(r'_\d+m$', original_name):
                    if verbose:
                        print(f"Skipping {p} as it already has a depth suffix")
                    continue

                suffix = f'_{depth}m'
                new_file_name = f'{original_name}{suffix}{extension}'

                # Replace 'Videos2frames' with 'Videos2framesdepth' in the path
                new_file_path = p.replace('Videos2frames', 'Videos2framesdepth')
                new_dir = os.path.dirname(new_file_path)
                os.makedirs(new_dir, exist_ok=True)

                new_file_path = os.path.join(new_dir, new_file_name)
                shutil.copy2(p, new_file_path)

                if verbose:
                    print(f'Copied {p} to {new_file_path}')
            except Exception as e:
                print(f'Error copying {p}: {e}')

        print(f"Summary: {len(matching_timestamps)} matches out of {len(iso_datetime_df)} images")

if __name__ == "__main__":
    main()