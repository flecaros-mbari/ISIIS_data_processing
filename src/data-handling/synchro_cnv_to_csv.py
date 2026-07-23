"""
Script Description:
===================
Converts Seabird CTD ".cnv" files from SynchroOSW deployments into a simple
CSV with one row per scan containing only 'timestamp' and 'depth'.

This only works for ".cnv" files that already carry a per-scan Julian-day
column (name = timeJ) alongside depth (name = depSM), which is the format
SynchroOSW CTD exports use (e.g. "*ctd_with_time.cnv"). Depth-binned ".cnv"
exports without a per-row time column are not supported by this script.

The absolute timestamp for each row is computed as:
    timestamp = Jan 1 00:00:00 of start_time's year + (timeJ - 1) days

Dependencies:
- pandas
- argparse
"""

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


def parse_cnv(file_path):
    """Parse a Seabird .cnv file into a DataFrame plus its start time.

    Args:
        file_path (str): path to the .cnv file

    Returns:
        df (DataFrame): raw data section, one column per '# name' entry
        start_time (datetime): cast start time parsed from the header
        names (list[str]): column names in file order
    """
    with open(file_path, "r", encoding="latin-1") as f:
        lines = f.readlines()

    names = []
    start_time = None
    data_start = None

    for i, line in enumerate(lines):
        if line.startswith("# name"):
            rhs = line.split("=", 1)[1].strip()
            column_name = rhs.split(":")[0].strip()
            names.append(column_name)

        if "start_time" in line and start_time is None:
            start_str = line.split("=")[1].split("[")[0].strip()
            start_time = datetime.strptime(start_str, "%b %d %Y %H:%M:%S")

        if line.startswith("*END*"):
            data_start = i + 1
            break

    data_lines = [l for l in lines[data_start:] if l.strip()]
    rows = [l.split() for l in data_lines]
    df = pd.DataFrame(rows, columns=names).astype(float)

    return df, start_time, names


def cnv_to_timestamp_depth(file_path):
    """Convert a SynchroOSW .cnv file into a (timestamp, depth) DataFrame.

    Args:
        file_path (str): path to the .cnv file

    Returns:
        out (DataFrame): columns 'timestamp' and 'depth'
    """
    df, start_time, names = parse_cnv(file_path)

    if "timeJ" not in names:
        raise ValueError(
            f"{file_path} has no 'timeJ' column - it looks like a depth-binned "
            "export without per-scan time. This script only supports the "
            "SynchroOSW '*ctd_with_time.cnv' format."
        )

    year_start = datetime(start_time.year, 1, 1)
    df["timestamp"] = year_start + pd.to_timedelta(df["timeJ"] - 1, unit="D")

    depth_col = "depSM" if "depSM" in df.columns else names[0]
    out = df[["timestamp", depth_col]].rename(columns={depth_col: "depth"})

    return out


def main():
    parser = argparse.ArgumentParser(
        description="Convert SynchroOSW CTD .cnv files into timestamp/depth CSVs."
    )
    parser.add_argument(
        "--cnv-path",
        type=str,
        nargs="+",
        required=True,
        help="Path(s) to one or more .cnv files to convert.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to save the output CSVs (default: same directory as each .cnv file).",
    )

    args = parser.parse_args()

    for cnv_path in args.cnv_path:
        cnv_path = Path(cnv_path)
        out_dir = Path(args.output_dir) if args.output_dir else cnv_path.parent
        out_path = out_dir / f"{cnv_path.stem}_timestamp_depth.csv"

        out = cnv_to_timestamp_depth(cnv_path)
        out.to_csv(out_path, index=False)
        print(f"Saved {len(out)} rows to {out_path}")


if __name__ == "__main__":
    main()
