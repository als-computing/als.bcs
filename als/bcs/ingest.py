#!/usr/bin/env python

"""bcs.ingest.py

    Extract data and metadata from BCS data files.
"""

import logging

logger = logging.getLogger(__name__)

import sys
import os

from collections import OrderedDict
from enum import auto, Enum, IntEnum
from pathlib import Path
from typing import Any, Mapping, Sequence
from warnings import warn

from datetime import datetime
import pytz

try:
    import ujson as json
except ImportError:
    import json

import pandas as pd

from .data import DataFileNumbers, get_data_file_numbers, read_data_file
from .errors import ScanFileNotFoundError, ScanFileRowWarning, warn_or_raise
from .find import replace_subpath
from .scans import import_scan_file, is_flying_scan

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Configure environment
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
default_tz_name = 'America/Los_Angeles'
default_timezone = pytz.timezone(default_tz_name)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_file_timestamps(file_path: str) -> Mapping[str, Any]:
    """Return file timestamps as dict"""
    file_stats = os.stat(file_path)
    sys_platform = sys.platform
    logger.debug(f"{sys_platform = }")
    if sys_platform.lower() == "windows":
        file_create_timestamp = file_stats.st_ctime	# Windows
    elif sys_platform.lower() == "darwin":
        file_create_timestamp = file_stats.st_birthtime	# Mac
    else:
        # We're probably on Linux. No easy way to get creation dates here,
        # so we'll settle for when its content was last modified.
        file_create_timestamp = file_stats.st_mtime
    return dict(
        file_path=file_path,
        created_timestamp=file_create_timestamp,
        modified_timestamp=os.path.getmtime(file_path),
        )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_filename(file_path: str) -> str:
    """Extract file name from file path"""
    return os.path.basename(file_path)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_filename_base(file_path: str) -> str:
    """Extract file name base (no extension) from file path"""
    filename = get_filename(file_path)
    filename_base = filename.rsplit('.', 1)[0]
    return filename_base

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def timestamp_to_string(timestamp: float, tz_name: str=default_tz_name) -> str:
    """Convert timestamp to timezone-aware string"""
    def format_datetime(datetime_obj):
        # return datetime.strftime(datetime_obj, "%Y-%m-%d %H:%M:%S.%f [%z]")
        return datetime.strftime(datetime_obj, "%Y-%m-%d %H:%M:%S [%z]")
    # Add TZ awareness
    timezone = pytz.timezone(tz_name)
    datetime_obj = datetime.fromtimestamp(timestamp, tz=timezone)
    return format_datetime(datetime_obj)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def timestamp_to_iso8601(timestamp: float, tz_name: str=default_tz_name,
                         ) -> str:
    """Convert timestamp to timezone-aware ISO 8601 string"""
    # Add TZ awareness
    timezone = pytz.timezone(tz_name)
    datetime_obj = datetime.fromtimestamp(timestamp, tz=timezone)
    return datetime_obj.isoformat()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def detect_mimetype(file_path: str, default_mimetype: str="") -> str:
    """Deduce the MIME type of a BCS (meta)data file"""

    def bcs_scantype(file_path: Path, default_type: str="") -> str:
        """Check for a BCS (meta)data file name"""
        if file_path.name.startswith("TrajScan"):
            return "traj-scan"
        elif file_path.name.startswith("SigScan"):
            return "sig-scan"
        elif file_path.name.startswith("MotScan"):
            return "mot-scan"
        elif file_path.name.startswith("TimeScan"):
            return "time-scan"
        elif file_path.name.startswith("Single Motor Scan"):
            return "bl-scan/sig-scan"
        elif file_path.name.startswith("Single Motor Flying Scan"):
            return "bl-scan/sig-fly-scan"
        elif file_path.name.startswith("Trajectory Scan"):
            return "bl-scan/traj-scan"
        elif file_path.name.startswith("From File Scan"):
            return "bl-scan/file-scan"
        elif file_path.name.startswith("Time Scan"):
            return "bl-scan/time-scan"
        elif file_path.name.startswith("Automation Run"):
            return "bl-scan/auto-run"
        else:
            return default_type

    file_path = Path(file_path)
    scan_type = bcs_scantype(file_path)

    if file_path.suffix == ".txt" and scan_type:
        return f"text/als/bcs/{scan_type}"
    elif file_path.suffixes == [".sdc", ".json"] and scan_type:
        return f"json/als/bcs/{scan_type}"
    else:
        return default_mimetype

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def read_bcs_txt_file(
        file_path: str, 
        subpath_replace_dict: Mapping[str, str]=None,
        ) -> Mapping[str, Any]:
    """Extract data and metadata from a BCS data file"""
    subpath_replace_dict = subpath_replace_dict or {}

    # Get some file metadata from the operating system
    data_file_numbers = DataFileNumbers(
        *get_data_file_numbers(file_path)
    )
    data_file_timestamps = get_file_timestamps(file_path)

    if detect_mimetype(file_path) == "text/als/bcs/bl-scan/auto-run":
        # Metadata for an Automation Run has a different format
        scan_runs = read_automation_run(file_path)
        file_header = {"scan_runs": scan_runs}
        # There is no corresponding data table
        data_df = None
    else:
        # Get the file header metadata
        file_header = get_data_file_header(
            file_path, 
            subpath_replace_dict=subpath_replace_dict,
            )
        # Extract the data table
        data_df = read_data_file(
            file_path, 
            subpath_replace_dict=subpath_replace_dict,
            )

    # Replace datetime.date objects with compatible field
    if file_header.get("date", None):
        iso_date = file_header["date"].isoformat()
        file_header["date"] = iso_date
    
    metadata = OrderedDict((
        ("file_info", OrderedDict((
            ("scan_number", data_file_numbers.scan),
            ("file_number", data_file_numbers.file),
            ("repeat_number", data_file_numbers.repeat),
            ("created_time", data_file_timestamps["created_timestamp"]),
            ("modified_time", data_file_timestamps["modified_timestamp"]),
            ("created_iso8601", timestamp_to_iso8601(
                data_file_timestamps["created_timestamp"])
                ),
            ("modified_iso8601", timestamp_to_iso8601(
                data_file_timestamps["modified_timestamp"])
                ),
            ))
        ),
        ("file_header", file_header),
    ))

    # Add metadata from optional JSON sidecar
    json_sidecar_path = file_path.replace(".txt", ".sdc.json")
    if Path(json_sidecar_path).exists() and (json_sidecar_path != file_path):
        sidecar = read_json_sidecar(json_sidecar_path)
        metadata["experiment"] = sidecar.get("Experiment", {})
        metadata["scan_setup"] = sidecar.get("Scan Setup", {})

    return dict(data=data_df, metadata=metadata)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def read_json_sidecar(file_path: str) -> Mapping[str, Any]:
    """Extract metadata from a BCS JSON sidecar file"""

    with open(file_path) as json_file:
        sidecar = json.load(json_file)
    
    return sidecar

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def read_automation_run(file_path: str) -> Sequence[Mapping[str, Any]]:
    """Extract metadata from a BCS Automation Run summary file"""

    with open(file_path, 'r') as run_file:
        for (header_row, file_line) in enumerate(run_file):
            logger.debug("[{}]: {}".format(header_row, file_line))
            if file_line.startswith("Time"):
                break
            if file_line[0].isdigit():
                header_row -= 1
                break

    runs_df = pd.read_csv(
        file_path,
        delimiter='\t',
        header=header_row,
        skip_blank_lines=False,
        )
    path_columns = (
        col for col in runs_df.columns 
            if "path" in col.lower().split()
        )
    for col in path_columns:
        runs_df[col] = runs_df[col].apply(
            lambda path: path.replace("\\", "/")
        )
    runs = sorted(
        runs_df.to_dict('records'), 
        key=lambda row: row["Time"],
        )
    
    return runs

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_data_file_header(
        data_file_path: str, 
        /,
        subpath_replace_dict: dict=None,
        *,
        raise_warning: bool=False,
        ) -> Mapping[str, Any]:
    """Extract the data file header.

        data_file_path: Fully qualified file path (dir + file) of data.
        subpath_replace_dict: {old_subpath: new_subpath, ...}
            *) Used for locating input scan file
        raise_warning: If True, raise an encountered warning as an Exception;
            otherwise report the warning.

        RETURNS: dict with header fields
    """

    if subpath_replace_dict is None:
        subpath_replace_dict = dict()

    logger.debug(f"***{data_file_path}")
    
    header_info = dict(
        scan_type=None,
        date=None,
        motors=[], 
        motor_first=[],
        motor_last=[],
        motor_step=[],
        # motor_velocity=[],  # Must be undefined for non-flying
        flying=False,
        )
    get_motor_name = False
    get_pause_motor_name = False
    get_scan_file_path = False
    get_memo = False
    is_time_scan = False

    class ROW_ERROR(Enum):
        FLYING_COMMAND = "Invalid or missing command for flying motor"
        FLYING_VALUE = "Invalid or missing value for flying motor"
        MOTOR_VALUE = "Invalid or missing value for motor"

    def handle_time_scan_numbers(header_linenum: int, file_line: str) -> bool:
        """Process a TimeScan header line that starts with a number.

        TimeScans have multiple header lines that start with a number.
        Extract and store the relevant information from the header line.

        header_linenum: int, 0-based index of the current file line
        file_line: str, data from the current file line

        RETURNS: bool, True if this is a header line; False otherwise.
        """
        class LINES(IntEnum):
            MEMO_LENGTH = auto()
            MEMO = auto()
            SAMPLES = auto()
            ACQUIRE = auto()
        
        nonlocal get_memo
        nonlocal header_info
        str_values = file_line.strip().split()

        if header_linenum == LINES.MEMO_LENGTH:
            if len(str_values) < 1:
                msg = f"Header line {header_linenum} must start with integer"
                raise ValueError(msg)
            memo_length = int(str_values[0]) - 1  # zero-terminated string
            if memo_length >= 0:
                get_memo = True
                return True
        elif header_linenum == LINES.MEMO:
            # This should only execute if header is malformed
            value_str = file_line.rstrip()
            header_info["memo"] = value_str
            get_memo = False
            return True
        elif header_linenum == LINES.SAMPLES:
            if len(str_values) < 1:
                msg = f"Header line {header_linenum} must start with integer"
                raise ValueError(msg)
            header_info["num_samples"] = int(str_values[0])
            return True
        elif header_linenum == LINES.ACQUIRE:
            if len(str_values) < 5:
                msg = (f"Header line {header_linenum} "
                        "must have at least 5 numbers")
                raise ValueError(msg)
            period_sec = float(str_values[1])
            count_sec = float(str_values[4])
            delay_sec = max(0., period_sec - count_sec)
            header_info["count_sec"] = count_sec
            header_info["delay_sec"] = delay_sec
            return True
        else:
            return False

    with open(data_file_path, 'r') as data_file:
        for (header_linenum, file_line) in enumerate(data_file):

            logger.debug(header_linenum, file_line)

            if get_motor_name:
                motor_name = file_line.strip()
                header_info["motors"].append(motor_name)
                get_motor_name = False
                continue
            if get_pause_motor_name:
                motor_name = file_line.strip()
                header_info["pause_motor"] = motor_name
                get_pause_motor_name = False
                get_scan_file_path = True
                continue
            if get_scan_file_path:
                scan_file_path_raw = file_line.rstrip()
                # Make the next transformation optional?
                scan_file_path = scan_file_path_raw.replace("\\", "/")
                header_info["scan_file_path"] = scan_file_path
                get_scan_file_path = False
                # Get scan file info
                (
                 scan_number, 
                 file_number, 
                 repeat_number,
                 ) = get_data_file_numbers(data_file_path)
                scan_file_path = replace_subpath(
                    scan_file_path, subpath_replace_dict)
                # logger.debug(f"{get_filename_base(data_file_path)}")
                # logger.debug(f"...{(scan_number, file_number, repeat_number) = }")
                try:
                    motor_df = import_scan_file(scan_file_path, file_number)
                except FileNotFoundError:
                    raise ScanFileNotFoundError(
                        scan_file_path=scan_file_path, data_file_path=data_file_path,
                    )
                # logger.debug(f"...{motor_df = }")
                header_info["motors"] = motor_df.columns.values
                header_info["motor_values"] = motor_df.values.T
                if is_flying_scan(scan_file_path):
                    header_info["flying"] = True
                    # Move flying motor values to primary motor values
                    flying_motor_values = header_info["motor_values"][-1]
                    key_names = [
                        "motor_first",
                        "motor_last",
                        "motor_step",
                        "motor_velocity",
                        ]
                    for key_name in key_names:
                        header_info[key_name] = []
                    for step, motor_value_str in enumerate(flying_motor_values):
                        if not isinstance(flying_motor_values, str):
                            warn_or_raise(
                                ScanFileRowWarning(
                                    scan_file_path=scan_file_path,
                                    file_number=file_number,
                                    step_number=step+1,
                                    description=ROW_ERROR.FLYING_COMMAND,
                                ),
                                raise_warning=raise_warning,
                            )
                            continue
                        motor_value_str = motor_value_str.strip().lower().lstrip('flying')
                        motor_value_str = motor_value_str.strip().lstrip('(').rstrip(')')
                        for (motor_value, key_name) in zip(
                                motor_value_str.split(','),
                                key_names,
                                ):
                            try:
                                header_info[key_name].append(float(motor_value))
                            except TypeError:
                                description = " ".join([
                                    ROW_ERROR.FLYING_VALUE,
                                    f"'{key_name}'",
                                ])
                                warn_or_raise(
                                    ScanFileRowWarning(
                                        scan_file_path=scan_file_path,
                                        file_number=file_number,
                                        step_number=step+1,
                                        description=description,
                                    ),
                                    raise_warning=raise_warning,
                                )
                    header_info["motor_values"] = header_info["motor_values"][:-1]
                continue
            if get_memo:
                value_str = file_line.rstrip()
                header_info["memo"] = value_str
                get_memo = False
                continue
            if file_line.startswith("Date: "):
                date_str = file_line.strip().split("Date: ", 1)[1]
                data_date = datetime.strptime(date_str, "%m/%d/%Y").date()
                header_info["date"] = data_date
                continue
            if file_line.startswith("Flying Scan"):
                header_info["flying"] = True
                header_info["scan_type"] = "Single Motor"
                get_motor_name = True
                continue
            if file_line.startswith("Start, Stop, Increment"):
                header_info["scan_type"] = "Single Motor"
                get_motor_name = True
                continue
            if file_line.startswith("From File"):
                # scan_file_used = True
                header_info["scan_type"] = "Trajectory"
                get_pause_motor_name = True
                continue
            # TODO: Add Two Motor Scan
            # TODO: Add Image Scan
            if file_line.startswith("Start"):
                value_str = file_line.strip().split("Start: ", 1)[1]
                header_info["motor_first"] = [float(value_str)]
                continue
            if file_line.startswith("Stop"):
                value_str = file_line.strip().split("Stop: ", 1)[1]
                header_info["motor_last"] = [float(value_str)]
                continue
            if file_line.startswith("Increment"):
                value_str = file_line.strip().split("Increment: ", 1)[1]
                header_info["motor_step"] = [float(value_str)]
                continue
            if file_line.startswith("X Center"):
                value_str = file_line.strip().split("X Center: ", 1)[1]
                header_info["motor_velocity"] = [float(value_str)]
                continue
            if file_line.startswith("Delay "):
                value_str = file_line.strip().split("Delay After Move (s): ", 1)[1]
                header_info["delay_sec"] = float(value_str)
                continue
            if file_line.startswith("Count "):
                value_str = file_line.strip().split("Count Time (s): ", 1)[1]
                header_info["count_sec"] = float(value_str)
                continue
            if file_line.startswith("Scan Number"):
                value_str = file_line.strip().split("Scan Number: ", 1)[1]
                try:
                    value = int(value_str)  # Repeat number
                except ValueError:
                    value = value_str  # Summary file; e.g. Avg, Sum, etc.
                header_info["repeat_number"] = value
                continue
            if file_line.startswith("Bi-directional"):
                value_str = file_line.strip().split("Bi-directional: ", 1)[1]
                header_info["bidirect"] = True if (
                    value_str=="Yes") else False
                continue
            if file_line.startswith("Stay "):
                value_str = file_line.strip().split("Stay at End: ", 1)[1]
                header_info["stay_at_end"] = bool(value_str)
                continue
            if file_line.startswith("Decription "):
                value_str = file_line.strip().split("Description Length: ", 1)[1]
                get_memo = bool(value_str)
                # Is length needed for multi-line memo?
                continue
            if file_line[0].isdigit():
                if is_time_scan and handle_time_scan_numbers(
                        header_linenum, file_line):
                    continue
                if header_linenum == 1:
                    is_time_scan = True
                    header_info["scan_type"] = "Time"
                    handle_time_scan_numbers(header_linenum, file_line)
                    continue
                # We've gone past the header
                header_linenum -= 1
                header_info["motor_header_linenum"] = header_linenum
                break

    return(header_info)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main() -> int:
    """The main routine."""
    return(0)
    

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    main()