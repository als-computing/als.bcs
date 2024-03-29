#!/usr/bin/env python

"""bcs.data.py

    Functions to inspect the BCS data files that will be ingested.
"""

import logging

logger = logging.getLogger(__name__)

import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CLASSES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@dataclass
class DataFileNumbers:
    """Holds the (scan, file, repeat) numbers from a data file path."""
    scan: int
    file: Optional[int] = None
    repeat: Optional[int] = None

    @classmethod
    def from_path(cls, data_file_path: str) -> "DataFileNumbers":
        """Construct DataFileNumbers from the data file path."

        data_file_path: Fully qualified file path (dir + file) of data.

        RETURNS: a new intance of DataFileNumbers
    """
        return DataFileNumbers(*get_data_file_numbers(data_file_path))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_data_file_numbers(data_file_path):
    """Extract the (scan, file, repeat) numbers from the data file path.

        data_file_path: Fully qualified file path (dir + file) of data.

        RETURNS: (scan, file, repeat) numbers found in the data file path
            *) XxxxScan$$$$-RRRR_FFFF.txt --> (scan, file, repeat) = ($, F, R)
    """

    (scan_number, file_number, repeat_number) = (None, None, None)

    file_basename = os.path.basename(data_file_path)
    file_basename_parts = os.path.splitext(file_basename)

    file_basename_parts = file_basename_parts[0].rsplit('_', 1)
    if len(file_basename_parts) > 1:
        file_number = int(file_basename_parts[1])

    file_basename_parts = file_basename_parts[0].rsplit('-', 1)
    if len(file_basename_parts) > 1:
        repeat_number = int(file_basename_parts[1])

    file_basename_parts = file_basename_parts[0].rsplit('Scan', 1)
    if len(file_basename_parts) > 1:
        try:
            scan_number = int(file_basename_parts[1])
        except ValueError as e:
            # This is a new Beamline/Integrated scan
            file_basename_parts = file_basename_parts[1].rsplit(' ', 1)
            scan_number = int(file_basename_parts[0])
            if len(file_basename_parts) > 1:
                file_number = int(file_basename_parts[-1])
    
    return(scan_number, file_number, repeat_number)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def read_data_file(
        data_file_path,
        subpath_replace_dict=None,
        **kwargs
        ):

    """Loads the data from all files into a PANDAS DataFrame.

        data_file_path: full path of data file
        subpath_replace_dict: {old_subpath: new_subpath, ...}
            *) Used for locating input scan file

        RETURNS: PANDAS DataFrame containing data from imported file
    """

    if subpath_replace_dict is None:
        subpath_replace_dict = dict()

    logger.debug("\n\nRead_data_file...")

    logger.debug("Opening: {}".format(data_file_path))

    isTimeScan = False
    file_name = os.path.basename(data_file_path)
    if file_name.startswith("Time"):
        isTimeScan = True

    with open(data_file_path, 'r') as data_file:
        for (header_row, file_line) in enumerate(data_file):
            logger.debug("[{}]: {}".format(header_row, file_line))
            if file_line.startswith("Time") or file_line.startswith("Frame"):
                break
            if file_line[0].isdigit() and not isTimeScan:
                header_row -= 1
                break

    data = pd.read_table(
        data_file_path,
        delimiter='\t',
        header=header_row,
        skip_blank_lines=False,
        )

    data["filename"] = os.path.splitext(
        os.path.basename(data_file_path)
    )[0]

    if len(data) > 0:
        logger.debug("...filename: {}".format(data["filename"][0]))
    # logger.debug("...filename: {}".format(data["filename"].values[0]))

    df = data

    return df

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main():
    """The main routine."""

    print(__doc__ + "\n")

    return(0)
    

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    main()