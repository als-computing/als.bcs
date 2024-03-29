#!/usr/bin/env python

"""bcs.scans.py

    Functions to inspect the BCS scan files that generated the data.
"""

import logging

logger = logging.getLogger(__name__)

from numpy import mod
import numpy as np
import pandas as pd


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CLASSES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class ScanFileNotFoundError(FileNotFoundError):
    """BCS Scan File was not found"""
    def __init__(
            self, *args: object, scan_file_path: str, data_file_path: str,
            ) -> None:
        message = (
            "Could not find the input scan file "
            f"'{scan_file_path}' for data file '{data_file_path}'; "
            "verify that it has not been moved or deleted."
        )
        super().__init__(message, *args)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_scan_file_path(data_file_path):
    """Extract the scan file from the data file header.

        data_file_path: Fully qualified file path (dir + file) of data.

        RETURNS: Fully qualified file path (dir + file) of scan input.
            "" = Not found
    """

    scan_file_used = False

    with open(data_file_path, 'r') as data_file:
        for (header_linenum, file_line) in enumerate(data_file):

            logger.debug(header_linenum, file_line)

            if file_line.startswith("From File"):
                scan_file_used = True
                continue
            if scan_file_used and file_line.rstrip():
                scan_file_path_raw = file_line.rstrip()
                scan_file_path = scan_file_path_raw.replace("\\", "/")
                if scan_file_path == "All Magnets":
                    # This is a quirk of a new file format
                    continue
                logger.debug("Scan file: {:s}".format(scan_file_path))
                return(scan_file_path)
                break
            if file_line[0].isdigit():
                # This data file did not use an input scan file
                return("")

    return("")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_scan_header_line_number(scan_file_path):
    """Extract line number of motor headers from scan file.

        scan_file_path: Fully qualified path (dir + file) of scan file.

        RETURNS: Zero-based line number of motor header row.
            -1 = Not found
    """

    with open(scan_file_path, 'r') as scan_file:
        for (header_linenum, file_line) in enumerate(scan_file):

            logger.debug(header_linenum, file_line)

            # if file_line[0].isdigit() or file_line.lower().startswith("file"):
            if (
                    file_line[0].isdigit() or 
                    file_line.lower().startswith("file") or
                    (file_line[0]=='-' and file_line[1].isdigit())
                    ):
                header_linenum -= 1
                return(header_linenum)

    return(-1)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_scan_line_numbers(scan_file_path, file_number=1):
    """Extract line numbers corresponding to file_number from scan file.

        scan_file_path: Fully qualified path (dir + file) of scan file.
        file_number: One-based file number (extracted from data file path).

        RETURNS: Tuple of zero-based line numbers (first, last) of motor
                    positions for output file_number specified.
            first = -1, if file_number not found
    """

    (first_line, last_line) = (-1, -1)  # Not found

    header_linenum = get_scan_header_line_number(scan_file_path)
    if header_linenum == -1:
        return(first_line, last_line)

    file_number = file_number or 1  # If file_number is None
    output_file_number = 1
    if output_file_number == file_number:
        next_linenum = header_linenum + 1
        (first_line, last_line) = (next_linenum, next_linenum)

    with open(scan_file_path, 'r') as scan_file:
        for (linenum, file_line) in enumerate(scan_file):

            # Move past header line
            if linenum <= header_linenum:
                continue

            logger.debug(linenum, file_line)

            if file_line.lower().startswith("file"):
                output_file_number += 1
                if output_file_number == file_number:
                    next_linenum = linenum + 1
                    (first_line, last_line) = (next_linenum, next_linenum)
                elif output_file_number > file_number:
                    return (first_line, last_line)
            else:
                if output_file_number == file_number:
                    last_line = linenum
        else:
            # This is a new Beamline/Integrated scan
            max_file_number = output_file_number
            if file_number > max_file_number:
                file_number = mod(file_number, max_file_number)
                if file_number == 0:
                    file_number = max_file_number
                (first_line, 
                 last_line,
                 ) = get_scan_line_numbers(scan_file_path, file_number)
    return (first_line, last_line)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def import_scan_file(scan_file_path, file_number=1):
    """Import motor positions from scan file into PANDAS DataFrame.

        scan_file_path: Fully qualified path (dir + file) of scan file.
        file_number: One-based file number (extracted from data file path).

        RETURNS: PANDAS DataFrame of imported motor positions
                    for output file_number specified.
    """

    header_linenum = get_scan_header_line_number(scan_file_path)
    (first_linenum, last_linenum) = get_scan_line_numbers(
        scan_file_path, file_number)

    skiprows = np.arange(header_linenum + 1, first_linenum)
    logger.debug("skiprows: {}".format(skiprows))

    nrows = 1 + last_linenum - first_linenum
    logger.debug("nrows: {}".format(nrows))

    try:
        df = pd.read_table(
            scan_file_path,
            delimiter='\t',
            header=header_linenum,
            skip_blank_lines=False,
            skiprows=skiprows,
            nrows=nrows,
        )
    except pd.errors.ParserError:
        # First line is a comment, and header line does not end with '\t'
        df = pd.read_table(
            scan_file_path,
            delimiter='\t',
            header=header_linenum,
            skip_blank_lines=False,
            skiprows=skiprows + [first_linenum],
            nrows=(nrows - 1),
        )
    # Ignore comment lines in the scan file
    def is_comment(value: str):
        return value.strip().startswith('#')
    comment_rows = df.iloc[:, 0].astype(str).apply(is_comment)
    comment_indices = df[comment_rows].index
    df.drop(comment_indices, inplace=True)
    
    # Check whether this is a Flying Scan; extract flying motor name
    if header_linenum > 0:
        with open(scan_file_path, 'r') as scan_file:
            first_line = scan_file.readline().rstrip()
            logger.debug(f"first_line: {first_line}")
        
        # first_line_parts = first_line.lower().rsplit('flying ', 1)
        first_line_parts = first_line.replace(
            'flying ', 'Flying ').rsplit('Flying ', 1)
        logger.debug(f"first_line_parts #1: {first_line_parts}")
        if len(first_line_parts) > 1:
            first_line = first_line_parts[-1]
            first_line_parts = first_line.rsplit('(', 1)
            logger.debug(f"first_line_parts #2: {first_line_parts}")
            if len(first_line_parts) > 1:
                first_line = first_line_parts[0]
                flying_name = first_line.strip()
                logger.debug(f"flying_name: {flying_name}")
                
                # Add Flying Motor to column names
                if df.columns[-1].startswith("Unnamed"):
                    df.columns = [*df.columns[:-1], flying_name]
                else:
                    # Repair scan file with correct number of columns
                    index_name = df.columns[0]
                    col_names = [*df.columns[1:], flying_name]
                    df.columns = col_names
                    df.index.rename(index_name, inplace=True)
                    df = df.reset_index()
                print(f'{df.columns=}')

    return(df)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def is_flying_scan(scan_file_path, file_number=1):
    """Check whether a flying scan generated the output file_number specified

        scan_file_path: Fully qualified path (dir + file) of scan file.
        file_number: One-based file number (extracted from data file path).

        RETURNS: True if a flying motor is found; False otherwise
    """

    header_linenum = get_scan_header_line_number(scan_file_path)
    (first_linenum, last_linenum) = get_scan_line_numbers(
        scan_file_path, file_number)

    skiprows = np.arange(header_linenum + 1, first_linenum)
    logger.debug("skiprows: {}".format(skiprows))

    nrows = 1 + last_linenum - first_linenum
    logger.debug("nrows: {}".format(nrows))

    try:
        df = pd.read_table(
            scan_file_path,
            delimiter='\t',
            header=header_linenum,
            skip_blank_lines=False,
            skiprows=skiprows,
            nrows=nrows,
        )
    except pd.errors.ParserError:
        # First line is a comment, and header line does not end with '\t'
        df = pd.read_table(
            scan_file_path,
            delimiter='\t',
            header=header_linenum,
            skip_blank_lines=False,
            skiprows=skiprows + [first_linenum],
            nrows=(nrows - 1),
        )
    
    # Check whether this is a Flying Scan; extract flying motor name
    if header_linenum > 0:
        with open(scan_file_path, 'r') as scan_file:
            first_line = scan_file.readline().rstrip()
            logger.debug(f"first_line: {first_line}")
        
        # first_line_parts = first_line.lower().rsplit('flying ', 1)
        first_line_parts = first_line.replace(
            'flying ', 'Flying ').rsplit('Flying ', 1)
        logger.debug(f"first_line_parts #1: {first_line_parts}")
        if len(first_line_parts) > 1:
            first_line = first_line_parts[-1]
            first_line_parts = first_line.rsplit('(', 1)
            logger.debug(f"first_line_parts #2: {first_line_parts}")
            if len(first_line_parts) > 1:
                first_line = first_line_parts[0]
                flying_name = first_line.strip()
                logger.debug(f"flying_name: {flying_name}")

                return True
                
                # Return name of flying motor also?
                # return(True, flying_name)

    return False

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