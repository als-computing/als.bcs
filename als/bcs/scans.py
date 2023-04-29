#!/usr/bin/env python

"""bcs.scans.py

    Functions to inspect the BCS scan files that generated the data.
"""

import logging

logger = logging.getLogger(__name__)

from typing import Optional, Sequence, Tuple, Union

import pandas as pd

from .errors import (
    ScanFileHeaderNotFoundError, ScanFileNotFoundError, 
    ScanFileRowError, ScanFileRowWarning,
    warn_or_raise,
)


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
                if ":/" in scan_file_path:
                    logger.debug("Scan file: {:s}".format(scan_file_path))
                    return(scan_file_path)
                else:
                    # This line indicates the pause/stop motor name or is blank
                    scan_file_path = ""
                    continue
            if file_line[0].isdigit():
                # This data file did not use an input scan file
                return("")

    return("")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_scan_header_line_number(scan_file_path, raise_exception=True):
    """Extract line number of motor headers from scan file.

        scan_file_path: Fully qualified path (dir + file) of scan file.
        raise_exception: When header line is not found,
            * raise ScanFileHeaderNotFoundError() if True;
            * otherwise return(-1)

        RETURNS: Zero-based line number of motor header row.
            -1 = Not found (if raise_exception == False)
        
        RAISES: ScanFileHeaderNotFoundError() if raise_exception == True
                and a header line is not found in the scan file
    """
    consecutive_skipped_lines = 0
    def is_blank_or_comment(file_line: str) -> bool:
        line_content = file_line.strip()
        if (not line_content) or (line_content.startswith('#')):
            return True
        else:
            return False

    with open(scan_file_path, 'r') as scan_file:
        for (header_linenum, file_line) in enumerate(scan_file):

            logger.debug(header_linenum, file_line)

            if is_blank_or_comment(file_line):
                consecutive_skipped_lines += 1
            else:
                if (
                        file_line[0].isdigit() or 
                        file_line.lower().startswith("file") or
                        (file_line[0]=='-' and file_line[1].isdigit())
                        ):
                    header_linenum -= consecutive_skipped_lines + 1
                    return(header_linenum)
                
                consecutive_skipped_lines = 0

    if raise_exception:
        raise ScanFileHeaderNotFoundError(scan_file_path)
    else:
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

    file_number_NOT_FOUND = (-1, -1)
    (first_line, last_line) = file_number_NOT_FOUND

    try:
        header_linenum = get_scan_header_line_number(scan_file_path)
    except ScanFileHeaderNotFoundError:
        return file_number_NOT_FOUND

    file_number = file_number or 1  # If file_number is None
    output_file_number = 1
    if output_file_number == file_number:
        next_linenum = header_linenum + 1
        (first_line, last_line) = (next_linenum, next_linenum)

    with open(scan_file_path, 'r') as scan_file:
        subscan_is_empty = True

        for (linenum, file_line) in enumerate(scan_file):

            # Move past the header
            if linenum <= header_linenum:
                continue

            logger.debug(linenum, file_line)

            if file_line.lower().startswith("file"):
                if not subscan_is_empty:
                    output_file_number += 1
                    subscan_is_empty = True
                if output_file_number == file_number:
                    next_linenum = linenum + 1
                    (first_line, last_line) = (next_linenum, next_linenum)
                elif output_file_number > file_number:
                    return (first_line, last_line)
            else:
                line_content = file_line.strip()
                if line_content and not line_content.startswith('#'):
                    subscan_is_empty = False
                    if output_file_number == file_number:
                        last_line = linenum
                elif (not line_content) and subscan_is_empty:
                    # Avoid parse error when subscan starts with an empty line
                    first_line += 1
                    last_line += 1
        else:
            if subscan_is_empty:
                return file_number_NOT_FOUND
    return (first_line, last_line)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def scan_file_motors(
        scan_file_path: str, 
        header_linenum: Optional[int] = None,
        ) -> Sequence[str]:
    """Extract the motor names from a scan file.

        scan_file_path: Fully qualified path (dir + file) of scan file.
        header_linenum: Zero-based line number of motor header row.

        RETURNS: List of motor names from the scan file.
    """

    header_linenum = (
        header_linenum or get_scan_header_line_number(scan_file_path)
    )
    df = parse_scan_file_lines(
        scan_file_path,
        header_linenum=header_linenum,
        )
    
    return df.columns.values

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
    
    if (first_linenum < 0) or (last_linenum < 0):
        raise IndexError(
            f"Data file number ({file_number}) exceeds the number" \
            f" of scan file outputs; scan file may have been modifed after" \
            f" data was captured: {scan_file_path}"
            )

    skiprows = range(header_linenum + 1, first_linenum)
    logger.debug("skiprows: {}".format(skiprows))

    nrows = 1 + last_linenum - first_linenum
    logger.debug("nrows: {}".format(nrows))
    
    return parse_scan_file_lines(
        scan_file_path,
        header_linenum=header_linenum,
        skiprows=skiprows,
        nrows=nrows,
        ) 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def parse_scan_file_lines(
        scan_file_path: str,
        header_linenum: int = 0,
        skiprows: Optional[Sequence[int]] = None,
        nrows: int = 0,
        ) -> pd.DataFrame:
    """Import motor positions from scan file into PANDAS DataFrame.

        scan_file_path: Fully qualified path (dir + file) of scan file.
        header_linenum: Zero-based line number of motor header row.
        skiprows: List of rows to ignore when parsing scan file.
        nrows: number of rows to read after the header.

        RETURNS: PANDAS DataFrame of imported motor positions
                    for output file_number specified.
    """

    skiprows = list(skiprows or ())

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
        if skiprows:
            first_linenum = skiprows[-1] + 1
        else:
            first_linenum = header_linenum + 1
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
    # Ignore empty lines in the scan file
    df.dropna(how='all', inplace=True)

    # There can be at most ONE unnamed column (count time or flying motor)
    unnamed_columns = df.columns[
        df.columns.str.startswith("Unnamed")
        ]
    excess_unnamed_columns = unnamed_columns[1:]
    df.drop(columns=excess_unnamed_columns, inplace=True)
    
    # Check whether this is a Flying Scan; extract flying motor name
    (_, flying_name) = is_flying_scan(
        scan_file_path, 
        return_motor=True,
        header_linenum=header_linenum,
        )
    if flying_name:
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
        logger.debug(f'{df.columns=}')

    return(df)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def is_flying_scan(
        scan_file_path: str, 
        /, 
        file_number: Optional[int] = None, 
        *,
        return_motor: bool = False,
        header_linenum: Optional[int] = None,
        ) -> Union[bool, Tuple[bool, Optional[str]]]:
    """Check whether a flying scan generated the output file_number specified.

        scan_file_path: Fully qualified path (dir + file) of scan file.
        file_number: One-based file number (extracted from data file path).
                     [DEPRECATED]: This parameter has no effect.
        return_motor: If True, also return the name of the flying motor.
        header_linenum: Zero-based line number of motor header row.

        RETURNS: True if a flying motor is found; False otherwise.
            If return_motor == True, this function returns a tuple:
            * (True, name_of_flying_motor: str) if a flying motor is found;
            * (False, None) otherwise.
    """

    header_linenum = (
        header_linenum or get_scan_header_line_number(scan_file_path)
    )
    if file_number is not None:
        deprecated = DeprecationWarning(
            "'file_number' parameter is no longer used by 'is_flying_scan()'"
        )
        warn_or_raise(deprecated)

    # Check whether this is a Flying Scan; extract flying motor name
    if header_linenum > 0:
        with open(scan_file_path, 'r') as scan_file:
            first_line = scan_file.readline().rstrip()
            logger.debug(f"first_line: {first_line}")
        
        first_line_parts = first_line.replace(
            'flying ', 'Flying ').rsplit('Flying ', 1)
        logger.debug(f"first_line_parts #1: {first_line_parts}")

        if len(first_line_parts) > 1:
            # This is a flying scan
            if not return_motor:
                return True
            
            first_line = first_line_parts[-1]
            first_line_parts = first_line.rsplit('(', 1)
            logger.debug(f"first_line_parts #2: {first_line_parts}")

            if len(first_line_parts) > 1:
                first_line = first_line_parts[0]
                flying_name = first_line.strip()
                logger.debug(f"flying_name: {flying_name}")

                return(True, flying_name)

    if return_motor:
        return (False, None)
    else:
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