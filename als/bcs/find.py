#!/usr/bin/env python

"""bcs.find.py

    Functions to simplify finding BCS files.
"""

import logging

import sys
import os

from datetime import datetime, date, time, timedelta
from dateutil import relativedelta as rel_date
import pytz

from numpy import array
import numpy as np
import glob
import pandas as pd


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def replace_subpath(file_path, subpath_replace_dict):
    """Replace specified parts of file path according to supplied dictionary.

        file_path: Fully qualified file path (dir + file).
        subpath_replace_dict: {old_subpath: new_subpath, ...}.
            *) All replacements performed, in order of items().
            *) Suggested to use collections.OrderedDict when order important.

        RETURNS: Fully qualified file path (dir + file) after all replacements.
    """

    new_file_path = file_path

    for (old_subpath, new_subpath) in subpath_replace_dict.items():

        new_file_path = new_file_path.replace(old_subpath, new_subpath)

        logging.debug(
                "Scan file (updated path): {:s} <WAS: {:s}, NOW: {:s}>".format(
                new_file_path, old_subpath, new_subpath,
                )
            )

    logging.debug("Scan file (updated path): {:s}".format(new_file_path))

    return(new_file_path)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def daterange(start_date, end_date, endpoint=True):
    """Iterable generator for range of dates. RETURNS datetime.date"""
    if endpoint:
        last_day = 1
    else:
        last_day = 0
    for ndays in range(int ((end_date - start_date).days + last_day)):
        yield start_date + timedelta(days=ndays)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def find_data_files_in_date_range(
        data_scan_numbers = None,
        first_date = None,
        last_date = None,
        data_file_base = "TrajScan",
        data_path_base = "",
        ):
    """Find all BCS data files matching date rage and name criteria

        *) Search for all BCS data files created: first_date to last_date
           ...which contain data_file_base in the filename

        data_scan_numbers: list or array of scan numbers for data files
            *) If None specified, return all files matching data_file_base
        first_date: datetime.date object for path name of earliest data files
            *) Defaults to the past week
        last_date: datetime.date object for path name of most recent data files
            *) Defaults to today
        data_file_base: Base name for data files
            *) e.g., "TrajScan", "SigScan", "TimeScan", "InstrumentScan"
        data_path_base: Base path for data file search

        RETURNS: list of matching filenames
    """

    if data_scan_numbers is None:
        data_file_search_strings = array([data_file_base])
    else:
        data_scan_numbers = array(data_scan_numbers)
        data_file_search_strings = array([
            "{}{:4d}".format(data_file_base, int(data_scan_number))
            for data_scan_number in data_scan_numbers
            ])

    if first_date is None:
        first_date = date.today() - timedelta(days=7)
        # first_date = date.today() - rel_date.relativedelta((days=7)

    if last_date is None:
        last_date = date.today()

    matching_file_paths = list()

    for data_file_date in daterange(first_date, last_date, endpoint=True):
        for data_file_search_string in data_file_search_strings:
            data_subpath_base = "{}/{}/".format(
                data_path_base,
                data_file_date.strftime("%Y"),
                )
            if not os.path.exists(data_subpath_base):
                data_subpath_base = data_path_base
            data_path_search_string = "{}/{}/{}*".format(
                data_subpath_base,
                data_file_date.strftime("%y%m%d"),
                data_file_search_string,
                )
            logging.debug("Search: " + data_path_search_string)
            match_results = glob.glob(data_path_search_string)
            if len(match_results) > 0:
                matching_file_paths += match_results

    logging.info("\n\nfind_data_files_in_date_range...")
    logging.info("len(matching_file_paths): {}".format(len(matching_file_paths)))

    return(matching_file_paths)

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