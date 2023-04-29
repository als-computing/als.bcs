#!/usr/bin/env python

"""bcs.errors.py

    Exceptions raised while processing BCS files.
"""

import logging

logger = logging.getLogger(__name__)

from typing import Generic, TypeVar, Union
from warnings import warn


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# TYPES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ErrorOrWarning = TypeVar(
    "ErrorOrWarning", bound=Union[Exception, Warning],
)


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
class ScanFileHeaderNotFoundError(EOFError):
    """BCS Scan File header information was not found"""
    def __init__(self, *args: object, scan_file_path: str) -> None:
        message = (
            "Could not find the header information within input scan file: "
            f"'{scan_file_path}'."
        )
        super().__init__(message, *args)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class ScanFileRowException(Generic[ErrorOrWarning]):
    """BCS Scan File row has invalid value(s) or format"""
    def __init__(
            self, 
            *args: object, 
            scan_file_path: str, 
            file_number: int = -1,
            step_number: int = -1,
            description: str = "",
            ) -> None:
        messages = [
            self._error_message(
                *args, 
                scan_file_path=scan_file_path, 
                file_number=file_number,
                step_number=step_number,
                description=description,
                ),
            " Check for missing values, spaces instead of tabs, or"
            " extra header rows."
        ]
        message = ''.join(messages)
        super().__init__(message, *args)

    def _error_message(
            self, 
            *args: object, 
            scan_file_path: str, 
            file_number: int = -1,
            step_number: int = -1,
            description: str = "",
            ) -> str:
        messages = [
            "Invalid value or format found in input scan file: ",
            f"'{scan_file_path}'"
        ]
        if file_number:
            messages.append(f"; file output number: {file_number}")
        if step_number:
            messages.append(f"; step number {step_number}")
        messages.append(".")
        if description:
            messages.append(f" {description}")
            if description[-1] != ".":
                messages.append(".")
        message = ''.join(messages)

        return message

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ScanFileRowError = ScanFileRowException[ValueError]
ScanFileRowWarning = ScanFileRowException[UserWarning]


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def warn_or_raise(warning: Warning, raise_warning: bool=False) -> None:
    """Report a warning or raise it as an exception.

        warning: Warning object to report.
        raise_warning: If True, raise warning as an Exception;
            otherwise report the warning.
    """
    if raise_warning:
        raise warning
    else:
        warn(warning)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~