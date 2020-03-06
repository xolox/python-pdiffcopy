# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 6, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Custom exceptions raised by the :mod:`pdiffcopy` modules."""

# External dependencies.
from humanfriendly.text import compact

# Public identifiers that require documentation.
__all__ = ("BenchmarkAbortedError", "DependencyError", "ProgramError")


class ProgramError(Exception):

    """The base exception class for all custom exceptions raised by the :mod:`pdiffcopy` modules."""

    def __init__(self, text, *args, **kw):
        """
        Initialize a :class:`ProgramError` object.

        For argument handling see the :func:`~humanfriendly.text.compact()`
        function. The resulting string is used as the exception message.
        """
        message = compact(text, *args, **kw)
        super(ProgramError, self).__init__(message)


class BenchmarkAbortedError(ProgramError):

    """Raised when the operator doesn't give explicit permission to run the benchmark."""


class DependencyError(ProgramError):

    """Raised when client or server installation requirements are missing."""
