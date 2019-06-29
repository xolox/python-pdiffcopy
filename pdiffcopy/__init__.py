# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: June 29, 2019
# URL: https://pdiffcopy.readthedocs.io

"""Python API for the ``pdiffcopy`` program."""

# Standard library modules.
import logging
import multiprocessing

# Semi-standard module versioning.
__version__ = '0.1'

BLOCK_SIZE = 1024 * 1024
"""The default block size for hashing."""

DEFAULT_CONCURRENCY = max(2, multiprocessing.cpu_count() / 2)
"""The default concurrency for hashing."""

# Initialize a logger for this module.
logger = logging.getLogger(__name__)
