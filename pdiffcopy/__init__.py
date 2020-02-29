# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: February 29, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Configuration defaults for the ``pdiffcopy`` program."""

# Standard library modules.
import multiprocessing

# Semi-standard module versioning.
__version__ = '0.1'

BLOCK_SIZE = 1024 * 1024
"""The default block size to be used by ``pdiffcopy`` (1 MiB)."""

DEFAULT_CONCURRENCY = int(max(2, multiprocessing.cpu_count() / 3.0))
"""The default concurrency to be used by ``pdiffcopy`` (at least two, at most 1/3 of available cores)."""
