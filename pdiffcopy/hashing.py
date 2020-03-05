# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 5, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Parallel hashing of files using :mod:`multiprocessing`."""

# Standard library modules.
import functools
import hashlib
import logging
import os

# External dependencies.
from six.moves import range

# Modules included in our package.
from pdiffcopy.mp import WorkerPool

# Public identifiers that require documentation.
__all__ = ("compute_hashes", "hash_worker", "logger")

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


def compute_hashes(filename, block_size, method, concurrency):
    """Compute checksums of a file in blocks (parallel)."""
    logger.info("Computing hashes of %s with a concurrency of %s ..", filename, concurrency)
    with WorkerPool(
        concurrency=concurrency,
        generator_fn=functools.partial(range, 0, os.path.getsize(filename), block_size),
        worker_fn=functools.partial(hash_worker, block_size=block_size, filename=filename, method=method),
    ) as pool:
        for offset, digest in pool:
            yield offset, digest


def hash_worker(offset, block_size, filename, method):
    """Worker function to be run in child processes."""
    with open(filename, "rb") as handle:
        handle.seek(offset)
        context = hashlib.new(method)
        context.update(handle.read(block_size))
        return offset, context.hexdigest()
