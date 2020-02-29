"""Utility functions used by the client as well as the server."""

# Standard library modules.
import errno
import logging
import os

# External dependencies.
from humanfriendly import format_size

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


def get_file_info(filename):
    size = get_file_size(filename)
    if size is not None:
        return {"size": size}
    else:
        return {}


def get_file_size(filename):
    try:
        return os.path.getsize(filename)
    except OSError as e:
        if e.errno == errno.ENOENT:
            return None
        raise


def read_block(filename, offset, size):
    """
    Read a block of data from a local file.

    :param filename: An absolute filename (a string).
    :param offset: The byte offset were reading starts (an integer).
    :param size: The number of bytes to read (an integer).
    :returns: The read data (a byte string).
    """
    logger.debug("Reading %s block %s (%i bytes) ..", filename, offset, size)
    with open(filename, "rb") as handle:
        handle.seek(offset)
        return handle.read(size)


def resize_file(filename, size):
    """
    Create or resize a local file, in preparation for synchronizing its contents.

    :param filename: An absolute filename (a string).
    :param size: The new size in bytes (an integer).
    """
    try:
        handle = open(filename, "r+b")
        logger.info("Resizing %s to %s (%s bytes) ..", filename, format_size(size), size)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        logger.info("Creating %s with size %s (%s bytes) ..", filename, format_size(size), size)
        handle = open(filename, "wb")
    try:
        handle.truncate(size)
    finally:
        handle.close()


def write_block(filename, offset, data):
    """
    Write a block of data to a local file.

    :param filename: An absolute filename (a string).
    :param offset: The byte offset were writing starts (an integer).
    :param data: The data to write (a byte string).
    """
    logger.debug("Writing %s block %s (size: %s) ..", filename, offset, len(data))
    with open(filename, "r+b") as handle:
        handle.seek(offset)
        handle.write(data)
        handle.flush()
