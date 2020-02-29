# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: February 29, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Serial and parallel file hashing functions."""

# Standard library modules.
import hashlib
import logging
import math
import multiprocessing
import os

# External dependencies.
from six.moves import range

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


def hash_generic(filename, block_size, concurrency, method):
    """Automatically pick between :func:`hash_serial()` and :func:`hash_parallel()`."""
    if concurrency == 1:
        return hash_serial(filename, block_size, method)
    else:
        return hash_parallel(filename, block_size, method, concurrency)


def hash_serial(filename, block_size, method):
    """Compute checksums of a file in blocks (serial)."""
    logger.info("Computing hashes of %s without concurrency ..", filename)
    offset = 0
    with open(filename, "rb") as handle:
        while True:
            data = handle.read(block_size)
            if not data:
                break
            context = hashlib.new(method)
            context.update(data)
            yield offset, context.hexdigest()
            offset += block_size


def hash_parallel(filename, block_size, method, concurrency):
    """Compute checksums of a file in blocks (parallel)."""
    logger.info("Computing hashes of %s with a concurrency of %s ..", filename, concurrency)
    filesize = os.path.getsize(filename)
    # Prepare for communication between processes.
    input_queue = multiprocessing.Queue(10)
    output_queue = multiprocessing.Queue(10)
    # Create an initial worker process to populate the input queue.
    pool = [multiprocessing.Process(target=enqueue_tasks, args=(input_queue, filesize, block_size, concurrency))]
    # Spawn worker processes to hash the file in blocks.
    while len(pool) < (concurrency + 1):
        pool.append(
            Worker(
                block_size=block_size,
                filename=filename,
                input_queue=input_queue,
                method=method,
                output_queue=output_queue,
            )
        )
    # Start the worker processes.
    for worker in pool:
        worker.start()
    # Yield generated hashes from the output queue.
    for _ in range(int(math.ceil(filesize / float(block_size)))):
        yield output_queue.get()
    # Shutdown the worker processes.
    logger.debug("Joining workers ..")
    for worker in pool:
        worker.join()


def enqueue_tasks(input_queue, filesize, block_size, concurrency):
    """Push tasks onto the input queue."""
    for offset in range(0, filesize, block_size):
        logger.debug("Input queue producer pushing job ..")
        input_queue.put(offset)
    logger.debug("Input queue producer done!")
    # Push one sentinel token for each of the workers so we can
    # guarantee the workers don't block when the work is done.
    for _ in range(concurrency):
        input_queue.put(None)
    input_queue.close()


class Worker(multiprocessing.Process):

    """
    Worker process to compute hashes of a file's blocks in parallel.

    The reason we define a custom :class:`multiprocessing.Process`
    and manage our own pool of workers (as opposed to just using
    :class:`multiprocessing.pool.Pool`) is mainly for performance
    reasons (see for example the file handle cache).
    """

    def __init__(self, filename, block_size, method, input_queue, output_queue):
        """Initialize a :class:`Worker` object."""
        # Initialize the superclass.
        super(Worker, self).__init__()
        # Store initializer arguments.
        self.block_size = block_size
        self.input_queue = input_queue
        self.method = method
        self.output_queue = output_queue
        # Open a handle to the file.
        self.handle = open(filename, "rb")

    def run(self):
        """Compute the hashes of requested blocks."""
        while True:
            logger.debug("Hash worker waiting for job ..")
            offset = self.input_queue.get()
            if offset is not None:
                logger.debug("Hash worker accepted job (offset=%i) ..", offset)
            else:
                logger.debug("Hash worker shutting down ..")
                break
            self.handle.seek(offset)
            context = hashlib.new(self.method)
            context.update(self.handle.read(self.block_size))
            logger.debug("Hash worker pushing output ..")
            self.output_queue.put((offset, context.hexdigest()))
