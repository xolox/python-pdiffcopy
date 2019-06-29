# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: June 29, 2019
# URL: https://pdiffcopy.readthedocs.io

"""HTTP API for the ``pdiffcopy`` program."""

# Standard library modules.
import hashlib
import logging
import math
import multiprocessing
import os

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


def hash_generic(filename, block_size, concurrency):
    if concurrency == 1:
        return hash_serial(filename, block_size)
    else:
        return hash_parallel(filename, block_size, concurrency)


def hash_serial(filename, block_size):
    """Compute checksums of a file in blocks (serial)."""
    logger.info("Computing hashes of %s without concurrency ..", filename)
    offset = 0
    with open(filename) as handle:
        while True:
            data = handle.read(block_size)
            if not data:
                break
            context = hashlib.sha1()
            context.update(data)
            yield offset, context.hexdigest()
            offset += block_size


def hash_parallel(filename, block_size, concurrency):
    """Compute checksums of a file in blocks (parallel)."""
    logger.info("Computing hashes of %s with a concurrency of %s ..", filename, concurrency)
    filesize = os.path.getsize(filename)
    # Prepare for communication between processes.
    workers = []
    input_queue = multiprocessing.Queue(10)
    output_queue = multiprocessing.Queue(10)
    # Create a worker process to put tasks on the input queue.
    workers.append(
        multiprocessing.Process(target=enqueue_tasks, args=(input_queue, filename, filesize, block_size, concurrency))
    )
    # Create worker processes to hash the file in blocks.
    while len(workers) < concurrency:
        workers.append(Worker(block_size, input_queue, output_queue))
    # Start the worker processes.
    for worker in workers:
        worker.start()
    # Yield generated hashes from the output queue.
    for _ in range(int(math.ceil(filesize / float(block_size)))):
        filename, offset, digest = output_queue.get()
        yield offset, digest
    # Shutdown the worker processes.
    logger.debug("Joining hash workers ..")
    for worker in workers:
        worker.join()


def enqueue_tasks(input_queue, filename, file_size, block_size, concurrency):
    """Push tasks onto the input queue."""
    for offset in xrange(0, file_size, block_size):
        logger.debug("Input queue producer pushing job ..")
        input_queue.put((filename, offset))
    logger.debug("Input queue producer done!")
    for _ in range(concurrency):
        input_queue.put((None, None))
    input_queue.close()


class Worker(multiprocessing.Process):

    """
    Worker process to compute hashes of a file's blocks in parallel.

    The reason we define a custom :class:`multiprocessing.Process`
    and manage our own pool of workers (as opposed to just using
    :class:`multiprocessing.Pool`) is mainly for performance
    reasons (see for example the file handle cache).
    """

    def __init__(self, block_size, input_queue, output_queue):
        """Initialize a :class:`Worker` object."""
        # Initialize the superclass.
        super(Worker, self).__init__()
        # Initialize internal state.
        self.file_handles = {}
        # Store initializer arguments.
        self.block_size = block_size
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self):
        """Compute the hashes of requested blocks."""
        while True:
            logger.debug("Hash worker waiting for job ..")
            filename, offset = self.input_queue.get()
            if filename:
                logger.debug("Hash worker accepted job (offset=%i) ..", offset)
            else:
                logger.debug("Hash worker shutting down ..")
                break
            handle = self.get_handle(filename)
            handle.seek(offset)
            context = hashlib.sha1()
            context.update(handle.read(self.block_size))
            logger.debug("Hash worker pushing output ..")
            self.output_queue.put((filename, offset, context.hexdigest()))

    def get_handle(self, filename):
        """Get a file handle, reusing previous results."""
        handle = self.file_handles.get(filename)
        if handle is None:
            handle = open(filename)
            self.file_handles[filename] = handle
        return handle
