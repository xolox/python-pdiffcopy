# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 6, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Adaptations of :mod:`multiprocessing` that make it easy to do the right thing."""

# Standard library modules.
import logging
import multiprocessing
import time

# External dependencies.
import coloredlogs
from property_manager import PropertyManager, lazy_property, required_property
from six.moves import queue

# Public identifiers that require documentation.
__all__ = ("Promise", "WorkerPool", "generator_adapter", "logger", "worker_adapter")

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


class Promise(multiprocessing.Process):

    """Execute a Python function in a child process and retrieve its return value."""

    def __init__(self, **options):
        """
        Initialize a :class:`Promise` object.

        The initializer arguments are the same as for
        :func:`multiprocessing.Process.__init__()`.

        The child process is started automatically.
        """
        super(Promise, self).__init__(**options)
        self.log_level = coloredlogs.get_level()
        self.queue = multiprocessing.Queue()
        self.start()

    def run(self):
        """Run the target function in a newly spawned child process."""
        try:
            initialize_child(self.log_level)
            logger.debug("Child process calling function ..")
            result = self._target(*self._args, **self._kwargs)
            logger.debug("Child process communicating return value ..")
            self.queue.put(result)
            logger.debug("Child process is done, exiting ..")
        except BaseException as e:
            logger.exception("Child process got exception, will re-raise in parent!")
            self.queue.put(e)

    def join(self):
        """Get the return value and wait for the child process to finish."""
        logger.debug("Parent process waiting for return value ..")
        result = self.queue.get()
        logger.debug("Parent process joining child process ..")
        super(Promise, self).join()
        if isinstance(result, BaseException):
            logger.debug("Parent process propagating exception ..")
            raise result
        else:
            logger.debug("Parent process reporting return value ..")
            return result


class WorkerPool(PropertyManager):

    """Simple to use worker pool implementation using :mod:`multiprocessing`."""

    @lazy_property
    def all_processes(self):
        """A list with all :class:`multiprocessing.Process` objects used by the pool."""
        return [self.generator_process] + self.worker_processes

    @required_property
    def concurrency(self):
        """The number of processes allowed to run simultaneously (an integer)."""

    @required_property
    def generator_fn(self):
        """A user defined generator to populate :attr:`input_queue`."""

    @lazy_property
    def generator_process(self):
        """A :class:`multiprocessing.Process` object to run :attr:`generator_fn`."""
        return multiprocessing.Process(
            target=generator_adapter,
            kwargs=dict(
                concurrency=self.concurrency,
                generator_fn=self.generator_fn,
                input_queue=self.input_queue,
                log_level=self.log_level,
            ),
        )

    @lazy_property
    def input_queue(self):
        """The input queue (a :class:`multiprocessing.Queue` object)."""
        return multiprocessing.Queue(self.concurrency)

    @required_property
    def log_level(self):
        """
        The logging level to configure in child processes (an integer).

        Defaults to the current log level in the parent process at the point
        when the worker processes are created.
        """
        return coloredlogs.get_level()

    @lazy_property
    def output_queue(self):
        """The output queue (a :class:`multiprocessing.Queue` object)."""
        return multiprocessing.Queue(self.concurrency)

    @required_property
    def polling_interval(self):
        """The time to wait between checking :attr:`output_queue` (a floating point number, defaults to 0.1 second)."""
        return 0.1

    @required_property
    def worker_fn(self):
        """A user defined worker function to consume :attr:`input_queue` and populate :attr:`output_queue`."""

    @lazy_property
    def worker_processes(self):
        """A list of :class:`multiprocessing.Process` objects to run :attr:`worker_fn`."""
        return [
            multiprocessing.Process(
                target=worker_adapter,
                kwargs=dict(
                    input_queue=self.input_queue,
                    log_level=self.log_level,
                    output_queue=self.output_queue,
                    worker_fn=self.worker_fn,
                ),
            )
            for i in range(self.concurrency)
        ]

    def __iter__(self):
        """Initialize the generator and worker processes and start yielding values from the :attr:`output_queue`."""
        # Start emptying the output queue to keep the workers busy (if we don't
        # then everything will block as soon as $concurrency values have been
        # pushed onto the output queue).
        logger.debug("Starting up worker pool with concurrency %s ..", self.concurrency)
        while any(p.is_alive() for p in self.all_processes):
            # Get the next value from the output queue.
            try:
                logger.debug("Waiting for value on output queue ..")
                yield self.output_queue.get(timeout=self.polling_interval)
            except queue.Empty:
                logger.debug("Got empty output queue, backing off ..")
                time.sleep(self.polling_interval)
        logger.debug("All worker processes have returned.")
        # Check if any values remain in the output queue at this point.
        while not self.output_queue.empty():
            logger.debug("Flushing output queue ..")
            yield self.output_queue.get()
        logger.debug("Worker pool has finished.")

    def __enter__(self):
        """Start up the generator and worker processes."""
        for i, worker in enumerate(self.all_processes, start=1):
            logger.debug("Starting worker process #%i ..", i)
            worker.start()
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        """Terminate any child processes that are still alive."""
        for worker in self.all_processes:
            if worker.is_alive():
                # Terminate workers that are still alive.
                worker.terminate()
            else:
                # Join workers that have returned in order to cleanup associated resources and dump
                # coverage statistics when this is being run as part of the test suite, for details
                # see https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html.
                worker.join()


def generator_adapter(concurrency, generator_fn, input_queue, log_level):
    """Adapter function for the generator process."""
    initialize_child(log_level)
    # Populate the input queue from the generator function.
    for value in generator_fn():
        logger.debug("Generator putting value onto input queue (%s) ..", value)
        input_queue.put(value)
    # Push one sentinel token for each worker process.
    for i in range(concurrency):
        logger.debug("Generator putting sentinel onto input queue  ..")
        input_queue.put(None)
    # Let multiprocessing know we've filled up the input queue.
    input_queue.close()
    logger.debug("Generator function is finished.")


def initialize_child(log_level=logging.INFO):
    """Initialize a child process created using :mod:`multiprocessing`."""
    coloredlogs.install(level=log_level)


def worker_adapter(input_queue, log_level, output_queue, worker_fn):
    """Adapter function for the worker processes."""
    initialize_child(log_level)
    while True:
        # Get the next value to process from the input queue.
        logger.debug("Worker waiting for value on input queue ..")
        input_value = input_queue.get()
        # Check for sentinel values.
        if input_value is None:
            logger.debug("Worker got sentinel value, exiting ..")
            break
        # Process the value using the worker function.
        logger.debug("Worker applying user defined function to value: %s", input_value)
        output_value = worker_fn(input_value)
        # Put the new value on the output queue.
        logger.debug("Worker putting value on output queue: %s", output_value)
        output_queue.put(output_value)
