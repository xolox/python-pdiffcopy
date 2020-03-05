# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 5, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Integration with :mod:`multiprocessing`."""

# Standard library modules.
import logging
import multiprocessing

# Public identifiers that require documentation.
__all__ = ("Promise", "logger")

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


class Promise(multiprocessing.Process):

    """Execute a Python function in a subprocess and retrieve its return value."""

    def __init__(self, **options):
        """
        Initialize a :class:`Promise` object.

        The initializer arguments are the same as for
        :func:`multiprocessing.Process.__init__()`.

        The subprocess is started automatically.
        """
        super(Promise, self).__init__(**options)
        self.queue = multiprocessing.Queue()
        self.start()

    def run(self):
        """Run the target function in a newly spawned subprocess."""
        try:
            logger.debug("Child process calling function ..")
            result = self._target(*self._args, **self._kwargs)
            logger.debug("Child process communicating return value ..")
            self.queue.put(result)
            logger.debug("Child process is done, exiting ..")
        except BaseException as e:
            logger.exception("Child process got exception, will re-raise in parent!")
            self.queue.put(e)

    def join(self):
        """Get the return value and wait for the subprocess to finish."""
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
