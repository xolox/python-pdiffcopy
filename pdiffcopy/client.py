# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 6, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Parallel, differential file copy client."""

# Standard library modules.
import functools
import os
import subprocess

# External dependencies.
import requests
from humanfriendly import Timer, format_size
from humanfriendly.prompts import prompt_for_confirmation
from humanfriendly.tables import format_pretty_table
from humanfriendly.terminal import output
from humanfriendly.terminal.spinners import Spinner
from humanfriendly.text import compact, format, pluralize
from property_manager import PropertyManager, cached_property, mutable_property, set_property
from six.moves.urllib.parse import urlencode, urlparse, urlunparse
from verboselogs import VerboseLogger

# Modules included in our package.
from pdiffcopy import BLOCK_SIZE, DEFAULT_CONCURRENCY, DEFAULT_PORT
from pdiffcopy.exceptions import BenchmarkAbortedError
from pdiffcopy.hashing import compute_hashes
from pdiffcopy.mp import Promise, WorkerPool
from pdiffcopy.utils import get_file_info, read_block, resize_file, write_block

# Public identifiers that require documentation.
__all__ = ("Client", "get_hashes_fn", "Location", "logger", "transfer_block_fn")

# Initialize a logger for this module.
logger = VerboseLogger(__name__)


class Client(PropertyManager):

    """Python API for the client side of the ``pdiffcopy`` program."""

    @mutable_property
    def benchmark(self):
        """How many times the benchmark should be run (an integer, defaults to 0)."""
        return 0

    @mutable_property
    def block_size(self):
        """The block size used by the client."""
        return BLOCK_SIZE

    @mutable_property
    def concurrency(self):
        """The number of parallel processes that the client is allowed to start."""
        return DEFAULT_CONCURRENCY

    @mutable_property
    def delta_transfer(self):
        """Whether delta transfer is enabled (a boolean, defaults to :data:`True`)."""
        return True

    @mutable_property
    def dry_run(self):
        """Whether the client is allowed to make changes."""
        return False

    @mutable_property
    def hash_method(self):
        """The block hash method (a string, defaults to 'sha1')."""
        return "sha1"

    @mutable_property
    def source(self):
        """The :class:`Location` from which data is read."""

    @source.setter
    def source(self, value):
        """Automatically coerce :attr:`source` to a :class:`Location`."""
        set_property(self, "source", Location(expression=value))

    @mutable_property
    def target(self):
        """The :class:`Location` to which data is written."""

    @target.setter
    def target(self, value):
        """Automatically coerce :attr:`target` to a :class:`Location`."""
        set_property(self, "target", Location(expression=value))

    def mutate_target(self, percentage):
        """Invalidate a percentage of the data in the :attr:`target` file."""
        if self.target.hostname:
            raise TypeError("Benchmark requires local target file!")
        num_bytes = self.target.file_size / 100 * percentage
        if self.target.file_size > 1024 * 1024 * 1024:
            block_size = 1024 * 1024
        else:
            block_size = 1024 * 256
        logger.notice("Mutating %i%% of the target file ..", percentage)
        with open(self.target.filename, "r+b") as handle:
            block = "\0" * block_size
            for i in range(num_bytes / block_size):
                handle.write(block)

    def run_benchmark(self):
        """Benchmark the effectiveness of the delta transfer implementation."""
        # Make sure the operator realizes what we're going to do, before it happens.
        if os.environ.get("PDIFFCOPY_BENCHMARK") != "allowed":
            logger.notice("Set $PDIFFCOPY_BENCHMARK=allowed to bypass the following interactive prompt.")
            question = """
                This will mutate the target file and then restore
                its original contents. Are you sure this is okay?
            """
            if not prompt_for_confirmation(compact(question), default=False):
                raise BenchmarkAbortedError("Permission to run benchmark denied.")
        samples = []
        logger.info("Performing initial synchronization to level the playing ground ..")
        self.synchronize_once()
        # If the target file didn't exist before we created it then
        # self.target.exists may have cached the value False.
        self.clear_cached_properties()
        # Get the rsync configuration from environment variables.
        rsync_server = os.environ.get("PDIFFCOPY_BENCHMARK_RSYNC_SERVER")
        rsync_module = os.environ.get("PDIFFCOPY_BENCHMARK_RSYNC_MODULE")
        rsync_root = os.environ.get("PDIFFCOPY_BENCHMARK_RSYNC_ROOT")
        have_rsync = rsync_server and rsync_module and rsync_root
        # Run the benchmark for the requested number of iterations.
        for i in range(1, self.benchmark + 1):
            # Initialize timers to compare pdiffcopy and rsync runtime.
            pdiffcopy_timer = Timer(resumable=True)
            rsync_timer = Timer(resumable=True)
            # Mutate the target file.
            difference = 100 / self.benchmark * i
            self.mutate_target(difference)
            # Synchronize using pdiffcopy.
            with Timer(resumable=True) as pdiffcopy_timer:
                num_blocks = self.synchronize_once()
            # Synchronize using rsync?
            if have_rsync:
                self.mutate_target(difference)
                with rsync_timer:
                    filename = os.path.relpath(self.target.filename, rsync_root)
                    expression = format("%s::%s", rsync_server, os.path.join(rsync_module, filename))
                    logger.info("Synchronizing changes using rsync ..")
                    subprocess.check_call(["rsync", expression, self.target.filename])
                    logger.info("Synchronized changes using rsync in %s ..", rsync_timer)
            # Summarize the results of this iteration.
            metrics = ["%i%%" % difference]
            metrics.append(format_size(num_blocks * self.block_size, binary=True))
            metrics.append(str(pdiffcopy_timer))
            if have_rsync:
                metrics.append(str(rsync_timer))
            samples.append(metrics)
        # Render an overview of the results in the form of a table.
        column_names = ["Delta size", "Data transferred", "Runtime of pdiffcopy"]
        if have_rsync:
            column_names.append("Runtime of rsync")
        output(format_pretty_table(samples, column_names=column_names))

    def synchronize(self):
        """Synchronize from :attr:`source` to :attr:`target` (possibly more than once, see :attr:`benchmark`)."""
        if self.benchmark > 0:
            self.run_benchmark()
        else:
            self.synchronize_once()

    def synchronize_once(self):
        """
        Synchronize from :attr:`source` to :attr:`target`.

        :returns: The number of blocks that differed (an integer).
        """
        timer = Timer()
        if self.delta_transfer and not self.target.exists:
            logger.info("Disabling delta transfer because target file doesn't exist ..")
            self.delta_transfer = False
        if self.delta_transfer:
            logger.info("Computing similarity index for delta transfer ..")
            offsets = self.find_changes()
        else:
            logger.info("Performing whole file copy (skipping delta transfer) ..")
            offsets = range(0, self.source.file_size, self.block_size)
        if offsets:
            self.transfer_changes(offsets)
            logger.info("Synchronized changes in %s ..", timer)
        else:
            logger.info("Nothing to do! (file contents match)")
        return len(offsets)

    def find_changes(self):
        """Helper for :func:`synchronize()` to compute the similarity index."""
        timer = Timer()
        logger.info("Computing hashes using %s ..", pluralize(self.concurrency, "worker"))
        hash_opts = dict(block_size=self.block_size, concurrency=self.concurrency, method=self.hash_method)
        source_promise = Promise(target=get_hashes_fn, args=[self.source], kwargs=hash_opts)
        target_promise = Promise(target=get_hashes_fn, args=[self.target], kwargs=hash_opts)
        source_hashes = source_promise.join()
        target_hashes = target_promise.join()
        num_hits = 0
        num_misses = 0
        todo = []
        for offset in sorted(set(source_hashes) | set(target_hashes)):
            if source_hashes.get(offset) == target_hashes.get(offset):
                num_hits += 1
            else:
                num_misses += 1
                todo.append(offset)
        logger.info("Computed %i%% similarity in %s.", num_hits / ((num_hits + num_misses) / 100.0), timer)
        return todo

    def transfer_changes(self, offsets):
        """
        Helper for :func:`synchronize()` to transfer the differences.

        :param offsets: A list of integers with the byte offsets of the blocks
                        to copy from :attr:`source` to :attr:`target`.
        """
        timer = Timer()
        formatted_size = format_size(self.block_size * len(offsets), binary=True)
        action = "download" if self.source.hostname else "upload"
        logger.info("Will %s %s totaling %s.", action, pluralize(len(offsets), "block"), formatted_size)
        if self.dry_run:
            return
        # Make sure the target file has the right size.
        if not (self.target.exists and self.source.file_size == self.target.file_size):
            self.target.resize(self.source.file_size)
        # Transfer changed blocks in parallel.
        pool = WorkerPool(
            concurrency=self.concurrency,
            generator_fn=functools.partial(iter, offsets),
            worker_fn=functools.partial(
                transfer_block_fn, block_size=self.block_size, source=self.source, target=self.target
            ),
        )
        spinner = Spinner(label="%sing changed blocks" % action.capitalize(), total=len(offsets))
        with pool, spinner:
            for i, result in enumerate(pool, start=1):
                spinner.step(progress=i)
        logger.info(
            "%sed %i blocks (%s) in %s (%s/s).",
            action.capitalize(),
            len(offsets),
            formatted_size,
            timer,
            format_size((self.block_size * len(offsets)) / timer.elapsed_time, binary=True),
        )


def get_hashes_fn(location, **options):
    """Adapter for :mod:`multiprocessing` used by :func:`Client.find_changes()`."""
    return location.get_hashes(**options)


def transfer_block_fn(offset, source, target, block_size):
    """Adapter for :mod:`multiprocessing` used by :func:`Client.transfer_changes()`."""
    target.write_block(offset, source.read_block(offset, block_size))


class Location(PropertyManager):

    """A local or remote file to be copied."""

    @cached_property
    def exists(self):
        """:data:`True` if the file exists, :data:`False` otherwise."""
        logger.info("Checking if %s exists ..", self.label)
        return bool(self.file_info)

    @mutable_property
    def expression(self):
        """The location expression (a string)."""
        if self.hostname:
            netloc = "%s:%s" % (self.hostname, self.port_number)
            return urlunparse(("http", netloc, self.filename, "", "", ""))
        else:
            return self.filename

    @expression.setter
    def expression(self, value):
        """Parse a location expression."""
        parsed_url = urlparse(value)
        if parsed_url.scheme and parsed_url.scheme != "http":
            msg = "Invalid URL scheme! (expected 'http', got %r instead)"
            raise ValueError(msg % parsed_url.scheme)
        if parsed_url.hostname:
            self.filename = parsed_url.path
            self.hostname = parsed_url.hostname
            self.port_number = parsed_url.port or DEFAULT_PORT
        else:
            self.filename = value
            self.hostname = None
            self.port_number = None
        assert self.filename is not None

    @mutable_property
    def filename(self):
        """The absolute pathname of the file to copy (a string)."""

    @mutable_property
    def hostname(self):
        """The host name of a pdiffcopy server (a string or :data:`None`)."""

    @property
    def label(self):
        """A human friendly label for the location (a string)."""
        vicinity = "remote" if self.hostname else "local"
        return "%s file %s" % (vicinity, self.filename)

    @mutable_property
    def port_number(self):
        """The port number of a pdiffcopy server (a number or :data:`None`)."""

    @cached_property
    def file_info(self):
        """A dictionary with file metadata."""
        if self.hostname:
            request_url = self.get_url("info", filename=self.filename)
            logger.debug("Requesting %s ..", request_url)
            response = requests.get(request_url)
            if response.status_code == 404:
                return {}
            else:
                response.raise_for_status()
                return response.json()
        else:
            return get_file_info(self.filename)

    @cached_property
    def file_size(self):
        """The size of the file in bytes (an integer)."""
        logger.info("Getting size of %s ..", self.label)
        return self.file_info.get("size")

    def get_hashes(self, **options):
        """
        Get the hashes of the blocks in a file.

        :param options: See :attr:`get_url()`.
        :returns: A generator of tokens with two values each:

                  1. A byte offset into the file (an integer).
                  2. The hash of the block starting at that offset (a string).
        """
        results = {}
        options.update(filename=self.filename)
        if self.hostname:
            logger.info("Requesting hashes from server ..")
            request_url = self.get_url("hashes", **options)
            logger.debug("Requesting %s ..", request_url)
            response = requests.get(request_url, stream=True)
            response.raise_for_status()
            for line in response.iter_lines(decode_unicode=True):
                offset, _, digest = line.partition("\t")
                results[int(offset)] = digest
        else:
            progress = 0
            block_size = options["block_size"]
            total = os.path.getsize(options["filename"])
            with Spinner(label="Computing hashes", total=total) as spinner:
                for offset, digest in compute_hashes(**options):
                    results[offset] = digest
                    progress += block_size
                    spinner.step(progress)
        return results

    def get_url(self, endpoint, **params):
        """
        Get the server URL for the given `endpoint`.

        :param endpoint: The name of a server side endpoint (a string).
        :param params: Any query string parameters.
        """
        return format(
            "http://{hostname}:{port}/{endpoint}?{params}",
            hostname=self.hostname,
            port=self.port_number,
            endpoint=endpoint,
            params=urlencode(params),
        )

    def read_block(self, offset, size):
        """
        Read a block of data from :attr:`filename`.

        :param offset: The byte offset where reading starts (an integer).
        :param size: The number of bytes to read (an integer).
        :returns: A byte string.
        """
        if self.hostname:
            request_url = self.get_url("blocks", filename=self.filename, offset=offset, size=size)
            logger.debug("Requesting %s ..", request_url)
            response = requests.get(request_url)
            response.raise_for_status()
            return response.content
        else:
            return read_block(self.filename, offset, size)

    def resize(self, size):
        """
        Adjust the size of :attr:`filename` to the given size.

        :param size: The new file size in bytes (an integer).
        """
        if self.hostname:
            request_url = self.get_url("resize", filename=self.filename, size=size)
            logger.debug("Posting to %s ..", request_url)
            requests.post(request_url).raise_for_status()
        else:
            resize_file(self.filename, size)

    def write_block(self, offset, data):
        """
        Write a block of data to :attr:`filename`.

        :param offset: The byte offset where writing starts (an integer).
        :param data: The byte string to write to the file.
        """
        if self.hostname:
            request_url = self.get_url("blocks", filename=self.filename, offset=offset)
            logger.debug("Posting to %s ..", request_url)
            response = requests.post(request_url, data=data)
            response.raise_for_status()
        else:
            write_block(self.filename, offset, data)
