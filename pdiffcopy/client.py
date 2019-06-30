# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: June 30, 2019
# URL: https://pdiffcopy.readthedocs.io

"""Parallel, differential file copy client."""

# Standard library modules.
import logging
import multiprocessing
import os

# External dependencies.
import requests
from humanfriendly import Spinner, Timer, format, format_size
from six.moves.urllib.parse import urlencode
from property_manager import PropertyManager, cached_property, mutable_property, set_property

# Modules included in our package.
from pdiffcopy import BLOCK_SIZE, DEFAULT_CONCURRENCY
from pdiffcopy.hashing import hash_generic

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


class Client(PropertyManager):

    @mutable_property
    def block_size(self):
        """The block size used by the client."""
        return BLOCK_SIZE

    @mutable_property
    def concurrency(self):
        """The number of parallel processes that the client is allowed to start."""
        return DEFAULT_CONCURRENCY

    @mutable_property
    def dry_run(self):
        """Whether the client is allowed to make changes."""
        return False

    @mutable_property
    def hash_method(self):
        """The block hash method (defaults to 'sha1')."""
        return "sha1"

    @property
    def hash_options(self):
        return dict(block_size=self.block_size, concurrency=self.concurrency, method=self.hash_method)

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

    def synchronize(self):
        offsets = self.find_changes()
        self.transfer_changes(offsets)

    def find_changes(self):
        timer = Timer()
        source_promise = Promise(get_hashes_pickleable, self.source, **self.hash_options)
        target_promise = Promise(get_hashes_pickleable, self.target, **self.hash_options)
        source_hashes = dict(source_promise.join())
        target_hashes = dict(target_promise.join())
        num_hits = 0
        num_misses = 0
        todo = []
        for offset in sorted(set(source_hashes) | set(target_hashes)):
            if source_hashes.get(offset) == target_hashes.get(offset):
                num_hits += 1
            else:
                num_misses += 1
                todo.append(offset)
        logger.info("Took %s to compute similarity index of %i%%.", timer, num_hits / ((num_hits + num_misses) / 100.0))
        return todo

    def transfer_changes(self, offsets):
        timer = Timer()
        if not offsets:
            logger.info("Nothing to do! (no changes to synchronize)")
            return
        formatted_size = format_size(self.block_size * len(offsets))
        logger.info("Will download %i blocks totaling %s.", len(offsets), formatted_size)
        if self.dry_run:
            return
        # Make sure the target file has the right size.
        if self.target.file_size != self.source.file_size:
            self.target.adjust_size(self.source.file_size)
        # Download changed blocks in parallel.
        num_blocks = len(offsets)
        pool = multiprocessing.Pool(self.concurrency)
        tasks = [(self.source, self.target, offset, self.block_size) for offset in offsets]
        with Spinner(label="Downloading changed blocks", total=num_blocks) as spinner:
            for i, result in enumerate(pool.imap_unordered(transfer_block, tasks), start=1):
                spinner.step(progress=i)
        logger.info("Downloaded %i blocks (%s) in %s.", len(offsets), formatted_size, timer)


def get_hashes_pickleable(location, **options):
    return dict(location.get_hashes(**options))


def transfer_block(args):
    source, target, offset, block_size = args
    data = source.block_read(offset, block_size)
    target.block_write(offset, data)


class Location(PropertyManager):

    """A local or remote file to be copied."""

    @mutable_property
    def expression(self):
        """The location expression (a string)."""
        address = ""
        if self.hostname:
            address = self.hostname
            if self.port_number:
                address += ":%i" % self.port_number
        return address + self.filename

    @expression.setter
    def expression(self, value):
        """Parse a location expression."""
        self.hostname = None
        self.port_number = None
        if value.startswith("/"):
            self.filename = value
        else:
            address, _, filename = value.partition("/")
            if ":" in address:
                hostname, _, port = address.partition(":")
                self.hostname = hostname
                self.port_number = int(port)
            else:
                self.hostname = address
            self.filename = "/" + filename

    @mutable_property
    def filename(self):
        """The absolute pathname of the file to copy (a string)."""

    @mutable_property
    def hostname(self):
        """The host name of a pdiffcopy server (a string or :data:`None`)."""

    @mutable_property
    def port_number(self):
        """The port number of a pdiffcopy server (a number or :data:`None`)."""

    @cached_property
    def file_size(self):
        logger.info("Getting file size of %s ..", self.filename)
        if self.hostname:
            request_url = self.get_url("info", filename=self.filename)
            logger.debug("Requesting %s ..", request_url)
            response = requests.get(request_url)
            response.raise_for_status()
            return int(response.content)
        else:
            return os.path.getsize(self.filename)

    def adjust_size(self, file_size):
        if self.hostname:
            raise Exception("Not implemented!")
        else:
            logger.info("Adjusting size of %s to %s (%s) ..", self.filename, format_size(file_size), file_size)
            with open(self.filename, "r+b") as handle:
                handle.truncate(file_size)

    def block_read(self, offset, block_size):
        logger.debug("Reading %s block %s ..", self.filename, offset)
        if self.hostname:
            request_url = self.get_url("blocks", filename=self.filename, offset=offset, block_size=block_size)
            logger.debug("Requesting %s ..", request_url)
            response = requests.get(request_url)
            response.raise_for_status()
            return response.content
        else:
            with open(self.filename, "rb") as handle:
                handle.seek(offset)
                return handle.read(block_size)

    def block_write(self, offset, data):
        if self.hostname:
            raise Exception("Not implemented!")
        else:
            logger.debug("Writing %s block %s (size: %s) ..", self.filename, offset, len(data))
            with open(self.filename, "r+b") as handle:
                handle.seek(offset)
                handle.write(data)
                handle.flush()

    def get_hashes(self, **options):
        """Get the hashes of the blocks in a file."""
        options.update(filename=self.filename)
        if self.hostname:
            logger.info("Requesting hashes from server at %s:%s ..", self.hostname, self.port_number)
            request_url = self.get_url("hashes", **options)
            logger.debug("Requesting %s ..", request_url)
            response = requests.get(request_url)
            response.raise_for_status()
            for line in response.iter_lines():
                tokens = line.split()
                yield int(tokens[0]), tokens[1]
        else:
            with Spinner(label="Computing local hashes", total=os.path.getsize(options["filename"])) as spinner:
                for offset, digest in hash_generic(**options):
                    yield offset, digest
                    spinner.step(progress=offset)

    def get_url(self, endpoint, **params):
        return format(
            "http://{hostname}:{port}/{endpoint}?{params}",
            hostname=self.hostname,
            port=self.port_number,
            endpoint=endpoint,
            params=urlencode(params),
        )


class Promise(multiprocessing.Process):

    """Function executed in a subprocess (asynchronously)."""

    def __init__(self, target, *args, **kw):
        """Initialize a :class:`Promise` object."""
        super(Promise, self).__init__()
        self.target = target
        self.args = args
        self.kw = kw
        self.queue = multiprocessing.Queue()
        self.start()

    def run(self):
        """Run the target function in a newly spawned subprocess."""
        try:
            status = True
            result = self.target(*self.args, **self.kw)
        except Exception as e:
            logger.exception("Promise raised exception! (will re-raise in parent)")
            status = False
            result = e
        self.queue.put((status, result))

    def join(self):
        """Wait for the background process to finish."""
        status, result = self.queue.get()
        super(Promise, self).join()
        if status:
            return result
        else:
            raise result
