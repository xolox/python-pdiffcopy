# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: June 29, 2019
# URL: https://pdiffcopy.readthedocs.io

"""Parallel, differential file copy client."""

# Standard library modules.
import logging
import multiprocessing

# External dependencies.
import requests
from humanfriendly import Timer, format, format_size
from six.moves.urllib.parse import urlencode
from property_manager import PropertyManager, mutable_property

# Modules included in our package.
from pdiffcopy import BLOCK_SIZE, DEFAULT_CONCURRENCY
from pdiffcopy.hashing import hash_generic

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


class Client(PropertyManager):

    @mutable_property
    def block_size(self):
        return BLOCK_SIZE

    @mutable_property
    def concurrency(self):
        return DEFAULT_CONCURRENCY

    @property
    def options(self):
        return dict(block_size=self.block_size, concurrency=self.concurrency)

    def synchronize(self, source, target):
        timer = Timer()
        source = Location(expression=source)
        target = Location(expression=target)
        source_promise = Promise(get_hashes_pickleable, source, **self.options)
        target_promise = Promise(get_hashes_pickleable, target, **self.options)
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
        if todo:
            logger.info("Will download %i blocks totaling %s.", len(todo), format_size(self.block_size * len(todo)))
        # TODO Transfer differences.


def get_hashes_pickleable(location, **options):
    return dict(location.get_hashes(**options))


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

    def get_hashes(self, **options):
        """Get the hashes of the blocks in a file."""
        options.update(filename=self.filename)
        if self.hostname:
            request_url = self.get_url("hashes", **options)
            logger.info("Requesting %s ..", request_url)
            response = requests.get(request_url)
            response.raise_for_status()
            for line in response.iter_lines():
                tokens = line.split()
                yield int(tokens[0]), tokens[1]
        else:
            for offset, digest in hash_generic(**options):
                yield offset, digest

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
