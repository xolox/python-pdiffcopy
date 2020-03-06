# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 6, 2020
# URL: https://pdiffcopy.readthedocs.io


"""Test suite for the ``pdiffcopy`` program."""

# Standard library modules.
import filecmp
import functools
import logging
import os
import random
import sys
import tempfile

# External dependencies.
from executor import execute
from executor.tcp import EphemeralTCPServer
from humanfriendly.text import format
from humanfriendly.testing import TemporaryDirectory, TestCase, run_cli
from property_manager import PropertyManager, lazy_property, required_property

# Modules included in our package.
from pdiffcopy.cli import main
from pdiffcopy.client import Location
from pdiffcopy.hashing import compute_hashes
from pdiffcopy.mp import WorkerPool

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


class TestSuite(TestCase):

    """:mod:`unittest` compatible container for `pdiffcopy` tests."""

    def test_benchmark(self):
        with Context() as context:
            # Create the target file.
            context.target.generate()
            # Generate a temporary rsync daemon configuration.
            rsyncd_config_file = os.path.join(context.directory.temporary_directory, "rsyncd.conf")
            rsyncd_module_name = "pdiffcopy_test"
            with open(rsyncd_config_file, "w") as handle:
                handle.write("[%s]\n" % rsyncd_module_name)
                handle.write("path = %s\n" % context.directory.temporary_directory)
                handle.write("use chroot = false\n")
            # Start the rsync daemon based on the generated configuration file.
            with RsyncDaemon(rsyncd_config_file) as rsyncd:
                # Run the benchmark without user interaction.
                os.environ["PDIFFCOPY_BENCHMARK"] = "allowed"
                # Instruct the benchmark to test rsync as well.
                os.environ["PDIFFCOPY_BENCHMARK_RSYNC_SERVER"] = "localhost:%i" % rsyncd.port_number
                os.environ["PDIFFCOPY_BENCHMARK_RSYNC_MODULE"] = rsyncd_module_name
                os.environ["PDIFFCOPY_BENCHMARK_RSYNC_ROOT"] = context.directory.temporary_directory
                # Run the benchmark using the command line interface.
                returncode, output = run_cli(
                    main, "--benchmark=5", context.source.location, context.target.pathname, capture=False
                )
                # Check that the command line interface reported success.
                assert returncode == 0
                # Check that the input and output file have the same content.
                assert filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_client_to_server_delta_transfer(self):
        """Test copying a file from the client to the server (with delta transfer)."""
        with Context() as context:
            # Create the target file.
            context.target.generate()
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.pathname, context.target.location, capture=False)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_client_to_server_dry_run(self):
        """Test copying a file from the client to the server (dry run)."""
        with Context() as context:
            # Create the target file.
            context.target.generate()
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, "-n", context.source.pathname, context.target.location, capture=False)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file differ still.
            assert not filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_client_to_server_full_transfer(self):
        """Test copying a file from the client to the server (no delta transfer)."""
        with Context() as context:
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.pathname, context.target.location, capture=False)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_client_to_server_no_transfer(self):
        """Test copying a file from the client to the server (nothing to transfer)."""
        with Context() as context:
            context.target.copy(context.source)
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.pathname, context.target.location, capture=False)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_compute_hashes(self):
        """Test that serial and parallel hashing produce the same result."""
        with tempfile.NamedTemporaryFile() as temporary_file:
            execute("dd", "if=/dev/urandom", "of=%s" % temporary_file.name, "bs=1M", "count=10")
            serial_hashes = dict(
                compute_hashes(filename=temporary_file.name, block_size=1024 * 1024, concurrency=1, method="sha1")
            )
            parallel_hashes = dict(
                compute_hashes(filename=temporary_file.name, block_size=1024 * 1024, concurrency=4, method="sha1")
            )
            assert serial_hashes == parallel_hashes

    def test_location_parsing(self):
        """Test parsing of location expressions."""
        # Check that locations default to local files.
        obj = Location(expression="/foo/bar")
        assert obj.expression == "/foo/bar"
        assert obj.filename == "/foo/bar"
        assert not obj.hostname
        assert not obj.port_number
        # Check that locations support remote files.
        obj = Location(expression="http://server:12345/foo/bar")
        assert obj.expression == "http://server:12345/foo/bar"
        assert obj.filename == "/foo/bar"
        assert obj.hostname == "server"
        assert obj.port_number == 12345
        # Check that unsupported URL schemes raise an exception.
        with self.assertRaises(ValueError):
            Location(expression="udp://server/filename")

    def test_main_module(self):
        """Test the ``python -m pdiffcopy`` command."""
        output = execute(sys.executable, "-m", "pdiffcopy", "--help", capture=True)
        assert "Usage:" in output

    def test_server_to_client_delta_transfer(self):
        """Test copying a file from the server to the client (with delta transfer)."""
        with Context() as context:
            # Create the target file.
            context.target.generate()
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.location, context.target.pathname, capture=False)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_server_to_client_dry_run(self):
        """Test copying a file from the server to the client (dry run)."""
        with Context() as context:
            # Create the target file.
            context.target.generate()
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, "-n", context.source.location, context.target.pathname, capture=False)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert not filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_server_to_client_full_transfer(self):
        """Test copying a file from the server to the client (no delta transfer)."""
        with Context() as context:
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.location, context.target.pathname, capture=False)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_server_to_client_no_transfer(self):
        """Test copying a file from the server to the client (nothing to transfer)."""
        with Context() as context:
            context.target.copy(context.source)
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.location, context.target.pathname, capture=False)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname)

    def test_usage_message(self):
        """Test the ``pdifcopy --help`` command."""
        for option in "-h", "--help":
            returncode, output = run_cli(main, option, capture=True)
            assert returncode == 0
            assert "Usage:" in output

    def test_mp(self):
        """Test the multiprocessing abstractions."""
        options = dict(concurrency=3, generator_fn=functools.partial(range, 10), worker_fn=mp_worker)
        with WorkerPool(**options) as pool:
            expected = sorted(map(mp_worker, range(10)))
            results = sorted([n for n in pool])
            assert results == expected


def mp_worker(n):
    """Simple worker function to test :class:`.WorkerPool`."""
    return n * 2


class Context(PropertyManager):

    """Test context"""

    @lazy_property
    def directory(self):
        """A temporary working directory."""
        return TemporaryDirectory()

    @lazy_property
    def server(self):
        """A temporary ``pdiffcopy`` server."""
        return ProgramServer()

    @lazy_property
    def source(self):
        """The datafile to use as a source."""
        datafile = DataFile(context=self, filename="source.bin")
        datafile.generate()
        return datafile

    @lazy_property
    def target(self):
        """The datafile to use as a target."""
        return DataFile(context=self, filename="target.bin")

    def __enter__(self):
        """Prepare the test context."""
        self.directory.__enter__()
        self.server.__enter__()
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        """Tear down the test context."""
        self.server.__exit__(exc_type, exc_value, traceback)
        self.directory.__exit__(exc_type, exc_value, traceback)


class DataFile(PropertyManager):

    """A data file to be synchronized by the test suite."""

    @required_property(repr=False)
    def context(self):
        """The :class:`Context` object."""

    @required_property
    def filename(self):
        """The filename of the datafile."""

    @lazy_property
    def location(self):
        """The filename accessed through the ``pdiffcopy`` server."""
        return format("http://localhost:%i/%s", self.context.server.port_number, self.pathname.lstrip("/"))

    @lazy_property
    def pathname(self):
        """The absolute pathname of the datafile."""
        return os.path.join(self.context.directory.temporary_directory, self.filename)

    def generate(self):
        """Generate a data file with random contents."""
        num_megabytes = random.randint(10, 25)
        logger.info("Generating datafile of %s MB at %s ..", num_megabytes, self.pathname)
        execute("dd", "if=/dev/urandom", "of=%s" % self.pathname, "bs=1M", "count=%s" % num_megabytes)

    def copy(self, other):
        """Copy another data file."""
        execute("cp", other.pathname, self.pathname)


class ProgramServer(EphemeralTCPServer):

    """Easy to use ``pdiffcopy --listen`` wrapper."""

    def __init__(self):
        """Initialize a :class:`ProgramServer` object."""
        super(ProgramServer, self).__init__(sys.executable, "-m", "pdiffcopy", "--listen", str(self.port_number))


class RsyncDaemon(EphemeralTCPServer):

    """Ephemeral rsync daemon server for testing purposes."""

    def __init__(self, config_file):
        """Initialize an :class:`RsyncDaemon` object."""
        super(RsyncDaemon, self).__init__(
            "rsync", "--no-detach", "--daemon", "--config", config_file, "--port=%s" % self.port_number
        )
