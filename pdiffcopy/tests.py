# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 2, 2020
# URL: https://pdiffcopy.readthedocs.io


"""Test suite for the ``pdiffcopy`` program."""

# Standard library modules.
import filecmp
import logging
import os
import random
import sys

# External dependencies.
from executor import execute
from executor.tcp import EphemeralTCPServer
from humanfriendly.text import format
from humanfriendly.testing import TemporaryDirectory, TestCase, run_cli
from property_manager import PropertyManager, lazy_property, required_property

# Modules included in our package.
from pdiffcopy.cli import main
from pdiffcopy.client import Location

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


class TestSuite(TestCase):

    """:mod:`unittest` compatible container for `pdiffcopy` tests."""

    def test_location_parsing(self):
        """Test parsing of location expressions."""
        obj = Location(expression='/foo/bar')
        assert obj.filename == '/foo/bar'
        assert not obj.hostname
        assert not obj.port_number

    def test_client_to_server_delta_transfer(self):
        """Test copying a file from the client to the server (using delta transfer)."""
        with Context() as context:
            # Create the target file.
            context.target.generate()
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.pathname, context.target.location)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname, shallow=False)

    def test_client_to_server_full_transfer(self):
        """Test copying a file from the client to the server (no delta transfer)."""
        with Context() as context:
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.pathname, context.target.location)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname, shallow=False)

    def test_main_module(self):
        """Test the ``python -m pdiffcopy`` command."""
        output = execute(sys.executable, "-m", "pdiffcopy", "--help", capture=True)
        assert "Usage:" in output

    def test_server_to_client_delta_transfer(self):
        """Test copying a file from the server to the client (using delta transfer)."""
        with Context() as context:
            # Create the target file.
            context.target.generate()
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.location, context.target.pathname)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname, shallow=False)

    def test_server_to_client_full_transfer(self):
        """Test copying a file from the server to the client (no delta transfer)."""
        with Context() as context:
            # Synchronize the file using the command line interface.
            returncode, output = run_cli(main, context.source.location, context.target.pathname)
            # Check that the command line interface reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(context.source.pathname, context.target.pathname, shallow=False)

    def test_usage_message(self):
        """Test the ``pdifcopy --help`` command."""
        for option in "-h", "--help":
            returncode, output = run_cli(main, option)
            assert returncode == 0
            assert "Usage:" in output


class Context(PropertyManager):

    """Test context"""

    @lazy_property
    def directory(self):
        """A temporary working directory."""
        return TemporaryDirectory()

    @lazy_property
    def server(self):
        """A temporary ``pdiffcopy`` server."""
        return TemporaryServer()

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

    @required_property
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


class TemporaryServer(EphemeralTCPServer):

    """Easy to use ``pdiffcopy --listen`` wrapper."""

    def __init__(self):
        """The command to run (a list of strings)."""
        command = [sys.executable, "-m", "pdiffcopy", "--listen", str(self.port_number)]
        super(TemporaryServer, self).__init__(*command)
