# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: February 29, 2020
# URL: https://pdiffcopy.readthedocs.io


"""Test suite for the ``pdiffcopy`` program."""

# Standard library modules.
import contextlib
import filecmp
import logging
import os
import shutil
import sys
import tempfile

# External dependencies.
from executor import execute
from executor.tcp import EphemeralTCPServer
from humanfriendly import format
from humanfriendly.testing import TestCase, run_cli

# Modules included in our package.
from pdiffcopy.cli import main

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


class pdiffcopy_tests(TestCase):

    """:mod:`unittest` compatible container for `pdiffcopy` tests."""

    def test_client_to_server(self):
        """Test synchronization from client to server."""
        with TemporaryServer() as server, temporary_directory() as directory:
            input_file = os.path.join(directory, "input.bin")
            output_file = os.path.join(directory, "output.bin")
            generate_data_file(input_file, 10)
            remote_file = format("localhost:%i/%s", server.port_number, input_file.lstrip("/"))
            returncode, output = run_cli(main, remote_file, output_file)
            # Check that the program reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(input_file, output_file, shallow=False)

    def test_main_module(self):
        """Test the ``python -m pdiffcopy`` command."""
        output = execute(sys.executable, "-m", "pdiffcopy", "--help", capture=True)
        assert "Usage:" in output

    def test_server_to_client(self):
        """Test synchronization from server to client."""
        with TemporaryServer() as server, temporary_directory() as directory:
            input_file = os.path.join(directory, "input.bin")
            output_file = os.path.join(directory, "output.bin")
            generate_data_file(input_file, 10)
            remote_file = format("localhost:%i/%s", server.port_number, output_file.lstrip("/"))
            returncode, output = run_cli(main, input_file, remote_file)
            # Check that the program reported success.
            assert returncode == 0
            # Check that the input and output file have the same content.
            assert filecmp.cmp(input_file, output_file, shallow=False)

    def test_usage_message(self):
        """Test the ``pdifcopy --help`` command."""
        for option in "-h", "--help":
            returncode, output = run_cli(main, option)
            assert returncode == 0
            assert "Usage:" in output


class TemporaryServer(EphemeralTCPServer):

    """Easy to use ``pdiffcopy --listen`` wrapper."""

    def __init__(self):
        """The command to run (a list of strings)."""
        command = [sys.executable, "-m", "pdiffcopy", "--listen", str(self.port_number)]
        super(TemporaryServer, self).__init__(*command)


@contextlib.contextmanager
def temporary_directory():
    """Context manager to create a temporary directory."""
    directory = tempfile.mkdtemp()
    yield directory
    shutil.rmtree(directory)


def generate_data_file(filename, megabytes):
    """Generate a data file with random contents."""
    logger.info("Generating datafile of %s MB at %s ..", megabytes, filename)
    execute("dd", "if=/dev/urandom", "of=%s" % filename, "bs=1M", "count=%s" % megabytes)
