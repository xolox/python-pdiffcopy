# Command line interface for pdiffcopy.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: June 29, 2019
# URL: https://pdiffcopy.readthedocs.io

"""
Usage: pdiffcopy [OPTIONS] [SOURCE, TARGET]

Copy files between systems like rsync, but optimized to copy very large files
(hundreds of gigabytes) by computing hashes in parallel on multiple CPU cores.

One of SOURCE and TARGET arguments is expected to be the pathname of a local
file and the other argument is expected to be an expression of the form
HOST:PORT/PATH. File data will be read from SOURCE and written to TARGET.

If no positional arguments are given the server is started.

  -b, --block-size=BYTES

    Customize the size of the blocks that are hashed. Can be a
    plain number (bytes) or an expression like 5KB, 1MB, etc.

  -l, --listen=ADDRESS

    Listen on the specified IP:PORT or PORT.

  -v, --verbose

    Increase logging verbosity.

  -q, --quiet

    Decrease logging verbosity.

  -h, --help

    Show this message and exit.
"""

# Standard library modules.
import getopt
import logging
import sys

# External dependencies.
import coloredlogs
from humanfriendly import parse_size
from humanfriendly.terminal import warning, usage

# Modules included in our package.
from pdiffcopy import BLOCK_SIZE, DEFAULT_CONCURRENCY
from pdiffcopy.client import Client
from pdiffcopy.server import DEFAULT_PORT, start_server

# Initialize a logger for this module.
logger = logging.getLogger(__name__)


def main():
    """The command line interface."""
    # Initialize logging to the terminal and system log.
    coloredlogs.install(syslog=True)
    # Parse the command line options.
    try:
        options, arguments = getopt.gnu_getopt(
            sys.argv[1:], "b:c:l:vqh", ["block-size=", "concurrency=", "listen=", "verbose", "quiet", "help"]
        )
    except Exception as e:
        warning("Error: %s", e)
        sys.exit(1)
    # Command line option defaults.
    block_size = BLOCK_SIZE
    concurrency = DEFAULT_CONCURRENCY
    listen_address = ("", DEFAULT_PORT)
    # Map parsed options to variables.
    for option, value in options:
        if option in ("-b", "--block-size"):
            block_size = parse_size(value)
        elif option in ("-c", "--concurrency"):
            concurrency = int(value)
        elif option in ("-l", "--listen"):
            if value.count(':') == 1:
                hostname, _, port = value.partition(':')
                listen_address = (hostname, int(port))
            elif value.isdigit():
                listen_address = ('', int(port))
            else:
                listen_address = (value, DEFAULT_PORT)
        elif option in ("-v", "--verbose"):
            coloredlogs.increase_verbosity()
        elif option in ("-q", "--quiet"):
            coloredlogs.decrease_verbosity()
        elif option in ("-h", "--help"):
            usage(__doc__)
            sys.exit(0)
    # Run the client or start the server.
    if arguments:
        if len(arguments) != 2:
            warning("Error: Two positional arguments expected!")
            sys.exit(1)
        Client(block_size=block_size, concurrency=concurrency).synchronize(*arguments)
    else:
        start_server(address=listen_address, concurrency=concurrency)
