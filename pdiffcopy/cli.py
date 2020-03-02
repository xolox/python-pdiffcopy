# Command line interface for pdiffcopy.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 2, 2020
# URL: https://pdiffcopy.readthedocs.io

"""
Usage: pdiffcopy [OPTIONS] [SOURCE, TARGET]

Copy individual data files between systems, optimized to copy very large files
(hundreds of gigabytes) by computing fingerprints on multiple CPU cores and
transferring only the changed blocks (again on multiple CPU cores).

One of the SOURCE and TARGET arguments is expected to be the pathname of a
local file and the other argument is expected to be a URL that provides the
location of a remote pdiffcopy server and a remote filename. File data will be
read from SOURCE and written to TARGET.

If no positional arguments are given the server is started.

Supported options:

  -b, --block-size=BYTES

    Customize the block size of the delta transfer. Can be a plain
    integer number (bytes) or an expression like 5K, 1MiB, etc.

  -m, --hash-method=NAME

    Customize the hash method of the delta transfer (defaults to 'sha1'
    but supports all hash methods provided by the Python hashlib module).

  -W, --whole-file

    Disable the delta transfer algorithm (skips computing
    of hashing and downloads all blocks unconditionally).

  -c, --concurrency=COUNT

    Change the number of parallel block hash / copy operations.

  -n, --dry-run

    Scan for differences between the source and target file and report the
    similarity index, but don't write any changed blocks to the target.

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
            sys.argv[1:],
            "b:m:Wc:l:nvqh",
            [
                "block-size=",
                "hash-method=",
                "whole-file",
                "concurrency=",
                "listen=",
                "dry-run",
                "verbose",
                "quiet",
                "help",
            ],
        )
    except Exception as e:
        warning("Error: %s", e)
        sys.exit(1)
    # Command line option defaults.
    block_size = BLOCK_SIZE
    concurrency = DEFAULT_CONCURRENCY
    delta_transfer = True
    dry_run = False
    hash_method = "sha1"
    listen_address = ("", DEFAULT_PORT)
    # Map parsed options to variables.
    for option, value in options:
        if option in ("-b", "--block-size"):
            block_size = parse_size(value)
        elif option in ("-m", "--hash-method"):
            hash_method = value
        elif option in ("-W", "--whole-file"):
            delta_transfer = False
        elif option in ("-c", "--concurrency"):
            concurrency = int(value)
        elif option in ("-l", "--listen"):
            if value.count(":") == 1:
                hostname, _, port = value.partition(":")
                listen_address = (hostname, int(port))
            elif value.isdigit():
                listen_address = ("", int(value))
            else:
                listen_address = (value, DEFAULT_PORT)
        elif option in ("-n", "--dry-run"):
            dry_run = True
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
        Client(
            block_size=block_size,
            concurrency=concurrency,
            delta_transfer=delta_transfer,
            dry_run=dry_run,
            hash_method=hash_method,
            source=arguments[0],
            target=arguments[1],
        ).synchronize()
    else:
        start_server(address=listen_address, concurrency=concurrency)
