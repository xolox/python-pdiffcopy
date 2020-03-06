# Fast large file synchronization inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: February 29, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Enable the use of ``python -m pdiffcopy ...`` to invoke the command line interface."""

# Modules included in our package.
from pdiffcopy.cli import main

if __name__ == "__main__":
    main()
