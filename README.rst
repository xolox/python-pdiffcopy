pdiffcopy: Fast synchronization of large files inspired by rsync
================================================================

.. image:: https://travis-ci.org/xolox/python-pdiffcopy.svg?branch=master
   :target: https://travis-ci.org/xolox/python-pdiffcopy

.. image:: https://coveralls.io/repos/xolox/python-pdiffcopy/badge.svg?branch=master
   :target: https://coveralls.io/r/xolox/python-pdiffcopy?branch=master

The ``pdiffcopy`` program can synchronize large files between Linux servers at
blazing speeds by transferring only changed blocks using massive concurrency.
It's currently tested on Python 2.7, 3.5, 3.6, 3.7, 3.8 and PyPy (2.7) on
Ubuntu Linux but is expected to work on most Linux systems.

.. contents::
   :local:

Installation
------------

The pdiffcopy package is available on PyPI_ which means installation should be
as simple as:

.. code-block:: console

   $ pip install pdiffcopy

There's actually a multitude of ways to install Python packages (e.g. the `per
user site-packages directory`_, `virtual environments`_ or just installing
system wide) and I have no intention of getting into that discussion here, so
if this intimidates you then read up on your options before returning to these
instructions 😉.

Command line
------------

.. A DRY solution to avoid duplication of the `pdiffcopy --help' text:
..
.. [[[cog
.. from humanfriendly.usage import inject_usage
.. inject_usage('pdiffcopy.cli')
.. ]]]

**Usage:** `pdiffcopy [OPTIONS] [SOURCE, TARGET]`

Copy files between systems like rsync, but optimized to copy very large files
(hundreds of gigabytes) by computing hashes in parallel on multiple CPU cores.

One of SOURCE and TARGET arguments is expected to be the pathname of a local
file and the other argument is expected to be an expression of the form
HOST:PORT/PATH. File data will be read from SOURCE and written to TARGET.

If no positional arguments are given the server is started.

**Supported options:**

.. csv-table::
   :header: Option, Description
   :widths: 30, 70


   "``-b``, ``--block-size=BYTES``","Customize the block size of the delta transfer. Can be a
   plain number (bytes) or an expression like 5KB, 1MB, etc."
   "``-m``, ``--hash-method=NAME``",Customize the hash method of the delta transfer (defaults to 'sha1').
   "``-W``, ``--whole-file``","Disable the delta transfer algorithm (skips computing
   of hashing and downloads all blocks unconditionally)."
   "``-c``, ``--concurrency=COUNT``",Change the number of parallel block hash / copy operations.
   "``-n``, ``--dry-run``","Scan for differences between the local and remote file and report the
   similarity index, but don't write any changed blocks to the target."
   "``-l``, ``--listen=ADDRESS``",Listen on the specified IP:PORT or PORT.
   "``-v``, ``--verbose``",Increase logging verbosity.
   "``-q``, ``--quiet``",Decrease logging verbosity.
   "``-h``, ``--help``",Show this message and exit.

.. [[[end]]]

Contact
-------

The latest version of pdiffcopy is available on PyPI_ and GitHub_. The
documentation is hosted on `Read the Docs`_ and includes a changelog_. For bug
reports please create an issue on GitHub_. If you have questions, suggestions,
etc. feel free to send me an e-mail at `peter@peterodding.com`_.

License
-------

This software is licensed under the `MIT license`_.

© 2020 Peter Odding.

.. External references:
.. _changelog: https://pdiffcopy.readthedocs.io/en/latest/changelog.html
.. _GitHub: https://github.com/xolox/python-pdiffcopy
.. _MIT license: http://en.wikipedia.org/wiki/MIT_License
.. _per user site-packages directory: https://www.python.org/dev/peps/pep-0370/
.. _peter@peterodding.com: peter@peterodding.com
.. _PyPI: https://pypi.org/project/pdiffcopy
.. _Read the Docs: https://pdiffcopy.readthedocs.io/
.. _virtual environments: http://docs.python-guide.org/en/latest/dev/virtualenvs/
