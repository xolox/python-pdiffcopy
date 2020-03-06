pdiffcopy: Fast large file synchronization inspired by rsync
============================================================

.. image:: https://travis-ci.org/xolox/python-pdiffcopy.svg?branch=master
   :target: https://travis-ci.org/xolox/python-pdiffcopy

.. image:: https://coveralls.io/repos/xolox/python-pdiffcopy/badge.svg?branch=master
   :target: https://coveralls.io/r/xolox/python-pdiffcopy?branch=master

The pdiffcopy program synchronizes large binary data files between Linux
servers at blazing speeds by performing delta transfers and spreading its work
over many CPU cores. It's currently tested on Python_ 2.7, 3.5+ and PyPy (2.7)
on Ubuntu Linux but is expected to work on most Linux systems.

.. contents::
   :local:

Status
------

Although the first prototype of pdiffcopy was developed back in June 2019 it
wasn't until March 2020 that the first release was published as an open source
project.

.. note:: This is an `alpha release`_, meaning it's not considered mature and
          you may encounter bugs. As such, if you're going to use pdiffcopy,
          I would suggest you to keep backups, be cautious and sanity check
          your results.

There are lots of features and improvements I'd love to add but more
importantly the project needs to actually be used for a while before
I'll consider changing the alpha label to beta or mature.

Installation
------------

The pdiffcopy package is available on PyPI_ which means installation should be
as simple as:

.. code-block:: console

   $ pip install pdiffcopy

There's actually a multitude of ways to install Python_ packages (e.g. the `per
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

Synchronize large binary data files between Linux servers at blazing speeds
by performing delta transfers and spreading the work over many CPU cores.

One of the SOURCE and TARGET arguments is expected to be the pathname of a
local file and the other argument is expected to be a URL that provides the
location of a remote pdiffcopy server and a remote filename. File data will be
read from SOURCE and written to TARGET.

If no positional arguments are given the server is started.

**Supported options:**

.. csv-table::
   :header: Option, Description
   :widths: 30, 70


   "``-b``, ``--block-size=BYTES``","Customize the block size of the delta transfer. Can be a plain
   integer number (bytes) or an expression like 5K, 1MiB, etc."
   "``-m``, ``--hash-method=NAME``","Customize the hash method of the delta transfer (defaults to 'sha1'
   but supports all hash methods provided by the Python hashlib module)."
   "``-W``, ``--whole-file``","Disable the delta transfer algorithm (skips computing
   of hashing and downloads all blocks unconditionally)."
   "``-c``, ``--concurrency=COUNT``",Change the number of parallel block hash / copy operations.
   "``-n``, ``--dry-run``","Scan for differences between the source and target file and report the
   similarity index, but don't write any changed blocks to the target."
   "``-B``, ``--benchmark=COUNT``","Evaluate the effectiveness of delta transfer by mutating the TARGET
   file (which must be a local file) and resynchronizing its contents.
   This process is repeated ``COUNT`` times, with varying similarity.
   At the end an overview is printed."
   "``-l``, ``--listen=ADDRESS``",Listen on the specified IP:PORT or PORT.
   "``-v``, ``--verbose``",Increase logging verbosity (can be repeated).
   "``-q``, ``--quiet``",Decrease logging verbosity (can be repeated).
   "``-h``, ``--help``",Show this message and exit.

.. [[[end]]]

Benchmarks
----------

The command line interface provides a simple way to evaluate the effectiveness
of the delta transfer implementation and compare it against rsync_. The tables
in the following sections are based on that benchmark.

.. contents::
   :local:

Low concurrency
~~~~~~~~~~~~~~~

:Concurrency: 6 processes on 4 CPU cores
:Disks: `Magnetic storage`_ (slow)
:Filesize: 1.79 GiB

The following table shows the results of the benchmark on a 1.79 GiB
datafile that's synchronized between two bare metal servers that each
have four CPU cores and spinning disks, where pdiffcopy was run with
a concurrency of six [#]_:

=====  =========  =============  =========================
Delta  Data size  pdiffcopy      rsync
=====  =========  =============  =========================
  10%    183 MiB   3.20 seconds              38.55 seconds
  20%    366 MiB   4.15 seconds              44.33 seconds
  30%    549 MiB   5.17 seconds              49.63 seconds
  40%    732 MiB   6.09 seconds              53.74 seconds
  50%    916 MiB   6.99 seconds              57.49 seconds
  60%   1.07 GiB   8.06 seconds  1 minute and 0.97 seconds
  70%   1.25 GiB   9.06 seconds  1 minute and 2.38 seconds
  80%   1.43 GiB  10.12 seconds  1 minute and 4.20 seconds
  90%   1.61 GiB  10.89 seconds  1 minute and 3.80 seconds
 100%   1.79 GiB  12.05 seconds  1 minute and 4.14 seconds
=====  =========  =============  =========================

.. [#] Allocating more processes than there are CPU cores available can make
       sense when the majority of the time spent by those processes is waiting
       for I/O (this definitely applies to pdiffcopy).

High concurrency
~~~~~~~~~~~~~~~~

:Concurrency: 10 processes on 48 CPU cores
:Disks: NVMe_ (fast)
:Filesize: 5.5 GiB

Here's a benchmark based on a 5.5 GB datafile that's synchronized between two
bare metal servers that each have 48 CPU cores and high-end NVMe_ disks, where
pdiffcopy was run with a concurrency of ten:

=====  =========  =============  ==========================
Delta  Data size  pdiffcopy      rsync
=====  =========  =============  ==========================
  10%    562 MiB   4.23 seconds               49.96 seconds
  20%   1.10 GiB   6.76 seconds  1 minute and  2.38 seconds
  30%   1.65 GiB   9.43 seconds  1 minute and 13.73 seconds
  40%   2.20 GiB  12.41 seconds  1 minute and 19.67 seconds
  50%   2.75 GiB  14.54 seconds  1 minute and 25.86 seconds
  60%   3.29 GiB  17.21 seconds  1 minute and 26.97 seconds
  70%   3.84 GiB  19.79 seconds  1 minute and 27.46 seconds
  80%   4.39 GiB  23.10 seconds  1 minute and 26.15 seconds
  90%   4.94 GiB  25.19 seconds  1 minute and 21.96 seconds
 100%   5.43 GiB  27.82 seconds  1 minute and 19.17 seconds
=====  =========  =============  ==========================

This benchmark shows how well pdiffcopy can scale up its performance by running
on a large number of CPU cores. Notice how the smaller the delta is, the bigger
the edge is that pdiffcopy has over rsync_? This is because pdiffcopy computes
the differences between the local and remote file using many CPU cores at the
same time. This operation requires only reading, and that parallelizes
surprisingly well on modern NVMe_ disks.

Silly concurrency
~~~~~~~~~~~~~~~~~

:Concurrency: 20 processes on 48 CPU cores
:Disks: NVMe_ (fast)
:Filesize: 5.5 GiB

In case you looked at the high concurrency benchmark above, noticed the large
number of CPU cores available and wondered whether increasing the concurrency
further would make a difference, this section is for you 😉. Having taken the
effort of developing pdiffcopy and enabling it to run on many CPU cores I was
curious myself so I reran the high concurrency benchmark using 20 processes
instead of 10. Here are the results:

=====  =========  =============  ==========================
Delta  Data size  pdiffcopy      rsync
=====  =========  =============  ==========================
  10%    562 MiB   3.80 seconds               49.71 seconds
  20%   1.10 GiB   6.25 seconds  1 minute and  3.37 seconds
  30%   1.65 GiB   8.90 seconds  1 minute and 12.40 seconds
  40%   2.20 GiB  11.44 seconds  1 minute and 19.57 seconds
  50%   2.75 GiB  14.21 seconds  1 minute and 25.43 seconds
  60%   3.29 GiB  16.45 seconds  1 minute and 28.12 seconds
  70%   3.84 GiB  19.05 seconds  1 minute and 28.34 seconds
  80%   4.39 GiB  21.95 seconds  1 minute and 25.49 seconds
  90%   4.94 GiB  24.60 seconds  1 minute and 22.27 seconds
 100%   5.43 GiB  26.42 seconds  1 minute and 18.73 seconds
=====  =========  =============  ==========================

As you can see increasing the concurrency from 10 to 20 does make the benchmark
a bit faster, however the margin is so small that it's hardly worth bothering.
I interpret this to mean that the NVMe_ disks on these servers can be more or
less saturated using 8--12 writer processes.

.. note:: In the end the question is how many CPU cores it takes to saturate
          your storage infrastructure. This can be determined through
          experimentation, which the benchmark can assist with. There are no
          fundamental reasons why 30 or even 50 processes couldn't work well,
          as long as your storage infrastructure can keep up...

Limitations
-----------

While inspired by rsync_ the goal definitely isn't feature parity with rsync_.
Right now only single files can be transferred and only the file data is
copied, not the metadata. It's a proof of concept that works but is limited.
While I'm tempted to add support for synchronization of directory trees and
file metadata just because its convenient, it's definitely not my intention to
compete with rsync_ in the domain of synchronizing large directory trees,
because I would most likely fail.

Error handling is currently very limited and interrupting the program using
Control-C may get you stuck with an angry pool of multiprocessing_ workers that
refuse to shut down 😝. In all seriousness, hitting Control-C a couple of times
should break out of it, otherwise try Control-\\ (that's a backslash, it should
send a QUIT signal).

History
-------

In June 2019 I found myself in a situation where I wanted to quickly
synchronize large binary datafiles (a small set of very large MySQL_
``*.ibd`` files totaling several hundred gigabytes) using the abundant
computing resources available to me (48 CPU cores, NVMe_ disks,
bonded network interfaces, you name it 😉).

I spent quite a bit of time experimenting with running many rsync_ processes in
parallel, but the small number of very large files was "clogging up the pipe"
so to speak, no matter what I did. This was how I realized that rsync_ was a
really poor fit, which was a disappointment for me because rsync_ has long been
one my go-to programs for ad hoc problem solving on Linux servers 🙂.

In any case I decided to prove to myself that the hardware available to me
could do much more than what rsync_ was getting me and after a weekend of
hacking on a prototype I had something that could outperform rsync_ even though
it was written in Python_ and used HTTP_ as a transport 😁. During this weekend
I decided that my prototype was worthy of being published as an open source
project, however it wasn't until months later that I actually found the time to
do so.

About the name
--------------

The name pdiffcopy is intended as a (possibly somewhat obscure) abbreviation of
"Parallel Differential Copy":

- Parallel because it's intended run on many CPU cores.
- Differential because of the delta transfer mechanism.

But mostly I just needed a short, unique name like rsync_ so that searching for
this project will actually turn up this project instead of a dozen others 😇.

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
.. _alpha release: https://en.wikipedia.org/wiki/Software_release_life_cycle#Alpha
.. _changelog: https://pdiffcopy.readthedocs.io/en/latest/changelog.html
.. _GitHub: https://github.com/xolox/python-pdiffcopy
.. _HTTP: https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol
.. _Magnetic storage: https://en.wikipedia.org/wiki/Hard_disk_drive
.. _MIT license: http://en.wikipedia.org/wiki/MIT_License
.. _multiprocessing: https://docs.python.org/library/multiprocessing.html
.. _MySQL: https://en.wikipedia.org/wiki/MySQL
.. _NVMe: https://en.wikipedia.org/wiki/NVM_Express
.. _per user site-packages directory: https://www.python.org/dev/peps/pep-0370/
.. _peter@peterodding.com: peter@peterodding.com
.. _PyPI: https://pypi.org/project/pdiffcopy
.. _Python: https://en.wikipedia.org/wiki/Python_(programming_language)
.. _Read the Docs: https://pdiffcopy.readthedocs.io/
.. _rsync: https://en.wikipedia.org/wiki/Rsync
.. _virtual environments: http://docs.python-guide.org/en/latest/dev/virtualenvs/
