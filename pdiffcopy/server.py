# Fast large file synchronization inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 6, 2020
# URL: https://pdiffcopy.readthedocs.io

"""Parallel, differential file copy server."""

# Standard library modules.
import logging

# External dependencies.
from flask import Flask, Response, jsonify, request
from gunicorn.app.base import BaseApplication
from six import iteritems
from six.moves.urllib.parse import urlparse

# Modules included in our package.
from pdiffcopy import BLOCK_SIZE, DEFAULT_CONCURRENCY, DEFAULT_PORT
from pdiffcopy.hashing import compute_hashes
from pdiffcopy.operations import get_file_info, read_block, resize_file, write_block

# Public identifiers that require documentation.
__all__ = (
    "app",
    "blocks_resource",
    "generate_hashes",
    "hashes_resource",
    "info_resource",
    "logger",
    "resize_action",
    "start_server",
)

# Initialize a logger for this module.
logger = logging.getLogger(__name__)

# Initialize a Flask application.
app = Flask(__name__)


def start_server(address=None, concurrency=4):
    """Start a multi threaded ``pdiffcopy`` HTTP server using :pypi:`gunicorn` and :pypi:`flask`."""
    if address:
        if address.isdigit():
            # Only a port number was given.
            listen_host = ""
            listen_port = int(address)
        else:
            # An IP address was given (with an optional port number). We
            # (ab)use the URL parsing module in the standard library to
            # enable proper support for IPv6 which enables monstrosities
            # like http://[2001:db8:1f70::999:de8:7648:6e8]:100/ ...
            parsed = urlparse("http://" + address)
            listen_host = parsed.hostname or ""
            listen_port = parsed.port or DEFAULT_PORT
    else:
        # No listen address specified, default to listening
        # on all available IP addresses and the default port.
        listen_host = ""
        listen_port = DEFAULT_PORT
    StandaloneApplication(
        app, {"bind": "%s:%s" % (listen_host, listen_port), "timeout": 0, "workers": concurrency}
    ).run()


@app.route("/blocks", methods=["GET", "POST"])
def blocks_resource():
    """Flask view to read or write a block of data."""
    filename = request.args["filename"]
    offset = int(request.args["offset"])
    if request.method == "GET":
        data = read_block(filename, offset, int(request.args["size"]))
        return Response(status=200, response=data, mimetype="application/octet-stream")
    elif request.method == "POST":
        write_block(filename, offset, request.data)
        return Response(status=200)
    else:
        return Response(status=405)


@app.route("/hashes")
def hashes_resource():
    """Flask view to get the hashes of a file."""
    return Response(
        mimetype="text/plain",
        response=generate_hashes(
            block_size=int(request.args.get("block_size", BLOCK_SIZE)),
            concurrency=int(request.args.get("concurrency", DEFAULT_CONCURRENCY)),
            filename=request.args.get("filename"),
            method=request.args.get("method"),
        ),
        status=200,
    )


@app.route("/info")
def info_resource():
    """Flask view to query the existence and size of a file on the server."""
    fn = request.args.get("filename")
    info = get_file_info(fn)
    if info:
        return jsonify(info)
    else:
        return Response(status=404)


@app.route("/resize", methods=["POST"])
def resize_action():
    """Flask view to create or resize_action a file on the server."""
    fn = request.args.get("filename")
    size = int(request.args.get("size"))
    resize_file(fn, size)
    return Response(status=200)


def generate_hashes(**options):
    """
    Helper for :func:`hashes_resource()`.

    :param options: See :func:`~pdiffcopy.hashing.compute_hashes()`.
    :returns: A generator of strings, one line each, with two fields per
              line (offset and digest) delimited by a tab character.
    """
    for offset, digest in compute_hashes(**options):
        yield "%i\t%s\n" % (offset, digest)


class StandaloneApplication(BaseApplication):

    """Integration between Flask and Gunicorn."""

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict(
            [(key, value) for key, value in iteritems(self.options) if key in self.cfg.settings and value is not None]
        )
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application
