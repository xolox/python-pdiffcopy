# Fast synchronization of large files inspired by rsync.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: June 29, 2019
# URL: https://pdiffcopy.readthedocs.io

"""Parallel, differential file copy server."""

# Standard library modules.
import logging

# External dependencies.
from flask import Flask, make_response, request, Response
from gunicorn.app.base import BaseApplication
from six import iteritems

# Modules included in our package.
from pdiffcopy import BLOCK_SIZE, DEFAULT_CONCURRENCY
from pdiffcopy.hashing import hash_generic

DEFAULT_PORT = 8000
"""The default port number for the HTTP server."""

# Initialize a logger for this module.
logger = logging.getLogger(__name__)

# Initialize a Flask application.
app = Flask(__name__)


def start_server(address=("", DEFAULT_PORT), concurrency=4):
    """Start a multi threaded HTTP server."""
    StandaloneApplication(app, {"bind": "%s:%s" % address, "workers": concurrency}).run()


@app.route("/blocks")
def get_block():
    filename = request.args.get("filename")
    offset = int(request.args.get("offset"))
    block_size = int(request.args.get("block_size", BLOCK_SIZE))
    with open(filename) as handle:
        handle.seek(offset)
        data = handle.read(block_size)
        return make_response(status=200, response=data, mimetype="application/octet-stream")


@app.route("/hashes")
def get_hashes():
    return Response(
        generate_hashes(
            filename=request.args.get("filename"),
            block_size=int(request.args.get("block_size", BLOCK_SIZE)),
            concurrency=int(request.args.get("concurrency", DEFAULT_CONCURRENCY)),
        ),
        mimetype="text/plain",
    )


def generate_hashes(**options):
    for offset, digest in hash_generic(**options):
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
