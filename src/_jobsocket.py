import os
import socket
import logging

from pathlib import Path

from .context import context
from .utils import Queue, _start_new_thread
from .parser import server_parser, client_parser

HOSTNAME = socket.getfqdn(socket.gethostname())
IPADDRESS = socket.gethostbyname(HOSTNAME)


class JobSocket(object):

    def __init__(self, socket_file=None, queue=None, host=None, port=None):
        self._host = host
        self._port = port or 0
        if socket_file:
            self._file = Path(socket_file).absolute()
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self._file = socket_file
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.queue = queue

    def listen(self, chunk=1024):
        if self._file:
            if self._file.is_socket():
                self._file.unlink()
            self.socket.bind(str(self._file))
            self.logger.debug("start runjob server ...")
        else:
            self.socket.bind((self._host or "", self._port))
            self._host, self._port = self.socket.getsockname()
            self.logger.debug("start runjob server %s:%s",
                              self._host, self._port)
        self.socket.listen(5)
        while True:
            conn, _ = self.socket.accept()
            data = conn.recv(chunk)
            if data and "-" in data.decode():
                name, status = data.decode().split("-")
                self.logger.debug(
                    "Recived data <- name: %s, status: %s", name, status)
                if self.queue:
                    self.queue.put((name, status))
            conn.close()

    def send(self, data):
        if self._file and self._file.is_socket():
            addr = str(self._file)
        else:
            addr = (self._host, self._port)
        try:
            self.socket.connect(addr)
        except (ConnectionRefusedError, TypeError) as e:
            self.logger.error("connect %s refused", addr)
            return
        else:
            self.logger.debug("connect %s success", addr)
        self.socket.send(data.encode())
        self.logger.debug("Send data -> '%s' success", data)

    def close(self):
        self.socket.close()
        try:
            if self._file.is_socket():
                self._file.unlink()
        except:
            pass

    def __del__(self):
        try:
            self.close()
        except:
            pass

    @property
    def logger(self):
        return context.log

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type:
            raise exc_type(exc_val)


def listen_job_status(sfile=None, queue=None, host=None, port=None):
    with JobSocket(socket_file=sfile, queue=queue, host=host, port=port) as js:
        js.listen()


def send_job_status(sfile, name, status, **kwargs):
    js = JobSocket(socket_file=sfile, **kwargs)
    data = "{}-{}".format(name, status)
    js.send(data)
    js.socket.close()


def job_server():
    args = server_parser().parse_args()
    context.init_log(level="debug")
    if not args.port and args.host and ":" in args.host:
        h = args.host.rsplit(":", 1)
        if len(h) == 2:
            args.host, args.port = h
        elif len(h) == 1:
            args.host = h[0]
    args.port = args.port or 0
    listen_job_status(sfile=args.file, host=args.host, port=int(args.port))


def job_client():
    args = client_parser().parse_args()
    context.init_log(level="debug")
    if not args.file and not (args.host or args.port):
        raise RuntimeError("No server host/port define")
    elif args.file:
        return send_job_status(args.file, args.name, args.status)
    elif not args.host:
        args.host = ""
    elif not args.port:
        try:
            args.host, args.port = args.host.rsplit(":", 1)
        except ValueError:
            raise RuntimeError("No server port define")
    send_job_status(args.file, args.name, args.status,
                    host=args.host, port=int(args.port))
