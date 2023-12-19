import os
import socket
import logging

from pathlib import Path


class JobSocket(object):

    def __init__(self, socket_file=None, queue=None):
        self._file = Path(socket_file).absolute()
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.queue = queue

    def listen(self, chunk=1024):
        if self._file.is_socket():
            self._file.unlink()
        self.socket.bind(str(self._file))
        self.socket.listen(1)
        while True:
            conn, _ = self.socket.accept()
            data = conn.recv(chunk)
            if data and "-" in data.decode():
                name, status = data.decode().split("-")
                self.logger.debug(
                    "Recived data: name: %s, status: %s", name, status)
                self.queue.put((name, status))
            conn.close()

    def send(self, data):
        if self._file.is_socket():
            self.socket.connect(str(self._file))
            self.socket.send(data.encode())
        else:
            raise RuntimeError(
                "socket '{}' not exists".format(str(self._file)))

    def close(self):
        self.socket.close()
        try:
            if self._file.is_socket():
                self._file.unlink()
        except:
            pass

    @property
    def logger(self):
        return logging.getLogger(__package__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type:
            raise exc_type(exc_val)


def listen_job_status(sfile, queue):
    with JobSocket(socket_file=sfile, queue=queue) as js:
        js.listen()


def send_job_status(sfile, name, status):
    js = JobSocket(socket_file=sfile)
    data = "{}-{}".format(name, status)
    js.send(data)
