from __future__ import absolute_import
import shutil
import socket
import subprocess
import tempfile
import time
import os
import requests
import yaml

__author__ = 'bergundy'


DEFAULT_CONNECT_TIMEOUT = 5.0


def waitPort(host, port, timeout=DEFAULT_CONNECT_TIMEOUT):
    """
    Wait until a socket becomes available for accepting connections
    :param host:
    :param port:
    :param timeout:
    :return:
    """
    deadline = time.time() + timeout
    while deadline > time.time():
        try:
            sock = socket.create_connection((host, port), timeout=min(1, timeout))
        except socket.error:
            continue
        else:
            sock.close()
            return

    raise RuntimeError("Timed out while trying to connect to meduza")


def findUnusedPort():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[-1]
    sock.close()
    return port


class DisposableMeduza(object):
    def __init__(self, meduzaExecutable=None):
        self.mdz = None
        self.port = None
        self.ctlPort = None
        self.tempDir = None
        self.meduzaExecutable = meduzaExecutable or os.getenv('MEDUZA_BIN')
        print self.meduzaExecutable
        assert self.meduzaExecutable, 'No meduzaExecutable param found and MEDUZA_BIN env variable is not set'

    def start(self, connectTimeout=DEFAULT_CONNECT_TIMEOUT):
        self.tempDir = tempfile.mkdtemp()
        os.chmod(self.tempDir, 0o755)
        self.port = findUnusedPort()
        self.ctlPort = findUnusedPort()
        config = {
            'server': {
                'listen': ':{}'.format(self.port),
                'ctl_listen': ':{}'.format(self.ctlPort)
            }
        }
        configfile = os.path.join(self.tempDir, 'meduza.conf')
        with open(configfile, 'w') as f:
            yaml.dump(config, f)

        self.mdz = subprocess.Popen((self.meduzaExecutable, '-test', '-conf', configfile), stderr=subprocess.PIPE)
        while True:
            out = self.mdz.stderr.readline().strip()
            if 'main.serverConfig' in out:
                break
        waitPort("localhost", self.port, connectTimeout)
        waitPort("localhost", self.ctlPort, connectTimeout)
        print "Started meduza!"

    def stop(self):
        if self.mdz is not None:
            self.mdz.terminate()
        shutil.rmtree(self.tempDir, ignore_errors=True)

    def installSchema(self, schema):
        installSchema(schema, self.ctlPort)


def installSchema(schema, port=9966):
    deployUrl = 'http://localhost:{}/deploy'.format(port)

    res = requests.post(deployUrl, schema, headers={"Content-Type": "text/yaml"})

    if res.status_code != 200 or res.content != 'OK':
        raise RuntimeError('Failed to install schema')
