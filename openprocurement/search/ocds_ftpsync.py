# -*- coding: utf-8 -*-
import os
import sys
import signal
import os.path
import logging
import logging.config

from ftplib import FTP
from ConfigParser import ConfigParser

logger = logging.getLogger(__name__)


class FTPSyncApp(object):
    config = {
        'host': '127.0.0.1',
        'port': 21,
        'timeout': 120,
        'user': 'anonymous',
        'passwd': 'anonymous@user.tld',
        'ftp_dir': '',
        'local_dir': '',
        'filematch': 'ocds-tender-*.json',
    }

    def __init__(self, config={}):
        self.config.update(config)
        self.config['timeout'] = float(self.config['timeout'])
        self.ftp = FTP()

    def run(self):
        logger.info("Connect to %s:%d",
            self.config['host'],
            self.config['port'])
        self.ftp.connect(
            self.config['host'],
            self.config['port'],
            self.config['timeout'])

        logger.info("Login as %s",
            self.config['user'])
        self.ftp.login(
            self.config['user'],
            self.config['passwd'])

        if self.config['ftp_dir']:
            self.ftp.cwd(self.config['ftp_dir'])

        if self.config['local_dir']:
            logger.info("CD %s", self.config['local_dir'])
            os.chdir(self.config['local_dir'])

        filematch = self.config['filematch']

        for filename in self.ftp.nlst(filematch):
            if os.path.exists(filename):
                logger.info("EXISTS %s", filename)
                continue
            try:
                fp = open(filename, 'wb')
                logger.info("RETR %s", filename)
                self.ftp.retrbinary('RETR ' + filename, fp.write)
                fp.close()
            except Exception as e:
                logger.error("Exception {}".format(e))
                os.unlink(filename)


def signal_handler(signo, frame):
    sys.exit(0)


def main():
    if len(sys.argv) < 2:
        print("Usage: ocds_ftpsync config.ini")
        sys.exit(1)

    logging.config.fileConfig(sys.argv[1])

    parser = ConfigParser()
    parser.read(sys.argv[1])

    signal.signal(signal.SIGTERM, signal_handler)

    config = parser.items('ftpsync')

    app = FTPSyncApp(config)
    app.run()

if __name__ == "__main__":
    main()


