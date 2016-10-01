# -*- coding: utf-8 -*-

import logging


class InfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno < logging.WARNING


class InfoHandler(logging.StreamHandler):
    def __init__(self, *args, **kwargs):
        logging.StreamHandler.__init__(self, *args, **kwargs)
        self.addFilter(InfoFilter())
