#!python
# coding=utf-8
import os
import logging
import unittest


def resource(*args):
    return os.path.join(
        os.path.dirname(__file__),
        'resources',
        *args
    )


def output(*args):
    return os.path.join(
        os.path.dirname(__file__),
        'output',
        *args
    )


def setup_testing_logger(level=None):
    level = level or logging.DEBUG
    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    sh.setLevel(level)
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [sh]


class GutilsTestClass(unittest.TestCase):
    def setUp(self):
        level = os.environ.get('GUTILS_LOGGING_LEVEL', 'DEBUG').upper()
        setup_testing_logger(getattr(logging, level))
