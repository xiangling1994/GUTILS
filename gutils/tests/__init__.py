#!python
# coding=utf-8
import os
import logging


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


def setup_testing_logger():
    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(sh)
