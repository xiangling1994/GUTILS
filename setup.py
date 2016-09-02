#!/usr/bin/env python

from distutils.core import setup

setup(
    name='gutils',
    version='1.0',
    author='Michael Lindemuth',
    author_email='mlindemu@usf.edu',
    install_requires=[
        'gbdr',
        'gsw',
        'numpy',
        'scipy'
    ],
    packages=[
        'gutils',
        'gutils.yo',
        'gutils.gps',
        'gutils.ctd'
    ]
)
