#!/usr/bin/env python

from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()


def version():
    with open('VERSION') as f:
        return f.read().strip()


reqs = [line.strip() for line in open('requirements.txt') if not line.startswith('#')]


setup(
    name='gutils',
    version=version(),
    descriptoin='A set of Python utilities for post processing Teledyne Webb '
                'Slocum Glider data',
    long_description=readme(),
    author='Michael Lindemuth',
    author_email='mlindemu@usf.edu',
    install_requires=reqs,
    url='https://github.com/axiom-data-science/GUTILS',
    packages=[
        'gutils',
        'gutils.ctd',
        'gutils.gps',
        'gutils.yo',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering'
    ],
)
