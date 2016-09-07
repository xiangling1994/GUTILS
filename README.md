# Glider Utilities (GUTILS)

A set of Python utilities for post processing glider data


## Installation

Available through [`conda`](http://conda.pydata.org/docs/install/quick.html). This library requires Python 3.5 or above.

```
$ conda create -n sgs python=3.5
$ source activate sgs
$ conda install -c axiom-data-science gutils
```

# SECOORA Glider System (SGS)

This package is part of the SECOORA Glider System (SGS) and was originally developed by the [CMS Ocean Technology Group](http://www.marine.usf.edu/COT/) at the University of South Florida. It is now maintained by [SECOORA](http://secoora.org) and [Axiom Data Science](http://axiomdatascience.com).

##### SGS Libraries

* [GDBR](https://github.com/axiom-data-science/GBDR): Reads and merges Teledyne Webb Slocum Glider data from *bd flight and science files.
* [GUTILS](https://github.com/axiom-data-science/GUTILS): A set of Python utilities for post processing glider data.
* [GNCW](https://github.com/axiom-data-science/GNCW): A library for creating NetCDF files for Teledyne Slocum Glider datasets.

##### SGS Applications

* [GSPS](https://github.com/axiom-data-science/GSPS): Watches a directory for new *db flight/science files and publishes the data to a ZeroMQ socket.
* [GDAM](https://github.com/axiom-data-science/GDAM): Watches a directory for new *db flight/science files and inserts the data into a MongoDB instance and then publishes the data to a ZeroMQ socket.
* [GSPS2NC](https://github.com/axiom-data-science/GSPS2NC): Subscribes to a GSPS publishing socket and outputs NetCDF files.
