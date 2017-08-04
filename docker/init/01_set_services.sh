#!/bin/bash
set -e
. /etc/profile

if [ ! -z "$RUN_BINARY_WATCH" ]; then
    mkdir -p /etc/service/gutils_binary_watch
    cp $PROJECT_ROOT/docker/gutils_binary_watch /etc/service/gutils_binary_watch/run
fi

if [ ! -z "$RUN_ASCII_WATCH" ]; then
    mkdir -p /etc/service/gutils_ascii_watch
    cp $PROJECT_ROOT/docker/gutils_ascii_watch /etc/service/gutils_ascii_watch/run
fi

if [ ! -z "$RUN_NETCDF_WATCH" ]; then
    mkdir -p /etc/service/gutils_netcdf_watch
    cp $PROJECT_ROOT/docker/gutils_netcdf_watch /etc/service/gutils_netcdf_watch/run
fi
