#!/bin/sh

if [ "$1" != "" ]; then
    SCRIPT=$1
    shift
    /usr/bin/env python /scripts/$SCRIPT "$@"
else
    echo "No script name"
fi

