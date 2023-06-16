#!/bin/bash
METHOD="$1"
shift
pushd "$METHOD"
SBT_OPTS="-Xmx2g" sbt "run -c ../config.json $*"
popd
