#!/bin/bash
METHOD="$1"
shift
pushd "$METHOD"
sbt "run -c ../giant_config.json $*"
popd
