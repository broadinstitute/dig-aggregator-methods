#!/bin/bash
METHOD="$1"
shift
pushd "$METHOD"
sbt "run -c ../ryan_config.json $*"
popd
