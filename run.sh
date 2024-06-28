#!/bin/bash
METHOD="$1"
shift
pushd "$METHOD"
sbt "run -c ../ryan_T1D_config.json $*"
popd
