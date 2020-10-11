#!/bin/bash
METHOD="$1"
shift
pushd "$METHOD"
sbt "run -c '../config.json' $*"
popd
