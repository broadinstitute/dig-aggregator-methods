#!/bin/bash
METHOD="$1"

python -m unittest discover -s $METHOD/src/test/ -p '*_test.py'
