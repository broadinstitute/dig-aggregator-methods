#!/bin/bash

RAW_ROOT=/mnt/var/single_cell

sudo aws s3 cp s3://dig-analysis-bin/genes/GRCh37/part-00000.json ${RAW_ROOT}/genes.json
