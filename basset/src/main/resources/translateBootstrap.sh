#!/bin/bash -xe

# create the work dir
WORK_DIR="/mnt/var/basset"
mkdir -p "${WORK_DIR}"

aws s3 cp s3://dig-analysis-data/bin/regionpytorch/basset_tissue_conversion.txt "${WORK_DIR}"
