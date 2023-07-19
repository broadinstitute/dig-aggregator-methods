#!/bin/bash -xe

# create the work dir
WORK_DIR="/mnt/var/basset"
sudo mkdir -p "${WORK_DIR}"

sudo aws s3 cp s3://dig-analysis-data/bin/regionpytorch/basset_tissue_conversion.txt "${WORK_DIR}"

sudo yum install -y zstd
