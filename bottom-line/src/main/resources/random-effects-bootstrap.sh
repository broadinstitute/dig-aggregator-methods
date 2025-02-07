#!/bin/bash -xe

WORKING_DIR=/mnt/var/metaanalysis
sudo mkdir -p "${WORKING_DIR}"
cd "${WORKING_DIR}"

sudo aws s3 cp s3://dig-analysis-bin/bin/intake/var_to_af.zip .
sudo unzip var_to_af.zip
sudo rm var_to_af.zip

# install dependencies
sudo yum install -y zstd
