#!/bin/bash -xe

WORKING_DIR=/mnt/var/metaanalysis
sudo mkdir -p "${WORKING_DIR}"
cd "${WORKING_DIR}"

# build Metsoft
sudo mkdir -p Metasoft
cd Metasoft
sudo aws s3 cp s3://dig-analysis-bin/metaanalysis/Metasoft.zip .
sudo unzip Metasoft.zip
cd "${WORKING_DIR}"

# install dependencies
sudo yum install -y zstd
