#!/bin/bash -xe

## NOTE: This uses a python 3 version of ldsc which is saved as a zip in s3
## Developed with python 3.8.12 installed

LDSC_ROOT=/mnt/var/ldsc
SUB_REGION=default

# install to the root directory
sudo mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# download all annot files for all ancestries
sudo mkdir -p annot
sudo aws s3 cp s3://dig-analysis-data/out/ldsc/regions/$SUB_REGION/ld_score/ ./annot/ --recursive
