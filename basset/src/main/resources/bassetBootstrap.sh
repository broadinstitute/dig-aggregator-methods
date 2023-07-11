#!/bin/bash -xe

# install the python libraries
pip3 install torch==1.5.1
pip3 install twobitreader
pip3 install numpy
pip3 install scikit-learn

sudo yum install -y zstd

# create the work dir
WORK_DIR="/mnt/var/basset"
sudo mkdir -p "${WORK_DIR}"

# copy the method resources always uploaded each run
sudo aws s3 cp s3://dig-analysis-data/resources/Basset/fullBassetScript.py "${WORK_DIR}"
sudo aws s3 cp s3://dig-analysis-data/resources/Basset/dcc_basset_lib.py "${WORK_DIR}"

# copy the binary resources
sudo aws s3 cp s3://dig-analysis-data/bin/regionpytorch/hg19.2bit "${WORK_DIR}"
sudo aws s3 cp s3://dig-analysis-data/bin/regionpytorch/basset_pretrained_model_reloaded.pth "${WORK_DIR}"
sudo aws s3 cp s3://dig-analysis-data/bin/regionpytorch/basset_labels.txt "${WORK_DIR}"
