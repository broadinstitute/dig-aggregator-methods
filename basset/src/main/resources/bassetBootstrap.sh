#!/bin/bash -xe

# need to upgrade pip
sudo python3 -m pip install --upgrade pip

# upgrading pip moves it to a location root doesn't have in its PATH
sudo ln -s /usr/local/bin/pip /bin/pip
sudo ln -s /usr/local/bin/pip3 /bin/pip3

# install the python libraries
sudo pip3 install torch==1.5.1
sudo pip3 install twobitreader
sudo pip3 install numpy
sudo pip3 install scikit-learn

# create the work dir
WORK_DIR="/mnt/var/basset"
mkdir -p "${WORK_DIR}"

# copy the method resources always uploaded each run
aws s3 cp s3://dig-analysis-data/resources/Basset/fullBassetScript.py "${WORK_DIR}"
aws s3 cp s3://dig-analysis-data/resources/Basset/dcc_basset_lib.py "${WORK_DIR}"

# copy the binary resources
aws s3 cp s3://dig-analysis-data/bin/regionpytorch/hg19.2bit "${WORK_DIR}"
aws s3 cp s3://dig-analysis-data/bin/regionpytorch/basset_pretrained_model_reloaded.pth "${WORK_DIR}"
aws s3 cp s3://dig-analysis-data/bin/regionpytorch/basset_labels.txt "${WORK_DIR}"
