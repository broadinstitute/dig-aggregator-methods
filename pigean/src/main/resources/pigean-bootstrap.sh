#!/bin/bash -xe

PIGEAN_ROOT=/mnt/var/pigean

# create a directory in /mnt/var to copy data locally
sudo mkdir -p "${PIGEAN_ROOT}"
sudo chmod 775 "${PIGEAN_ROOT}"

# install to the metal directory
cd "${PIGEAN_ROOT}"

sudo aws s3 cp s3://dig-analysis-data/out/pigean/gene_lists/ . --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_sets/ . --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/misc/ . --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/models/ . --recursive

sudo yum -y install git
sudo git clone https://github.com/flannick/pigean.git
sudo aws s3 cp s3://dig-analysis-data/out/pigean/staging/combined/ . --recursive

# install dependencies
sudo pip3.11 install numpy
sudo pip3.11 install scipy
