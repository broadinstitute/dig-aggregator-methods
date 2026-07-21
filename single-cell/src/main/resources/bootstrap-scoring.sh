#!/bin/bash -xe

ROOT_DIR=/mnt/var/single_cell

# create a directory in /mnt/var to copy data locally
sudo mkdir -p "${ROOT_DIR}"
sudo chmod 775 "${ROOT_DIR}"

# install to the metal directory
cd "${ROOT_DIR}"

sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_lists/ pigean/ --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_sets/ pigean/ --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/misc/ pigean/ --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/models/ pigean/ --recursive
sudo aws s3 cp s3://dig-analysis-data/out/pigean/staging/combined/ pigean/ --recursive

sudo aws s3 cp s3://dig-analysis-bin/single_cell/scoring/ misc/ --recursive

sudo yum -y install git
sudo git clone https://github.com/flannick/pigean.git pigean/pigean
sudo git clone -b aws-changes https://github.com/flannick/dig-cell-state-scoring.git

# install dependencies
sudo pip3.11 install numpy
sudo pip3.11 install scipy
sudo pip3.11 install pandas
sudo pip3.11 install pyyaml

# install dependencies (python 3.9 which hadoop is running baseline)
sudo pip3 install numpy
