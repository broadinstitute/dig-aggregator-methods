#!/bin/bash -xe

PIGEAN_ROOT=/mnt/var/pigean

# create a directory in /mnt/var to copy data locally
sudo mkdir -p "${PIGEAN_ROOT}"
sudo chmod 775 "${PIGEAN_ROOT}"

# install to the metal directory
cd "${PIGEAN_ROOT}"

sudo aws s3 cp s3://dig-analysis-bin/pigean/methods/factor_graph.py .

# install dependencies
sudo pip3.11 install numpy
sudo pip3.11 install scipy
sudo pip3.11 install networkx
sudo pip3.11 install matplotlib
