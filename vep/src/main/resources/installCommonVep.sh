#!/bin/bash -xe

VEP_ROOT=/mnt/var/vep

# NOTE: This is performed as a STEP instead of a bootstrap step, because AWS
#       will timeout the cluster if the bootstrap takes over 1 hour to run.

# create a vep directory in /mnt/var to copy data locally
sudo mkdir -p $VEP_ROOT
sudo chmod 775 $VEP_ROOT

# install to the VEP directory
cd $VEP_ROOT

# download the VEP program + data and extract it
sudo aws s3 cp s3://dig-analysis-bin/vep/vep-97.tar.gz .
sudo tar zxf vep-97.tar.gz
