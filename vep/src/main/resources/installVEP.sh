#!/bin/bash -xe

VEP_ROOT=/mnt/var/vep

# NOTE: This is performed as a STEP instead of a bootstrap step, because AWS
#       will timeout the cluster if the bootstrap takes over 1 hour to run.

# create a vep directory in /mnt/var to copy data locally
mkdir -p $VEP_ROOT
chmod 775 $VEP_ROOT

# install to the VEP directory
cd $VEP_ROOT

# download the VEP program + data and extract it
aws s3 cp s3://dig-analysis-data/bin/vep/vep-94.tar.gz .
tar zxf vep-94.tar.gz

# build and install ensembl-xs locally
cd "$VEP_ROOT/ensembl-xs"
perl Makefile.PL
make
sudo make install
