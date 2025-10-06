#!/bin/bash -xe

sudo yum install -y git

# bioindex to get access to shared library functions
sudo pip3 install git+https://github.com/broadinstitute/dig-bioindex.git@master#egg=bioindex

#Fix versions to make this more deterministic (latest as of September 15, 2025)
sudo pip3 install -U numpy==1.26.4
sudo pip3 install -U matplotlib==3.9.4
sudo pip3 install -U pandas==1.5.3
sudo pip3 install -U statsmodels==0.14.5

# A quirk of the new system is that this needs to be upgraded, but can't be done without breaking awscli
# This is just directing it to overwrite just the version needed (this may change if AWS changes the image)
sudo pip3 install --target=/usr/local/lib/python3.9/site-packages/ -U python-dateutil==2.8.1 --force-reinstall

sudo yum install -y zstd
