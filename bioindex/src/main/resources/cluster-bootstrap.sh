#!/bin/bash -xe

sudo yum install -y git

# bioindex to get access to shared library functions
sudo pip3 install git+https://github.com/broadinstitute/dig-bioindex.git@master#egg=bioindex

# other python libs used by various stages
#
#Uninstall numpy so any system-wide numpy version won't conflict with the one Pandas pulls in
sudo pip3 uninstall --yes numpy
#Fix versions to make this more deterministic
sudo pip3 install matplotlib==3.4.2
sudo pip3 install pandas==1.2.5
sudo pip3 install statsmodels==0.12.2

sudo yum install -y zstd
