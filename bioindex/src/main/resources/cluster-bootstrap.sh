#!/bin/bash -xe

sudo yum install -y git

# bioindex to get access to shared library functions
sudo pip3 install git+git://github.com/broadinstitute/dig-bioindex.git@master#egg=bioindex

# other python libs used by various stages
sudo pip3 install matplotlib
sudo pip3 install pandas
sudo pip3 install statsmodels
