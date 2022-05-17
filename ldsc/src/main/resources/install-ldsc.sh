#!/bin/bash -xe

## WARNING: ldsc requires Python 2.7!

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# install apps
sudo yum install -y python2
sudo yum install -y python2-pip
sudo yum install -y git

# clone the git repository
git clone https://github.com/bulik/ldsc.git

# install dependencies
sudo pip2 install -U bitarray
sudo pip2 install -U nose
sudo pip2 install -U pybedtools
sudo pip2 install -U scipy
sudo pip2 install -U pandas
sudo pip2 install -U numpy

# download the 1000g LD data for ancestries
#aws s3 cp s3://dig-analysis-data/bin/clumping/ . --recursive

# unzip each ancestry file into its own directory
for f in *.zip; do unzip -d "${f%*.zip}" "$f"; done
