#!/bin/bash -xe

## WARNING: ldsc requires Python 2.7!

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# clone the git repository
git clone https://github.com/bulik/ldsc.git

# install dependencies
sudo pip install bitarray==0.8
sudo pip install nose==1.3
sudo pip install pybedtools==0.7
sudo pip install scipy==0.18
sudo pip install pandas==0.20
sudo pip install numpy==1.16

# download the 1000g LD data for ancestries
aws s3 cp s3://dig-analysis-data/bin/clumping/ . --recursive

# unzip each ancestry file into its own directory
for f in *.zip; do unzip -d "${f%*.zip}" "$f"; done
