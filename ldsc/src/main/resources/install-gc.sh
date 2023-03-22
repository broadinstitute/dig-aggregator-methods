#!/bin/bash -xe

## NOTE: This uses a python 3 version of ldsc which is saved as a zip in s3
## Developed with python 3.8.12 installed

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# clone the git repository (
aws s3 cp s3://dig-analysis-data/bin/ldsc/ldsc-python-3-2023-03-09.zip ./
unzip ldsc-python-3-2023-03-09.zip -d ./ldsc/

# Download all ancestry specific ldscores into the proper location
for ANCESTRY in AFR AMR EAS EUR SAS
do
  mkdir -p ./ldscore/$ANCESTRY
  aws s3 cp s3://dig-analysis-data/bin/ldsc/ldscore/ldscore_$ANCESTRY.zip ./ldscore/$ANCESTRY/
  unzip ./ldscore/$ANCESTRY/ldscore_$ANCESTRY.zip -d ./ldscore/$ANCESTRY/
  rm ./ldscore/$ANCESTRY/ldscore_$ANCESTRY.zip
done

# install dependencies
sudo yum install -y python3-devel

# reinstall numpy with openblas for multithreading
sudo yum -y install openblas-devel
sudo pip3 uninstall -y numpy
sudo pip3 install numpy

# install rest of python dependencies
sudo pip3 install -U bitarray
sudo pip3 install -U boto3
sudo pip3 install -U Cython
sudo pip3 install -U pybind11
sudo pip3 install -U pythran
sudo pip3 install -U scipy
sudo pip3 install -U pandas
