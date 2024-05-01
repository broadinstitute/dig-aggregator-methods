#!/bin/bash -xe

## NOTE: This uses a python 3 version of ldsc which is saved as a zip in s3
## Developed with python 3.8.12 installed

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
sudo mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# clone the git repository (
sudo aws s3 cp s3://dig-analysis-bin/ldsc/ldsc-python-3-2024-03-27.zip ./ldsc.zip
sudo unzip ldsc.zip -d ./ldsc/

# Download all ancestry specific ldscores into the proper location
for ANCESTRY in AFR AMR EAS EUR SAS
do
  sudo mkdir -p ./ldscore/$ANCESTRY
  sudo aws s3 cp s3://dig-analysis-data/bin/ldsc/ldscore/ldscore_$ANCESTRY.zip ./ldscore/$ANCESTRY/
  sudo unzip ./ldscore/$ANCESTRY/ldscore_$ANCESTRY.zip -d ./ldscore/$ANCESTRY/
  sudo rm ./ldscore/$ANCESTRY/ldscore_$ANCESTRY.zip
done

# install dependencies
sudo yum install -y python3-devel

# reinstall numpy with openblas for multithreading
sudo yum -y install openblas-devel
pip3 uninstall -y numpy
pip3 install numpy

# install rest of python dependencies
pip3 install -U bitarray
pip3 install -U boto3
pip3 install -U Cython
pip3 install -U pybind11
pip3 install -U pythran
pip3 install -U scipy
pip3 install -U pandas
