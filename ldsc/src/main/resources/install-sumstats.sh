#!/bin/bash -xe

## NOTE: This uses a python 3 version of ldsc which is saved as a zip in s3
## Developed with python 3.8.12 installed

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# Download zipped version of python 3 ldsc codebase
aws s3 cp s3://dig-analysis-data/bin/ldsc/ldsc-python-3-2023-03-09.zip ./
unzip ldsc-python-3-2023-03-09.zip -d ./ldsc/

## Download hapmap3 snps (full)
# From https://data.broadinstitute.org/alkesgroup/LDSCORE/w_hm3.snplist.bz2 downloaded September 9, 2022
aws s3 cp s3://dig-analysis-data/bin/ldsc/w_hm3.snplist.bz2 ./
bunzip2 w_hm3.snplist.bz2
mkdir ./snps
mv w_hm3.snplist ./snps/

# install dependencies
sudo yum install -y python3-devel

# reinstall numpy with openblas for multithreading
sudo yum -y install openblas-devel
sudo pip3 uninstall -y numpy
sudo pip3 install numpy

# install rest of python dependencies
sudo pip3 install -U bitarray
sudo pip3 install -U boto3
sudo pip3 install -U sqlalchemy
sudo pip3 install -U pymysql
sudo pip3 install -U Cython
sudo pip3 install -U numpy
sudo pip3 install -U pybind11
sudo pip3 install -U pythran
sudo pip3 install -U scipy
sudo pip3 install -U pandas

# fetch snps for mapping
aws s3 cp s3://dig-analysis-data/out/varianteffect/snp/ ./ --recursive --exclude="_SUCCESS"
head -n 1 ./part-*.csv | uniq >> ./snp.csv
tail -n +2 ./part-*.csv >> ./snp.csv
rm ./part-*.csv
