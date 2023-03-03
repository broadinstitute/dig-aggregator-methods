#!/bin/bash -xe

## NOTE: This uses a python 3 version of ldsc which is saved as a zip in s3
## Developed with python 3.8.12 installed

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
sudo mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# Download zipped version of python 3 ldsc codebase
sudo aws s3 cp s3://dig-analysis-data/bin/ldsc/ldsc-python-3-2023-03-02.zip ./
sudo unzip ldsc-python-3-2023-03-02.zip -d ./ldsc/

## Download hapmap3 snps (full)
# From https://data.broadinstitute.org/alkesgroup/LDSCORE/w_hm3.snplist.bz2 downloaded September 9, 2022
sudo aws s3 cp s3://dig-analysis-data/bin/ldsc/w_hm3.snplist.bz2 ./
sudo bunzip2 w_hm3.snplist.bz2
sudo mkdir ./snps
sudo mv w_hm3.snplist ./snps/

# install dependencies
sudo yum install -y python3-devel
sudo yum install -y zstd
pip3 install -U bitarray
pip3 install -U boto3
pip3 install -U sqlalchemy
pip3 install -U pymysql
pip3 install -U Cython
pip3 install -U numpy
pip3 install -U pybind11
pip3 install -U pythran
pip3 install -U scipy
pip3 install -U pandas

# fetch snps for mapping
sudo aws s3 cp s3://dig-analysis-data/out/varianteffect/snp/ ./ --recursive --exclude="_SUCCESS"
sudo touch ./snp.csv
sudo chmod 777 ./snp.csv
sudo head -n 1 ./part-*.csv | uniq >> ./snp.csv
sudo tail -n +2 ./part-*.csv >> ./snp.csv
sudo rm ./part-*.csv
