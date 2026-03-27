#!/bin/bash -xe

## NOTE: This uses a python 3 version of ldsc which is saved as a zip in s3
## Developed with python 3.8.12 installed

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
sudo mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# Download zipped version of python 3 ldsc codebase
sudo aws s3 cp s3://dig-analysis-bin/ldsc/ldsc-python-3-2024-03-27.zip ./ldsc.zip
sudo unzip ldsc.zip -d ./ldsc/

## Download hapmap3 snps (full)
# From https://data.broadinstitute.org/alkesgroup/LDSCORE/w_hm3.snplist.bz2 downloaded September 9, 2022
sudo mkdir -p ./snps
sudo aws s3 cp s3://dig-analysis-bin/ldsc/w_hm3.snplist.bz2 ./
sudo bunzip2 w_hm3.snplist.bz2
sudo mv w_hm3.snplist ./snps/

## Download hapmap3 snps (chr specific)
# From https://data.broadinstitute.org/alkesgroup/LDSCORE/hapmap3_snps.tgz downloaded January 20, 2023
sudo aws s3 cp s3://dig-analysis-bin/ldsc/hapmap3_snps.tgz ./
sudo tar -xf hapmap3_snps.tgz
sudo mv hapmap3_snps/* ./snps/
sudo rm -r hapmap3_snps
sudo rm hapmap3_snps.tgz

# install yum dependencies
sudo yum install -y python3-devel
sudo yum install -y bzip2-devel

# reinstall numpy with openblas for multithreading
sudo yum -y install openblas-devel
sudo pip3 uninstall -y numpy
pip3 install numpy

# install rest of python dependencies
pip3 install -U bitarray==1.9.2
pip3 install -U boto3
pip3 install -U sqlalchemy
pip3 install -U pymysql
pip3 install -U Cython
pip3 install -U pybind11
pip3 install -U pythran
pip3 install -U scipy
pip3 install -U pandas
pip3 install -U pybedtools

# install BEDTools
sudo aws s3 cp s3://dig-analysis-bin/ldsc/bedtools-2.30.0.tar.gz ./
sudo tar -zxvf bedtools-2.30.0.tar.gz
cd bedtools2
sudo make
sudo cp bin/* /usr/local/bin/
cd "$LDSC_ROOT"

# g1000 datasets
sudo mkdir -p ./g1000
for ANCESTRY in AFR AMR EAS EUR SAS
do
  sudo mkdir -p ./g1000/$ANCESTRY
  sudo aws s3 cp s3://dig-analysis-bin/ldsc/g1000/g1000_chr_$ANCESTRY.zip ./
  sudo unzip g1000_chr_$ANCESTRY.zip -d ./g1000/$ANCESTRY/
  sudo rm g1000_chr_$ANCESTRY.zip
done

# weights
sudo mkdir -p ./weights
for ANCESTRY in AFR AMR EAS EUR SAS
do
  sudo mkdir -p ./weights/$ANCESTRY
  sudo aws s3 cp s3://dig-analysis-bin/ldsc/weights/weights_$ANCESTRY.zip ./
  sudo unzip weights_$ANCESTRY.zip -d ./weights/$ANCESTRY/
  sudo rm weights_$ANCESTRY.zip
done

# frq
sudo mkdir -p ./frq
for ANCESTRY in AFR AMR EAS EUR SAS
do
  sudo mkdir -p ./frq/$ANCESTRY
  sudo aws s3 cp s3://dig-analysis-bin/ldsc/frq/frq_$ANCESTRY.zip ./
  sudo unzip frq_$ANCESTRY.zip -d ./frq/$ANCESTRY/
  sudo rm frq_$ANCESTRY.zip
done

# baseline
sudo mkdir -p ./baseline
for ANCESTRY in AFR AMR EAS EUR SAS
do
  sudo mkdir -p ./baseline/$ANCESTRY
  sudo aws s3 cp s3://dig-analysis-bin/ldsc/baseline/baseline_$ANCESTRY.zip ./
  sudo unzip baseline_$ANCESTRY.zip -d ./baseline/$ANCESTRY/
  sudo rm baseline_$ANCESTRY.zip
done
