#!/bin/bash -xe

CLUMPING_ROOT=/mnt/var/clumping

# create a clumping directory in /mnt/var to copy data locally
sudo mkdir -p "${CLUMPING_ROOT}"
sudo chmod 775 "${CLUMPING_ROOT}"

# install to the metal directory
cd "${CLUMPING_ROOT}"

# download the 1000g BED/BIM/FAM data and PLINK 1.9
sudo aws s3 cp s3://dig-analysis-bin/clumping/ . --recursive
sudo aws s3 cp s3://dig-analysis-bin/plink/plink_linux_x86_64_20201019.zip .

# unzip each ancestry file into its own directory
for f in *.zip; do sudo unzip -d "${f%*.zip}" "$f"; done

# unzip the plink executable
sudo unzip plink_linux_x86_64_20201019.zip

# download the dbSNP mapping file
sudo aws s3 cp "s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv" ./snps.csv

# install packages
sudo yum -y install python3-devel
sudo yum install -y zstd

pip3 install cython==0.29.24
pip3 install pandas==1.3.3
pip3 install scipy==1.7.1
