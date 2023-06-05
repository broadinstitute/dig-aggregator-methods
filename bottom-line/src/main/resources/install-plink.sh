#!/bin/bash -xe

CLUMPING_ROOT=/mnt/var/clumping

# create a clumping directory in /mnt/var to copy data locally
mkdir -p "${CLUMPING_ROOT}"
chmod 775 "${CLUMPING_ROOT}"

# install to the metal directory
cd "${CLUMPING_ROOT}"

# download the 1000g BED/BIM/FAM data and PLINK 1.9
aws s3 cp s3://dig-analysis-data/bin/clumping/ . --recursive
aws s3 cp s3://dig-analysis-data/bin/plink/plink_linux_x86_64_20201019.zip .

# unzip each ancestry file into its own directory
for f in *.zip; do unzip -d "${f%*.zip}" "$f"; done

# unzip the plink executable
unzip plink_linux_x86_64_20201019.zip

# download the getmerge-strip-headers script
aws s3 cp s3://dig-analysis-data/resources/scripts/getmerge-strip-headers.sh .
chmod +x getmerge-strip-headers.sh

# download the dbSNP mapping file
./getmerge-strip-headers.sh s3://dig-analysis-data/out/varianteffect/snp/part-*.csv ./snps.csv

# install packages
sudo yum -y install python3-devel
sudo yum install -y zstd

sudo pip3 install cython==0.29.24
sudo pip3 install pandas==1.3.3
sudo pip3 install scipy==1.7.1
