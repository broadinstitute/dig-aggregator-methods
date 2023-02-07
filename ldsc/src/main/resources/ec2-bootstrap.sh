#!/bin/bash

sudo amazon-linux-extras install python3.8
sudo ln -s /usr/bin/python3.8 /usr/bin/python3

sudo yum install -y python38-devel gcc gcc-c++ curl wget unzip make bzip2-devel zlib-devel xz-devel

wget https://bootstrap.pypa.io/get-pip.py
python3.8 get-pip.py

pip3 install -U bitarray
pip3 install -U boto3
pip3 install -U sqlalchemy
pip3 install -U pymysql
pip3 install -U scipy
pip3 install -U pandas
pip3 install -U pybedtools

wget https://github.com/arq5x/bedtools2/releases/download/v2.30.0/bedtools-2.30.0.tar.gz
tar -zxvf bedtools-2.30.0.tar.gz
cd bedtools2
make
sudo cp bin/* /usr/local/bin/
cd ~

# Needed data
sudo mkdir -p /mnt/
sudo mkdir -p /mnt/var
sudo mkdir -p /mnt/var/ldsc
cd /mnt/var/ldsc

sudo mkdir -p ./g1000/EUR
sudo aws s3 cp s3://dig-analysis-data/bin/ldsc/g1000/g1000_chr_EUR.zip ./
sudo unzip g1000_chr_EUR.zip -d ./g1000/EUR/
sudo rm g1000_chr_EUR.zip

sudo aws s3 cp s3://dig-analysis-data/bin/ldsc/ldsc-python-3-2022-09-06.zip ./
sudo unzip ldsc-python-3-2022-09-06.zip -d ./ldsc/

sudo aws s3 cp s3://dig-analysis-data/bin/ldsc/w_hm3.snplist.bz2 ./
sudo bunzip2 w_hm3.snplist.bz2
sudo mkdir -p ./snps
sudo mv w_hm3.snplist ./snps/

sudo aws s3 cp s3://dig-analysis-data/bin/ldsc/hapmap3_snps.tgz ./
sudo tar -xf hapmap3_snps.tgz
sudo mv hapmap3_snps/* ./snps/
sudo rm -r hapmap3_snps
sudo rm hapmap3_snps.tgz

cd ~
