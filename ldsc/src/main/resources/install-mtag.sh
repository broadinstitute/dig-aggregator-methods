#!/bin/bash -xe

MTAG_ROOT=/mnt/var/mtag
sudo mkdir -p "$MTAG_ROOT"
cd "$MTAG_ROOT"

sudo yum -y install git
sudo git clone -b python3 https://github.com/psmadbec/mtag.git

sudo pip3.11 install numpy
sudo pip3.11 install scipy
sudo pip3.11 install pandas
sudo pip3.11 install joblib
sudo pip3.11 install bitarray

for ANCESTRY in AFR AMR EAS EUR SAS
do
  sudo mkdir -p ./ldscore/$ANCESTRY
  sudo aws s3 cp s3://dig-analysis-data/bin/ldsc/ldscore/ldscore_$ANCESTRY.zip ./ldscore/$ANCESTRY/
  sudo unzip ./ldscore/$ANCESTRY/ldscore_$ANCESTRY.zip -d ./ldscore/$ANCESTRY/
  sudo rm ./ldscore/$ANCESTRY/ldscore_$ANCESTRY.zip
done

