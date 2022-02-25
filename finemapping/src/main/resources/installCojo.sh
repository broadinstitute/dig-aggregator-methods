#!/bin/bash -xe

S3DIR="s3://dig-analysis-data"
COJO_DIR="/mnt/var/cojo"

# make the work directory
mkdir -p "${COJO_DIR}"
cd "${COJO_DIR}"
mkdir output 
mkdir input 

# copy and unpack the cojo executables
aws s3 cp "${S3DIR}/bin/cojo/gcta_1.93.2beta.zip" .
unzip -o gcta_1.93.2beta.zip
aws s3 cp "${S3DIR}/resources/Finemapping/runCojo.py" .

# copy and unpack the G1000 ancestries
aws s3 cp "${S3DIR}/bin/g1000/g1000_afr.zip" .
unzip -o g1000_afr.zip
aws s3 cp "${S3DIR}/bin/g1000/g1000_amr.zip" .
unzip -o g1000_amr.zip
aws s3 cp "${S3DIR}/bin/g1000/g1000_eas.zip" .
unzip -o g1000_eas.zip
aws s3 cp "${S3DIR}/bin/g1000/g1000_eur.zip" .
unzip -o g1000_eur.zip
aws s3 cp "${S3DIR}/bin/g1000/g1000_sas.zip" .
unzip -o g1000_sas.zip

# install the python libraries
sudo pip3 install boto3
sudo pip3 install numpy==1.19.4
sudo pip3 install pandas==1.1.4

