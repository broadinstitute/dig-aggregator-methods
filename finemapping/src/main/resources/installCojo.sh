#!/bin/bash -xe

S3DIR="s3://dig-analysis-data"
COJO_DIR="/mnt/var/cojo"

# make the work directory
mkdir -p "${COJO_DIR}"
cd "${COJO_DIR}"

# make the ancestry directories
mkdir EU 
mkdir AA 
mkdir EA 
mkdir SA 
mkdir HS 

# copy the g1000 data files
aws s3 cp "${S3DIR}/bin/g1000/g1000_eur.bed" .
aws s3 cp "${S3DIR}/bin/magma/g1000_eur.bim" .
aws s3 cp "${S3DIR}/bin/magma/g1000_eur.fam" .
aws s3 cp "${S3DIR}/bin/magma/g1000_eur.synonyms" .

# copy and extract the magma program
aws s3 cp "${S3DIR}/bin/magma/magma_v1.07bb_static.zip" .
unzip magma_v1.07bb_static.zip
