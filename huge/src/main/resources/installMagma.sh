#!/bin/bash -xe

S3DIR="s3://dig-analysis-data"
MAGMA_DIR="/mnt/var/magma"

# make the work directory
mkdir -p "${MAGMA_DIR}"
cd "${MAGMA_DIR}"

# copy data files
aws s3 cp "${S3DIR}/bin/magma/NCBI37.3.gene.loc" .
aws s3 cp "${S3DIR}/bin/magma/g1000_eur.bed" .
aws s3 cp "${S3DIR}/bin/magma/g1000_eur.bim" .
aws s3 cp "${S3DIR}/bin/magma/g1000_eur.fam" .
aws s3 cp "${S3DIR}/bin/magma/g1000_eur.synonyms" .

# copy and extract the magma program
aws s3 cp "${S3DIR}/bin/magma/magma_v1.07bb_static.zip" .
unzip magma_v1.07bb_static.zip
