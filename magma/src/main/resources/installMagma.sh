#!/bin/bash -xe

S3DIR="s3://dig-analysis-data"
MAGMA_DIR="/mnt/var/magma"

# make the work directory
sudo mkdir -p "${MAGMA_DIR}"
cd "${MAGMA_DIR}"
sudo mkdir -p "${MAGMA_DIR}"/pathway-genes

# copy data files
sudo aws s3 cp "${S3DIR}/bin/magma/NCBI37.3.gene.loc" .

sudo aws s3 cp "${S3DIR}/bin/magma/g1000_amr.zip" .
sudo unzip -o g1000_amr.zip
sudo aws s3 cp "${S3DIR}/bin/magma/g1000_afr.zip" .
sudo unzip -o g1000_afr.zip
sudo aws s3 cp "${S3DIR}/bin/magma/g1000_eas.zip" .
sudo unzip -o g1000_eas.zip
sudo aws s3 cp "${S3DIR}/bin/magma/g1000_eur.zip" .
sudo unzip -o g1000_eur.zip
sudo aws s3 cp "${S3DIR}/bin/magma/g1000_sas.zip" .
sudo unzip -o g1000_sas.zip

sudo aws s3 cp "${S3DIR}/bin/magma/pathwayGenes.txt" "${MAGMA_DIR}"/pathway-genes/

# copy and extract the magma program
sudo aws s3 cp "${S3DIR}/bin/magma/magma_v1.07bb_static.zip" .
sudo unzip -o magma_v1.07bb_static.zip
sudo chmod 777 "${MAGMA_DIR}/magma"
