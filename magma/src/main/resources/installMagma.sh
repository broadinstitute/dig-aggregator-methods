#!/bin/bash -xe

S3DIR="s3://dig-analysis-data"
MAGMA_DIR="/mnt/var/magma"

# make the work directory
sudo mkdir -p "${MAGMA_DIR}"
cd "${MAGMA_DIR}"
sudo mkdir -p "${MAGMA_DIR}"/pathway-genes

# copy data files
sudo aws s3 cp "${S3DIR}/bin/magma/NCBI37.3.gene.loc" .

for ancestry in amr afr eas eur sas
do
  sudo aws s3 cp "${S3DIR}/bin/magma/g1000_${ancestry}.zip" .
  sudo unzip -o g1000_$ancestry.zip
  sudo chmod 777 g1000_$ancestry.bed
  sudo chmod 777 g1000_$ancestry.bim
  sudo chmod 777 g1000_$ancestry.fam
  sudo chmod 777 g1000_$ancestry.synonyms
  sudo rm g1000_$ancestry.zip
done

sudo aws s3 cp "${S3DIR}/bin/magma/pathwayGenes.txt" "${MAGMA_DIR}"/pathway-genes/
sudo chmod 777 "${MAGMA_DIR}/pathway-genes/pathwayGenes.txt"

# copy and extract the magma program
sudo aws s3 cp "${S3DIR}/bin/magma/magma_v1.07bb_static.zip" .
sudo unzip -o magma_v1.07bb_static.zip
sudo chmod 777 "${MAGMA_DIR}/magma"
