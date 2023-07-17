#!/bin/bash -xe

S3DIR="s3://dig-analysis-data"
MAGMA_DIR="/mnt/var/magma"

# make the work directory
mkdir -p "${MAGMA_DIR}"
cd "${MAGMA_DIR}"
mkdir -p "${MAGMA_DIR}"/pathway-genes

# download a common script for use
aws s3 cp "${S3DIR}/resources/scripts/getmerge-strip-headers.sh" "${MAGMA_DIR}/"
chmod +x "${MAGMA_DIR}/getmerge-strip-headers.sh"

# get script
aws s3 cp "${S3DIR}/resources/Magma/geneAssociations.sh" "${MAGMA_DIR}/"
chmod +x "${MAGMA_DIR}/geneAssociations.sh"

# copy data files
aws s3 cp "${S3DIR}/bin/magma/NCBI37.3.gene.loc" .

# download a common script for use
aws s3 cp "${S3DIR}/resources/scripts/getmerge-strip-headers.sh" .
chmod +x getmerge-strip-headers.sh

aws s3 cp "${S3DIR}/bin/magma/g1000_amr.zip" .
unzip -o g1000_amr.zip
aws s3 cp "${S3DIR}/bin/magma/g1000_afr.zip" .
unzip -o g1000_afr.zip
aws s3 cp "${S3DIR}/bin/magma/g1000_eas.zip" .
unzip -o g1000_eas.zip
aws s3 cp "${S3DIR}/bin/magma/g1000_eur.zip" .
unzip -o g1000_eur.zip
aws s3 cp "${S3DIR}/bin/magma/g1000_sas.zip" .
unzip -o g1000_sas.zip

aws s3 cp "${S3DIR}/bin/magma/pathwayGenes.txt" "${MAGMA_DIR}"/pathway-genes/

# copy and extract the magma program
aws s3 cp "${S3DIR}/bin/magma/magma_v1.07bb_static.zip" .
unzip -o magma_v1.07bb_static.zip
