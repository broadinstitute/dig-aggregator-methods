#!/bin/bash -xe

echo "JOB_BUCKET     = ${JOB_BUCKET}"
echo "JOB_METHOD     = ${JOB_METHOD}"
echo "JOB_STAGE      = ${JOB_STAGE}"
echo "JOB_PREFIX     = ${JOB_PREFIX}"

# set where the source and destination is in S3 and the phenotype
OUT_DIR="${JOB_BUCKET}/out/magma"
MAGMA_DIR="/mnt/var/magma"
PHENOTYPE=$(basename -- "$1")

# download a common script for use
aws s3 cp "${JOB_BUCKET}/resources/scripts/getmerge-strip-headers.sh" .
chmod +x getmerge-strip-headers.sh

# download associations for this phenotype locally
./getmerge-strip-headers.sh "${OUT_DIR}/variant-associations/${PHENOTYPE}/part-*" ./associations.csv

# create symbolic links to the magma data
ln -s "${MAGMA_DIR}/g1000_eur.bed" .
ln -s "${MAGMA_DIR}/g1000_eur.bim" .
ln -s "${MAGMA_DIR}/g1000_eur.fam" .
ln -s "${MAGMA_DIR}/g1000_eur.synonyms" .

# copy the variant gene annotations file from S3
aws s3 cp "${OUT_DIR}/staging/variants/variants.genes.annot" .

# run magma
"${MAGMA_DIR}/magma" --bfile ./g1000_eur --pval ./associations.csv ncol=subjects --gene-annot ./variants.genes.annot --out ./associations

# copy the output of VEP back to S3
aws s3 cp ./associations.genes.out "${OUT_DIR}/staging/genes/${PHENOTYPE}/associations.genes.out"
aws s3 cp ./associations.genes.raw "${OUT_DIR}/staging/genes/${PHENOTYPE}/associations.genes.raw"
aws s3 cp ./associations.log "${OUT_DIR}/staging/genes/${PHENOTYPE}/associations.log"

# delete the input and output files to save disk space for other steps
rm associations.*
rm variants.genes.annot
