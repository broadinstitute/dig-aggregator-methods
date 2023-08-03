#!/bin/bash -xe

echo "JOB_BUCKET     = ${JOB_BUCKET}"
echo "JOB_METHOD     = ${JOB_METHOD}"
echo "JOB_STAGE      = ${JOB_STAGE}"
echo "JOB_PREFIX     = ${JOB_PREFIX}"

# set where the source and destination is in S3 and the phenotype
OUT_DIR="${JOB_BUCKET}/out/magma"
MAGMA_DIR="/mnt/var/magma"
PHENOTYPE=$1
ANCESTRY=$2
G1000_ANCESTRY=$3



PARTSDIR="${OUT_DIR}/variant-associations/${PHENOTYPE}/ancestry=$ANCESTRY/part-*"

# get all the part files for this phenotype
PARTS=($(hadoop fs -ls -C $PARTSDIR)) || PARTS=()

# bugger out if there are no parts files
if [[ "${#PARTS[@]}" -eq 0 ]]; then
  exit 0
fi

# download associations for this phenotype locally
"${MAGMA_DIR}/getmerge-strip-headers.sh" $PARTSDIR ./associations.csv

# create symbolic links to the magma data
ln -s "${MAGMA_DIR}/g1000_$G1000_ANCESTRY.bed" .
ln -s "${MAGMA_DIR}/g1000_$G1000_ANCESTRY.bim" .
ln -s "${MAGMA_DIR}/g1000_$G1000_ANCESTRY.fam" .
ln -s "${MAGMA_DIR}/g1000_$G1000_ANCESTRY.synonyms" .

# copy the variant gene annotations file from S3
aws s3 cp "${OUT_DIR}/staging/variants/variants.genes.annot" .

# run magma (catch error from bad input data)
"${MAGMA_DIR}/magma" --bfile ./g1000_$G1000_ANCESTRY \
  --pval ./associations.csv ncol=subjects \
  --gene-annot ./variants.genes.annot \
  --out ./associations || echo "ERROR running MAGMA"

# For some ancestries the sample size can be prohibitively small to run magma, copy if files exist
aws s3 cp ./associations.genes.out "${OUT_DIR}/staging/genes/${PHENOTYPE}/ancestry=${ANCESTRY}/associations.genes.out" || echo "No .out file present"
aws s3 cp ./associations.genes.raw "${OUT_DIR}/staging/genes/${PHENOTYPE}/ancestry=${ANCESTRY}/associations.genes.raw" || echo "No .raw file present"
aws s3 cp ./associations.log "${OUT_DIR}/staging/genes/${PHENOTYPE}/ancestry=${ANCESTRY}/associations.log" || echo "No .log file present"

# delete the input and output files to save disk space for other steps
rm associations.*
rm variants.genes.annot
