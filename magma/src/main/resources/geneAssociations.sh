#!/bin/bash -xe

# set where the source and destination is in S3 and the phenotype
S3_IN="${INPUT_PATH}/out/magma"
S3_OUT="${OUTPUT_PATH}/out/magma"
MAGMA_DIR="/mnt/var/magma"
PHENOTYPE=$1
ANCESTRY=$2
G1000_ANCESTRY=$3

# download a common script for use
aws s3 cp "s3://dig-analysis-bin/magma/getmerge-strip-headers.sh" .
chmod +x getmerge-strip-headers.sh

PARTSDIR="${S3_IN}/variant-associations/${PHENOTYPE}/ancestry=$ANCESTRY/part-*"

# get all the part files for this phenotype
PARTS=($(hadoop fs -ls -C $PARTSDIR)) || PARTS=()

# bugger out if there are no parts files
if [[ "${#PARTS[@]}" -eq 0 ]]; then
  exit 0
fi

# download associations for this phenotype locally
./getmerge-strip-headers.sh $PARTSDIR ./associations.csv

# create symbolic links to the magma data
ln -s "${MAGMA_DIR}/g1000_$G1000_ANCESTRY.bed" .
ln -s "${MAGMA_DIR}/g1000_$G1000_ANCESTRY.bim" .
ln -s "${MAGMA_DIR}/g1000_$G1000_ANCESTRY.fam" .
ln -s "${MAGMA_DIR}/g1000_$G1000_ANCESTRY.synonyms" .

# copy the variant gene annotations file from S3
aws s3 cp "${S3_IN}/staging/variants/variants.genes.annot" .

# run magma (catch error from bad input data)
"${MAGMA_DIR}/magma" --bfile ./g1000_$G1000_ANCESTRY \
  --pval ./associations.csv ncol=subjects \
  --gene-annot ./variants.genes.annot \
  --out ./associations || echo "ERROR running MAGMA"

# For some ancestries the sample size can be prohibitively small to run magma, copy if files exist
aws s3 cp ./associations.genes.out "${S3_OUT}/staging/genes/${PHENOTYPE}/ancestry=${ANCESTRY}/associations.genes.out" || echo "No .out file present"
aws s3 cp ./associations.genes.raw "${S3_OUT}/staging/genes/${PHENOTYPE}/ancestry=${ANCESTRY}/associations.genes.raw" || echo "No .raw file present"
aws s3 cp ./associations.log "${S3_OUT}/staging/genes/${PHENOTYPE}/ancestry=${ANCESTRY}/associations.log" || echo "No .log file present"

# delete the input and output files to save disk space for other steps
rm associations.*
rm variants.genes.annot
