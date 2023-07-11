#!/bin/bash -xe

# set where the source and destination is in S3 and the phenotype
OUT_DIR="s3://dis-analysis-data/out/magma"
MAGMA_DIR="/mnt/var/magma"
PHENOTYPE=$1
INPUT_TYPE=$2
ANCESTRY_OR_DATASET=$3
G1000_ANCESTRY=$4

if [[ $INPUT_TYPE == "ancestry" ]]
then
  INPUT_PARTITION="ancestry=${ANCESTRY_OR_DATASET}"
else
  INPUT_PARTITION="dataset=${ANCESTRY_OR_DATASET}"
fi
PARTSDIR="${OUT_DIR}/variant-associations/${PHENOTYPE}/${INPUT_PARTITION}/part-*"

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
aws s3 cp ./associations.genes.out "${OUT_DIR}/staging/genes/${PHENOTYPE}/${INPUT_PARTITION}/associations.genes.out" || echo "No .out file present"
aws s3 cp ./associations.genes.raw "${OUT_DIR}/staging/genes/${PHENOTYPE}/${INPUT_PARTITION}/associations.genes.raw" || echo "No .raw file present"
aws s3 cp ./associations.log "${OUT_DIR}/staging/genes/${PHENOTYPE}/${INPUT_PARTITION}/associations.log" || echo "No .log file present"
touch _SUCCESS
aws s3 cp _SUCCESS "${OUT_DIR}/staging/genes/${PHENOTYPE}/${INPUT_PARTITION}/"

# delete the input and output files to save disk space for other steps
rm associations.*
rm variants.genes.annot
rm _SUCCESS
