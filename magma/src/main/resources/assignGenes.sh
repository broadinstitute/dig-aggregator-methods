#!/bin/bash -xe

# set where the source and destination is in S3
S3_IN="${INPUT_PATH}/out/magma"
S3_OUT="${OUTPUT_PATH}/out/magma"
MAGMA_DIR="/mnt/var/magma"

# copy all the variants into a single file
hadoop fs -getmerge -nl "${S3_IN}/variants/*.csv" variants.csv

# run magma command
"${MAGMA_DIR}/magma" --annotate --snp-loc ./variants.csv --gene-loc "${MAGMA_DIR}/NCBI37.3.gene.loc" --out ./variants

# copy the output of MAGMA back to S3
aws s3 cp ./variants.genes.annot "${S3_OUT}/staging/variants/variants.genes.annot"
aws s3 cp ./variants.log "${S3_OUT}/staging/variants/variants.log"
