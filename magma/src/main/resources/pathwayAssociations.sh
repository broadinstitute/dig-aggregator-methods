#!/bin/bash -xe

echo "JOB_BUCKET     = ${JOB_BUCKET}"
echo "JOB_METHOD     = ${JOB_METHOD}"
echo "JOB_STAGE      = ${JOB_STAGE}"
echo "JOB_PREFIX     = ${JOB_PREFIX}"

# set where the source and destination is in S3 and the phenotype
OUT_DIR="${JOB_BUCKET}/out/magma"
MAGMA_DIR="/mnt/var/magma"
PHENOTYPE=$(basename -- "$1")

# create symbolic links to the magma data
ln -s "${MAGMA_DIR}/pathway-genes/pathwayGenes.txt" .

# copy the genes phenotype associations file computed by magma from S3
aws s3 cp "${OUT_DIR}/staging/genes/${PHENOTYPE}/associations.genes.raw" .

# run magma
# "${MAGMA_DIR}/magma" --bfile ./g1000_eur --pval ./associations.csv ncol=subjects --gene-annot ./variants.genes.annot --out ./associations
"${MAGMA_DIR}/magma" --gene-results associations.genes.raw --set-annot pathwayGenes.txt --out associations.pathways

# copy the output of magma back to S3
aws s3 cp ./associations.pathways.gsa.out "${OUT_DIR}/staging/pathways/${PHENOTYPE}/associations.pathways.gsa.out"
aws s3 cp ./associations.pathways.gsa.genes.out "${OUT_DIR}/staging/pathways/${PHENOTYPE}/associations.pathways.gsa.genes.out"
aws s3 cp ./associations.pathways.gsa.sets.genes.out "${OUT_DIR}/staging/pathways/${PHENOTYPE}/associations.pathways.gsa.sets.genes.out"
aws s3 cp ./associations.pathways.log "${OUT_DIR}/staging/pathways/${PHENOTYPE}/associations.pathways.log"

# aws s3 cp ./associations.genes.raw "${OUT_DIR}/staging/genes/${PHENOTYPE}/associations.genes.raw"
# aws s3 cp ./associations.log "${OUT_DIR}/staging/genes/${PHENOTYPE}/associations.log"

# delete the input and output files to save disk space for other steps
rm associations.*
# rm variants.genes.annot
