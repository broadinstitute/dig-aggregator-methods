#!/bin/bash -xe

# set where the source and destination is in S3 and the phenotype
S3_IN="${INPUT_PATH}/out/magma"
S3_OUT="${OUTPUT_PATH}/out/magma"
MAGMA_DIR="/mnt/var/magma"
PHENOTYPE=$1
ANCESTRY=$2

# create symbolic links to the magma data
ln -s "${MAGMA_DIR}/pathway-genes/pathwayGenes.txt" .

# check to see that the necessary file exists and bugger out if the needed file is not present
declare FILEINFO=$(aws s3 ls "${S3_IN}/staging/genes/${PHENOTYPE}/ancestry=${ANCESTRY}/associations.genes.raw")
if [[ -z $FILEINFO ]]; then
  exit 0
fi

# copy the genes phenotype associations file computed by magma from S3
aws s3 cp "${S3_IN}/staging/genes/${PHENOTYPE}/ancestry=${ANCESTRY}/associations.genes.raw" .

# run magma
# NOTE: create separate output directory since need to do recursive copy of output since some files don't always get created, so can't specify files to copy
mkdir -p "output_${PHENOTYPE}_${ANCESTRY}"
"${MAGMA_DIR}/magma" --gene-results associations.genes.raw \
  --set-annot pathwayGenes.txt \
  --out "output_${PHENOTYPE}_${ANCESTRY}/associations.pathways"

# copy the output of magma back to S3
cd "output_${PHENOTYPE}_${ANCESTRY}"
aws s3 cp --recursive . "${S3_OUT}/staging/pathways/${PHENOTYPE}/ancestry=${ANCESTRY}/"

# delete the input and output files to save disk space for other steps
# now cleanup output directory
cd ..
rm -rf "output_${PHENOTYPE}_${ANCESTRY}"
