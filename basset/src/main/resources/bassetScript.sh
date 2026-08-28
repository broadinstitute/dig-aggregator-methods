#!/bin/bash -xe

echo "Argument passed: $*"

# set where the source and destination is in S3 and where VEP is
S3DIR="s3://dig-analysis-data/out"

# get the name of the part file from the command line; set the output filename
PART=$(basename -- "$1")
OUTFILE="${PART%.*}.json"
VARIANTS="variants.csv"
BASSET_DIR="/mnt/var/basset"
STEP_DIR="${PWD}"

# copy the part file from S3 to local
aws s3 cp "$S3DIR/varianteffect/variants/$PART" .

# keep only the variant ID column from the input file
cut -f 6 "${PART}" > "${STEP_DIR}/${VARIANTS}"

# do work from the basset directory
cd "${BASSET_DIR}"

# run pytorch script
python3 "fullBassetScript.py" -i "${STEP_DIR}/$VARIANTS" -b 100 -o "${STEP_DIR}/$OUTFILE"

# compress and copy the output of VEP back to S3
zstd "${STEP_DIR}/${OUTFILE}"
aws s3 cp "${STEP_DIR}/${OUTFILE}.zst" "$S3DIR/basset/variants/${OUTFILE}.zst"

# delete the files to save space for future steps
rm "${STEP_DIR}/${OUTFILE}"
rm "${STEP_DIR}/${OUTFILE}.zst"
rm "${STEP_DIR}/${VARIANTS}"
rm "${STEP_DIR}/${PART}"
