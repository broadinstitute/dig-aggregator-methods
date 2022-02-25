#!/bin/bash -xe

echo "JOB_BUCKET     = ${JOB_BUCKET}"
echo "JOB_METHOD     = ${JOB_METHOD}"
echo "JOB_STAGE      = ${JOB_STAGE}"
echo "JOB_PREFIX     = ${JOB_PREFIX}"

# set where the source and destination is in S3 and the phenotype
PHENOTYPE=$(basename -- "$1")
IN_DIR=$(basename -- "$2")
OUT_DIR=$(basename -- "$3")
COJO_DIR="/mnt/var/cojo"

# run the command
python3 ${COJO_DIR}/runCojo.py ${PHENOTYPE} ${IN_DIR} ${OUT_DIR}

