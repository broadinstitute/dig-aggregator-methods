#!/bin/bash -xe
#
# usage: mergeRegions.sh partition
#

S3_DIR="s3://dig-analysis-data/out/ldsr"

# which partition is being made
PARTITION=$1

# create a temporary file to download all the partition data into
TMP_FILE=$(mktemp bed.XXXXXX)
BED_FILE="${PARTITION}.csv"

# merge all the part files together into the temp file
hadoop fs -getmerge -nl -skip-empty-file "${S3_DIR}/regions/partitioned/*/partition=${PARTITION}/part-*" "${TMP_FILE}"

# sort the bed file by chromosome and then start position
sort -u -k1,1 -k2,2n "${TMP_FILE}" | grep -v '^$' > "${BED_FILE}"

# write the bed file back to S3
aws s3 cp "${BED_FILE}" "${S3_DIR}/regions/merged/${PARTITION}/${BED_FILE}"

# delete the merged file to leave space for future steps
rm "${BED_FILE}"
