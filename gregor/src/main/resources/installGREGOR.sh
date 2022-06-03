#!/bin/bash -xe
#
# usage: installGREGOR.sh <ancestry> [r2]
#           where
#               r2       = "0.2" | "0.7" (default = "0.7")
#

GREGOR_ROOT=/mnt/var/gregor

# s3 location to download from
S3_BUCKET="s3://dig-analysis-data"

GREGOR_DIR="${S3_BUCKET}/out/gregor"
LDSC_DIR="${S3_BUCKET}/out/ldsc"
SUB_REGION="default"

# locations for the region files
BED_INDEX_FILE="${GREGOR_ROOT}/bed.file.index"
REGIONS_DIR="${GREGOR_ROOT}/regions"

# NOTE: This is performed as a STEP instead of a bootstrap step, because AWS
#       will timeout the cluster if the bootstrap takes over 1 hour to run.

# create a gregor directory in /mnt/var to copy data locally
mkdir -p "${GREGOR_ROOT}"
chmod 775 "${GREGOR_ROOT}"

# install to the VEP directory
cd "${GREGOR_ROOT}"

# get the r2 value to use
R2=${1:-"0.7"}

# all ancestry REF files need to be installed
ANCESTRIES=(AFR AMR ASN EUR SAN)

# create the ref directory
mkdir -p ref
cd ref

# install each REF file
for ANCESTRY in "${ANCESTRIES[@]}"; do
    REF="GREGOR.ref.${ANCESTRY}.LD.ge.${R2}.tar.gz"

    # download and extract the tarball for the 1000g reference
    aws s3 cp "${S3_BUCKET}/bin/gregor/${REF}" .
    tar zxf "${REF}"
done

# download and extract GREGOR itself
cd ${GREGOR_ROOT}
aws s3 cp "${S3_BUCKET}/bin/gregor/GREGOR.v1.4.0.tar.gz" .
tar zxf GREGOR.v1.4.0.tar.gz

# At this point, the output of the MergeRegionsStage will be the same for every
# run of GREGOR, so we can copy all the partitioned bed files locally.
#
# The BED_INDEX_FILE will be used in every configuration file sent to GREGOR
# as well, and contain the list of every file.

# ensure that the regions directory exists and bed index file
mkdir -p "${REGIONS_DIR}"

# copy all the partitions to the regions directory
aws s3 cp "${LDSC_DIR}/regions/${SUB_REGION}/merged/" "${REGIONS_DIR}" --recursive
find "${REGIONS_DIR}" -empty -type f -delete

# need to export or bash -c won't inherit
export REGIONS_DIR

# flatten the partitions relocating the files to the regions directory
find "${REGIONS_DIR}" -type f -exec mv -i '{}' "${REGIONS_DIR}" ';'
find "${REGIONS_DIR}" -empty -type d -delete

# remove the csv file extension
find "${REGIONS_DIR}" -type f -name '*.csv' | while read f; do mv "$f" "${f%.*}"; done

# write all the partition files to the index file
find "${REGIONS_DIR}" -type f > "${BED_INDEX_FILE}"
