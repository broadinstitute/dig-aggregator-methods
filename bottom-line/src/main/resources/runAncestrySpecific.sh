#!/bin/bash -xe

PHENOTYPE="$1"

# output HDFS location
S3_PATH="s3://dig-analysis-data/out/metaanalysis"

# working directory
LOCAL_DIR="/mnt/var/metal"

# read and output directories
SRCDIR="${S3_PATH}/variants/${PHENOTYPE}"
OUTDIR="${LOCAL_DIR}/ancestry-specific/${PHENOTYPE}"

# local scripts
RUN_METAL="/home/hadoop/bin/runMETAL.sh"
GET_MERGE="/home/hadoop/bin/getmerge-strip-headers-zstd.sh"

# start with a clean working directory
sudo rm -rf "${OUTDIR}"
sudo mkdir -p "${OUTDIR}"

# get all the part files for this phenotype
PARTS=($(hadoop fs -ls -C "${SRCDIR}/*/*/*/part-*")) || PARTS=()

# bugger out if there are no parts files
if [[ "${#PARTS[@]}" -eq 0 ]]; then
  exit 0
fi

# get all the unique ancestries
ANCESTRIES=($(printf '%s\n' "${PARTS[@]}" | xargs dirname | xargs dirname | awk -F "=" '{print $NF}' | sort | uniq))

# Remove Mixed ancestry because we never want to run that through METAL
ANCESTRIES=($(printf '%s\n' "${ANCESTRIES[@]}" | grep -v Mixed)) || ANCESTRIES=()

# for each ancestry get all the datasets
for ANCESTRY in "${ANCESTRIES[@]}"; do
    ANCESTRY_DIR="${OUTDIR}/ancestry=${ANCESTRY}"
    ANALYSIS_DIR="${ANCESTRY_DIR}/_analysis"

    # get all the unique datasets for this ancestry
    DATASETS=($(printf '%s\n' "${PARTS[@]}" | grep "/ancestry=${ANCESTRY}/" | xargs dirname | xargs dirname | xargs dirname | awk -F "=" '{print $NF}' | sort | uniq))

    # collect all the common variants for each dataset together
    for DATASET in "${DATASETS[@]}"; do
        GLOB="${SRCDIR}/dataset=${DATASET}/ancestry=${ANCESTRY}/rare=false/"

        # create the destination directory and merge variants there
        sudo mkdir -p "${ANCESTRY_DIR}/${DATASET}"
        sudo bash "${GET_MERGE}" "${GLOB}" "${ANCESTRY_DIR}/${DATASET}/common.csv"
    done

    # collect all the input files together into an array
    INPUT_FILES=($(find "${ANCESTRY_DIR}" -name "common.csv" | xargs realpath))

    # first run with samplesize (overlap on), then with stderr
    sudo bash "${RUN_METAL}" "SAMPLESIZE" "ON" "${ANALYSIS_DIR}" "${INPUT_FILES[@]}"
    sudo bash "${RUN_METAL}" "STDERR" "OFF" "${ANALYSIS_DIR}" "${INPUT_FILES[@]}"

    # upload the resuts to S3
    sudo aws s3 cp "${ANALYSIS_DIR}/scheme=SAMPLESIZE/" "${S3_PATH}/staging/ancestry-specific/${PHENOTYPE}/ancestry=${ANCESTRY}/scheme=SAMPLESIZE/" --recursive
    sudo aws s3 cp "${ANALYSIS_DIR}/scheme=STDERR/" "${S3_PATH}/staging/ancestry-specific/${PHENOTYPE}/ancestry=${ANCESTRY}/scheme=STDERR/" --recursive

    # remove the analysis directory
    sudo rm -rf "${ANALYSIS_DIR}"
done
