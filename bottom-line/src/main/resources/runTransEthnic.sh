#!/bin/bash -xe

PHENOTYPE="$1"

# output HDFS location
S3_PATH="s3://psmadbec-test/furkan-ta1/out/metaanalysis"

# working directory
LOCAL_DIR="/mnt/var/metal"

# read and output directories
SRCDIR="${S3_PATH}/ancestry-specific/${PHENOTYPE}"
OUTDIR="${LOCAL_DIR}/trans-ethnic/${PHENOTYPE}"

# local scripts
RUN_METAL="/home/hadoop/bin/runMETAL.sh"
GET_MERGE="/home/hadoop/bin/getmerge-strip-headers.sh"

# start with a clean working directory
sudo rm -rf "${OUTDIR}"
sudo mkdir -p "${OUTDIR}"

# find all the variants processed by the ancestry-specific step
PARTS=($(hadoop fs -ls -C "${SRCDIR}/*/part-*")) || PARTS=()

# bugger out if there are no parts files
if [[ "${#PARTS[@]}" -eq 0 ]]; then
  exit 0
fi

# find all the ancestries processed
ANCESTRIES=($(printf '%s\n' "${PARTS[@]}" | xargs dirname | awk -F "=" '{print $NF}' | sort | uniq))

# if no variants/ancestries found then exit (nothing to do)
if [[ "${#ANCESTRIES[@]}" -eq 0 ]]; then
    exit 0
fi

# for each ancestry, merge all the results into a single file
for ANCESTRY in "${ANCESTRIES[@]}"; do
    GLOB="${SRCDIR}/ancestry=${ANCESTRY}/part-*"
    ANCESTRY_DIR="${OUTDIR}/ancestry=${ANCESTRY}"
    JSON_FILE="${ANCESTRY_DIR}/variants.json"
    CSV_FILE="${ANCESTRY_DIR}/variants.csv"

    # create the destination directory and merge variants there
    sudo mkdir -p "${ANCESTRY_DIR}"
    sudo hadoop fs -getmerge -nl -skip-empty-file "${GLOB}" "${JSON_FILE}"

    # use jq to convert the json file to csv
    sudo chmod 777 $ANCESTRY_DIR
    head -n 1 "${JSON_FILE}" | sudo jq -r 'keys_unsorted | @tsv' > "${CSV_FILE}"
    cat "${JSON_FILE}" | sudo jq -r 'map(.) | @tsv' >> "${CSV_FILE}"
done

# where to run the analysis
ANALYSIS_DIR="${OUTDIR}/_analysis"

# collect all the input files together into an array
INPUT_FILES=($(find "${OUTDIR}" -name "variants.csv" | xargs realpath))

# run METAL across all ancestries with OVERLAP OFF
sudo bash "${RUN_METAL}" "SAMPLESIZE" "OFF" "${ANALYSIS_DIR}" "${INPUT_FILES[@]}"
sudo bash "${RUN_METAL}" "STDERR" "OFF" "${ANALYSIS_DIR}" "${INPUT_FILES[@]}"

# nuke any previously existing staging data
sudo aws s3 rm "${S3_PATH}/staging/metaanalysis/trans-ethnic/${PHENOTYPE}/" --recursive

# upload the resuts to S3
sudo aws s3 cp "${ANALYSIS_DIR}/scheme=SAMPLESIZE/" "${S3_PATH}/staging/trans-ethnic/${PHENOTYPE}/scheme=SAMPLESIZE/" --recursive
sudo aws s3 cp "${ANALYSIS_DIR}/scheme=STDERR/" "${S3_PATH}/staging/trans-ethnic/${PHENOTYPE}/scheme=STDERR/" --recursive
