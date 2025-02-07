#!/bin/bash -xe

PHENOTYPE="$1"

# output HDFS location
S3_IN="$INPUT_PATH/out/metaanalysis"
S3_OUT="$INPUT_PATH/out/metaanalysis"

# working directory
LOCAL_DIR="/mnt/var/MRMEGA"

# read and output directories
SRCDIR="${S3_IN}/bottom-line/ancestry-specific/${PHENOTYPE}"
OUTDIR="${LOCAL_DIR}/random-effects/${PHENOTYPE}"

# local scripts
RUN_MRMEGA="/home/hadoop/bin/runMRMEGA.sh"

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
    GLOB="${SRCDIR}/ancestry=${ANCESTRY}/"
    ANCESTRY_DIR="${OUTDIR}/ancestry=${ANCESTRY}"
    JSON_FILE="${ANCESTRY_DIR}/variants.json"
    CSV_FILE="${ANCESTRY_DIR}/variants.csv"

    # create the destination directory and merge variants there
    sudo mkdir -p tmp_files
    sudo mkdir -p "${ANCESTRY_DIR}"
    sudo chmod 777 "${ANCESTRY_DIR}"

    sudo aws s3 cp "${GLOB}" ./tmp_files/ --recursive
    sudo zstd -d --rm ./tmp_files/part-*
    cat ./tmp_files/part-* > "${JSON_FILE}"
    sudo rm -r tmp_files

    # use jq to convert the json file to csv
    head -n 1 "${JSON_FILE}" | sudo jq -r 'keys_unsorted | @tsv' > "${CSV_FILE}"
    cat "${JSON_FILE}" | sudo jq -r 'map(.) | @tsv' >> "${CSV_FILE}"
    sudo rm "${JSON_FILE}"
done

# where to run the analysis
ANALYSIS_DIR="${OUTDIR}/_analysis"

# collect all the input files together into an array
find "${OUTDIR}" -name "variants.csv" | xargs realpath > "${ANALYSIS_DIR}/MRMEGA.in"

# run MRMEGA across all ancestries
sudo bash "${RUN_MRMEGA}" "${ANALYSIS_DIR}"

sudo zstd --rm "${ANALYSIS_DIR}/MRMEGA.tbl"

# upload the results to S3
sudo aws s3 cp "${ANALYSIS_DIR}/" "${S3_OUT}/bottom-line/staging/random-effects/${PHENOTYPE}/" --recursive
sudo touch "${ANALYSIS_DIR}/_SUCCESS"
sudo aws s3 cp "${ANALYSIS_DIR}/_SUCCESS" "${S3_OUT}/bottom-line/staging/random-effects/${PHENOTYPE}/"

sudo rm -rf "${OUTDIR}"
