#!/bin/bash -xe

PHENOTYPE="$1"
ANCESTRY="$2"

# output HDFS location
S3_IN="$INPUT_PATH/out/metaanalysis"
S3_OUT="$OUTPUT_PATH/out/metaanalysis"

# working directory
LOCAL_DIR="/mnt/var/metal"

# read and output directories
SRCDIR="${S3_IN}/variants/${PHENOTYPE}"
OUTDIR="${LOCAL_DIR}/ancestry-specific/${PHENOTYPE}/${ANCESTRY}"

# local scripts
RUN_METAL="/home/hadoop/bin/runMETAL.sh"
GET_MERGE="/home/hadoop/bin/getmerge-strip-headers.sh"

# start with a clean working directory
sudo rm -rf "${OUTDIR}"
sudo mkdir -p "${OUTDIR}"

# get all the part files for this phenotype
readarray -t PARTS < <(aws s3 ls "${SRCDIR}/" --recursive | grep "/ancestry=${ANCESTRY}/" | grep "/part-" | awk -v srcdir="${SRCDIR}" '{$1=$2=$3=""; print srcdir "/" substr($0,4)}' || true)

# bugger out if there are no parts files
if [[ "${#PARTS[@]}" -eq 0 ]]; then
  exit 0
fi

ANCESTRY_DIR="${OUTDIR}/ancestry=${ANCESTRY}"
ANALYSIS_DIR="${ANCESTRY_DIR}/_analysis"

# get all the unique datasets for this ancestry
readarray -t DATASETS < <(printf '%s\n' "${PARTS[@]}" | while IFS= read -r path; do
    # Extract dataset name from path, handling spaces
    dataset_path=$(dirname "$(dirname "$(dirname "$path")")")
    dataset_name="${dataset_path##*/dataset=}"
    echo "$dataset_name"
done | sort | uniq)

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

sudo zstd --rm "${ANALYSIS_DIR}/scheme=SAMPLESIZE/METAANALYSIS1.tbl"
sudo zstd --rm "${ANALYSIS_DIR}/scheme=STDERR/METAANALYSIS1.tbl"

# upload the resuts to S3
sudo aws s3 cp "${ANALYSIS_DIR}/scheme=SAMPLESIZE/" "${S3_OUT}/bottom-line/staging/ancestry-specific/${PHENOTYPE}/ancestry=${ANCESTRY}/scheme=SAMPLESIZE/" --recursive
sudo aws s3 cp "${ANALYSIS_DIR}/scheme=STDERR/" "${S3_OUT}/bottom-line/staging/ancestry-specific/${PHENOTYPE}/ancestry=${ANCESTRY}/scheme=STDERR/" --recursive
sudo touch "${ANALYSIS_DIR}/_SUCCESS"
sudo aws s3 cp "${ANALYSIS_DIR}/_SUCCESS" "${S3_OUT}/bottom-line/staging/ancestry-specific/${PHENOTYPE}/ancestry=${ANCESTRY}/"

# remove the analysis directory
sudo rm -rf "${OUTDIR}"
