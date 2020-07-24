#!/bin/bash -xe

SCHEME="$1"             # "STDERR" or "SAMPLESIZE"
OVERLAP="$2"            # "ON" or "OFF"
WORKING_DIR="$3"

# remove arguments
shift 3

# create the directory
SCRIPT_DIR="${WORKING_DIR}/scheme=${SCHEME}"
mkdir -p "${SCRIPT_DIR}"

# get all the input files as rest of arguments
INPUT_FILES=("$@")
SCRIPT_FILE="${SCRIPT_DIR}/metal.script"

# base script
cat > "${SCRIPT_FILE}" <<EOF
SCHEME ${SCHEME}
OVERLAP ${OVERLAP}
SEPARATOR TAB
COLUMNCOUNTING LENIENT
MARKERLABEL varId
ALLELELABELS reference alt
PVALUELABEL pValue
EFFECTLABEL beta
WEIGHTLABEL n
STDERRLABEL stdErr
CUSTOMVARIABLE TotalSampleSize
LABEL TotalSampleSize AS n
EOF

# append each input file to the script
for INPUT_FILE in "${INPUT_FILES[@]}"; do
    echo "PROCESS ${INPUT_FILE}" >> "${SCRIPT_FILE}"
done

# set the output file and finish script
cat >> "${SCRIPT_FILE}" <<EOF
OUTFILE ${SCRIPT_DIR}/METAANALYSIS .tbl
ANALYZE
QUIT
EOF

# run it
/home/hadoop/bin/metal "${SCRIPT_FILE}"
