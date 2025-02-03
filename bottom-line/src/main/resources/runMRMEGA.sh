#!/bin/bash -xe

WORKING_DIR="$1"

# remove arguments
shift 1

# create the directory
mkdir -p "${WORKING_DIR}"

# get all the input files as rest of arguments
INPUT_FILES=("$@")

# append each input file to the script
for INPUT_FILE in "${INPUT_FILES[@]}"; do
    echo "${INPUT_FILE}" >> "MRMEGA.in"
done

# run it
/home/hadoop/bin/MR-MEGA/MR-MEGA -qt -o "${WORKING_DIR}/MRMEGA.tbl" -i MRMEGA.in
