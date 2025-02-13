#!/bin/bash -xe

WORKING_DIR="$1"

# remove arguments
shift 1

# run it
/mnt/var/metaanalysis/MR-MEGA/MR-MEGA --name_pos position  --name_chr chromosome --name_n n \
  --name_se stdErr --name_beta beta --name_eaf eaf \
  --name_ea alt --name_nea reference --name_marker varId \
  --qt --no_std_names -o "${WORKING_DIR}/MRMEGA.tbl" -i "${WORKING_DIR}/MRMEGA.in"
