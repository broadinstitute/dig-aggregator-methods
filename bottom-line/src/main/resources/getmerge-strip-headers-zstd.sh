#!/bin/bash
if [[ ! $# -eq 2 ]]; then
  echo "Usage: merge.sh <hdfs-glob> <local-file>"
  exit 1
fi

# extract parameters
glob=$1
outfile=$2

# download files
mkdir -p tmp_files
aws s3 cp $glob ./tmp_files/ --recursive
zstd -d --rm ./tmp_files/part-*

# merge files
awk '(NR == 1) || (FNR > 1)' ./tmp_files/part-* > "$outfile"
rm -r tmp_files
