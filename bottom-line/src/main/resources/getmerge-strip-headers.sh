#!/bin/bash
if [[ ! $# -eq 2 ]]; then
  echo "Usage: merge.sh <hdfs-glob> <local-file>"
  exit 1
fi

# extract parameters
glob=$1
outfile=$2

# merge all the data files together
hadoop fs -getmerge -nl -skip-empty-file "$glob" "$outfile"

# discover the header to skip it later
header=$(head -n 1 "$outfile")

# where to do temporary file processing
tmp="$(dirname "$outfile")/tmp.csv"

# run awk to remove extra CSV headers
awk -v h="$header" 'NR>1 && $0~h {next} {print}' "$outfile" > "$tmp" && mv "$tmp" "$outfile"
