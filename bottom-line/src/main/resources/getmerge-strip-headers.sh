#!/bin/bash
if [[ ! $# -eq 2 ]]; then
  echo "Usage: merge.sh <hdfs-glob> <local-file>"
  exit 1
fi

# extract parameters
glob=$1
outfile=$2

# create unique temporary directory to avoid conflicts
tmpdir="tmp_files_$$_$(date +%s)"
mkdir -p "$tmpdir"

# download files
aws s3 cp "$glob" "./$tmpdir/" --recursive
find "./$tmpdir" -name "part-*" -exec zstd -d --rm {} \;

# merge files
find "./$tmpdir" -name "part-*" -exec awk '(NR == 1) || (FNR > 1)' {} + > "$outfile"
rm -r "$tmpdir"
