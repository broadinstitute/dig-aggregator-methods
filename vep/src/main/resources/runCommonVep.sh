#!/bin/bash -xe

# set where the source and destination is in S3 and where VEP is
S3_IN="$INPUT_PATH/out/varianteffect"
S3_OUT="$OUTPUT_PATH/out/varianteffect"
VEPDIR="/mnt/var/vep"

# get the name of the part file from the command line; set the output filename
PART=$(basename -- "$1")
OUTFILE="${PART%.*}.json"
WARNINGS="${OUTFILE}_warnings.txt"

# update the path to include samtools and tabix
PATH="$PATH:$VEPDIR/samtools-1.9/:$VEPDIR/ensembl-vep/htslib"

# copy the part file from S3 to local
aws s3 cp "$S3_IN/variants/$PART" .

# ensure the file is sorted
sort -k1,1 -k2,2n "$PART" > "$PART.sorted"

# count the number of processors (used for forking)
CPUS=7

perl "$VEPDIR/ensembl-vep/vep" \
    --dir "$VEPDIR" \
    --fork "$CPUS" \
    --ASSEMBLY GRCh37 \
    --json \
    --offline \
    --no_stats \
    --nearest symbol \
    --symbol \
    --af_1kg \
    --pick_allele \
    --exclude_null_alleles \
    --pick_order tsl,biotype,appris,rank,ccds,canonical,length \
    -i "$PART.sorted" \
    -o "$OUTFILE" \
    --force_overwrite

# copy the output of VEP back to S3
aws s3 cp "$OUTFILE" "$S3_OUT/common-effects/$OUTFILE"

# delete the input and output files; keep the cluster clean
rm "$PART"
rm "$PART.sorted"
rm "$OUTFILE"

# check for a warnings file, upload that, too and then delete it
if [ -e "$WARNINGS" ]; then
    aws s3 cp "$WARNINGS" "$S3_OUT/common-warnings/$WARNINGS"
    rm "$WARNINGS"
fi
