#!/bin/bash -xe

echo "JOB_BUCKET     = ${JOB_BUCKET}"
echo "JOB_METHOD     = ${JOB_METHOD}"
echo "JOB_STAGE      = ${JOB_STAGE}"
echo "JOB_PREFIX     = ${JOB_PREFIX}"
echo "JOB_DRYRUN     = ${JOB_DRYRUN}"
#
# You can also pass command line arguments to the script from your stage.
#

echo "Argument passed: $*"

# set where the source and destination is in S3 and where VEP is
S3DIR="s3://dig-analysis-data/out"

# get the name of the part file from the command line; set the output filename
PART=$(basename -- "$1")
OUTFILE="${PART%.*}claussnitzerlab.json"
WARNINGS="${OUTFILE}_warnings.txt"
WORK_DIR="/mnt/var/claussnitzerlab"

# copy the part file from S3 to local
aws s3 cp "$S3DIR/varianteffect/common/$PART" "${WORK_DIR}"

# copy the basset python files and the model weights file
aws s3 cp s3://dig-analysis-data/bin/regionpytorch/fullNasaScript.py "${WORK_DIR}"
aws s3 cp s3://dig-analysis-data/bin/regionpytorch/dcc_basset_lib.py "${WORK_DIR}"

# cd to work directory
cd "${WORK_DIR}"

# run pytorch script
python3 fullNasaScript.py -i "$PART" -b 100 -o "$OUTFILE"

# copy the output of VEP back to S3
aws s3 cp "$OUTFILE" "$S3DIR/regionpytorch/claussnitzerlab/$OUTFILE"

# delete the input and output files; keep the cluster clean
rm "$PART"
rm "$OUTFILE"

# check for a warnings file, upload that, too and then delete it
if [ -e "$WARNINGS" ]; then
    aws s3 cp "$WARNINGS" "$S3DIR/regionpytorch/claussnitzerlab/warnings/$WARNINGS"
    rm "$WARNINGS"
fi

