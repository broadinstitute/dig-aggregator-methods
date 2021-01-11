#!/bin/bash -xe

echo "JOB_BUCKET     = ${JOB_BUCKET}"
echo "JOB_METHOD     = ${JOB_METHOD}"
echo "JOB_STAGE      = ${JOB_STAGE}"
echo "JOB_PREFIX     = ${JOB_PREFIX}"

#
# You can also pass command line arguments to the script from your stage.
#

echo "Argument passed: $*"


# set where the source and destination is in S3 and where VEP is
S3DIR="s3://dig-analysis-data"

# get the name of the part file from the command line; set the output filename
PART=$(basename -- "$1")
WARNINGS="magma_step2_warnings.txt"
WORK_DIR="/mnt/var/magma/step2"

# copy the input file from S3 to local
aws s3 cp "$S3DIR/out/magma/step1GatherVariants/$PART" "${WORK_DIR}/inputVariants.txt"

# cd to the work directory
cd "${WORK_DIR}"

# run magma command
./magma --annotate --snp-loc ./inputVariants.txt --gene-loc ./NCBI37.3.gene.loc --out ./geneVariants

# copy the output of VEP back to S3
aws s3 cp ./geneVariants.genes.annot "$S3DIR/out/magma/step2VariantToGene/geneVariants.txt"

# delete the input and output files; keep the cluster clean
rm ./geneVariants.genes.annot

# check for a warnings file, upload that, too and then delete it
if [ -e "$WARNINGS" ]; then
    aws s3 cp "$WARNINGS" "$S3DIR/regionpytorch/basset/warnings/$WARNINGS"
    rm "$WARNINGS"
fi

#
# You have access to the AWS CLI to copy/read data from S3.
#

# aws s3 ls "${JOB_BUCKET}/out/metaanalysis/trans-ethnic/$1/" --recursive

#
# You can also use the hadoop command.
#

# hadoop fs -ls "${JOB_BUCKET}/out/metaanalysis/trans-ethnic/$1/*"
