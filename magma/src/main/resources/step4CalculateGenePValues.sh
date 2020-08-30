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
PART1=$(basename -- "$1")
PHENOTYPE=$(basename -- "$2")
WARNINGS="magma_step2_warnings.txt"
WORK_DIR="/mnt/var/magma/step4"

# log
echo "PART1          = ${PART1}"
echo "PHENOTYPE      = ${PHENOTYPE}"

# copy the input file from S3 to local
aws s3 cp "$S3DIR/out/magma/step2VariantToGene/geneVariants.txt" "${WORK_DIR}"
aws s3 cp "$S3DIR/out/magma/step3VariantPValues/$PHENOTYPE/$PART1" "${WORK_DIR}/inputPvalueVariants.txt"

# copy the magma software into the directory
aws s3 cp "$S3DIR/bin/magma/g1000_eur.bed" "${WORK_DIR}"
aws s3 cp "$S3DIR/bin/magma/g1000_eur.bim" "${WORK_DIR}"
aws s3 cp "$S3DIR/bin/magma/g1000_eur.fam" "${WORK_DIR}"
aws s3 cp "$S3DIR/bin/magma/g1000_eur.synonyms" "${WORK_DIR}"
aws s3 cp "$S3DIR/bin/magma/magma_v1.07bb_static.zip" "${WORK_DIR}"

# cd to the work directory
cd "${WORK_DIR}"

# unzip the magma executable
unzip magma_v1.07bb_static.zip

# run magma command
./magma --bfile ./g1000_eur --pval ./inputPvalueVariants.txt ncol=subjects --gene-annot ./geneVariants.txt --out ./genePValues

# copy the output of VEP back to S3
aws s3 cp ./genePValues.genes.out "$S3DIR/out/magma/step4GenePValues/${PHENOTYPE}/genePValues.txt"

# delete the input and output files; keep the cluster clean
rm ./genePValues.genes.out
rm ./geneVariants.txt
rm "./${PART1}"

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
