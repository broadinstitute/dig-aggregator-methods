#!/bin/bash -xe

echo "JOB_BUCKET     = ${JOB_BUCKET}"
echo "JOB_METHOD     = ${JOB_METHOD}"
echo "JOB_STAGE      = ${JOB_STAGE}"
echo "JOB_PREFIX     = ${JOB_PREFIX}"

#
# You can also pass command line arguments to the script from your stage.
#

echo "Argument passed: $*"

#
# You have access to the AWS CLI to copy/read data from S3.
#

aws s3 ls "${JOB_BUCKET}/out/metaanalysis/trans-ethnic/$1/" --recursive

#
# You can also use the hadoop command.
#

hadoop fs -ls "${JOB_BUCKET}/out/metaanalysis/trans-ethnic/$1/*"
