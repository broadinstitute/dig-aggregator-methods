#!/bin/bash -xe

LDSC_ROOT=/mnt/var/ldsc
S3DIR=$JOB_BUCKET
PHENOTYPE=$1

# input and output
SRCDIR="${S3DIR}/out/metaanalysis/staging/trans-ethnic/${PHENOTYPE}"
OUTDIR="${S3DIR}/out/ldsc/sumstats/${PHENOTYPE}"

# TODO: Remove references to staging data. Use trans-ethnic results instead
# download the METAL results for this phenotype
aws s3 cp "${SRCDIR}/scheme=SAMPLESIZE/METAANALYSIS1.tbl" .

# name of the output file
SUMSTATS="${PHENOTYPE}.sumstats.gz"
LOG="${PHENOTYPE}.log"

# run the munge script
python2 "${LDSC_ROOT}/ldsc/munge_sumstats.py" \
    --sumstats "METAANALYSIS1.tbl" \
    --out "${PHENOTYPE}"

# copy the output file back to S3
aws s3 cp "${SUMSTATS}" "${OUTDIR}/${SUMSTATS}"
aws s3 cp "${LOG}" "${OUTDIR}/${LOG}"

# delete the file data to make room for other files to be processed
rm METAANALYSIS1.tbl
rm "${SUMSTATS}"
rm "${LOG}"
