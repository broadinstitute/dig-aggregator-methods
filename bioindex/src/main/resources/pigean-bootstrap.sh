#!/bin/bash -xe

PIGEAN_ROOT=/mnt/var/pigean
sudo mkdir -p "${PIGEAN_ROOT}"
sudo chmod 775 "${PIGEAN_ROOT}"
cd "${PIGEAN_ROOT}"

sudo aws s3 cp s3://dig-analysis-cfde/pigean/bin/study_to_efo.tsv .
