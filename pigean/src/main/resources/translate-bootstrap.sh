#!/bin/bash -xe

PIGEAN_ROOT=/mnt/var/pigean

# create a directory in /mnt/var to copy data locally
sudo mkdir -p "${PIGEAN_ROOT}"
sudo chmod 775 "${PIGEAN_ROOT}"

# install to the metal directory
cd "${PIGEAN_ROOT}"

sudo aws s3 cp s3://dig-analysis-bin/pigean/misc/ . --recursive
