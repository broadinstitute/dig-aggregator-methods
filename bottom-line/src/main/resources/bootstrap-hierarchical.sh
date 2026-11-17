#!/bin/bash -xe

METAL_ROOT=/mnt/var/metal

sudo mkdir -p "${METAL_ROOT}"
sudo chmod 775 "${METAL_ROOT}"
cd "${METAL_ROOT}"

sudo aws s3 cp s3://dig-analysis-bin/metaanalysis/metal-2026-11-13.zip .
sudo unzip metal-2026-11-13.zip
sudo rm metal-2026-11-13.zip

sudo yum install -y zstd
sudo pip3 install -U scipy
