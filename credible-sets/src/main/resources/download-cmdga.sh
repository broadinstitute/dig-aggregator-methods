#!/bin/bash -xe

C2CT_ROOT=/mnt/var/c2ct

# create a clumping directory in /mnt/var to copy data locally
sudo mkdir -p "${C2CT_ROOT}"
sudo chmod 775 "${C2CT_ROOT}"

# install to the metal directory
cd "${C2CT_ROOT}"

sudo aws s3 cp s3://dig-igvf-lipids/out/ldsc/regions/merged/annotation-tissue-biosample/ ./annotation-tissue-biosample/ --recursive
