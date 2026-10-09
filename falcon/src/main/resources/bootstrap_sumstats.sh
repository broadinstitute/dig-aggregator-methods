#!/bin/bash -xe

DATA_ROOT=/mnt/var/falcon

sudo yum install -y zstd

sudo mkdir -p "$DATA_ROOT"
cd "$DATA_ROOT"

sudo aws s3 cp s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv ./snp.csv

sudo pip3 install scipy
