#!/bin/bash

RAW_ROOT=/mnt/var/raw

sudo aws s3 cp s3://dig-analysis-cfde/GTEx/bin/gtex_to_uberon.tsv ${RAW_ROOT}/
sudo aws s3 cp s3://dig-analysis-cfde/GTEx/bin/phenotype_to_mondo.tsv ${RAW_ROOT}/
