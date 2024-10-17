#!/bin/bash

RAW_ROOT=/mnt/var/raw

sudo aws s3 cp s3://dig-analysis-cfde/KidsFirst/bin/tissue_map.tsv ${RAW_ROOT}/
sudo aws s3 cp s3://dig-analysis-cfde/KidsFirst/bin/phenotype_map.tsv ${RAW_ROOT}/
sudo aws s3 cp s3://dig-analysis-cfde/catalog/out/pigean/gene_set_count/ ${RAW_ROOT}/gene_set_count/ --recursive
