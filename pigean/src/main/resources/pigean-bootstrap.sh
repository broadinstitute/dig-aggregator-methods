#!/bin/bash -xe

PIGEAN_ROOT=/mnt/var/pigean

# create a directory in /mnt/var to copy data locally
sudo mkdir -p "${PIGEAN_ROOT}"
sudo chmod 775 "${PIGEAN_ROOT}"

# install to the metal directory
cd "${PIGEAN_ROOT}"

sudo aws s3 cp s3://dig-analysis-bin/pigean/NCBI37.3.plink.gene.exons.loc .
sudo aws s3 cp s3://dig-analysis-bin/pigean/NCBI37.3.plink.gene.loc .
sudo aws s3 cp s3://dig-analysis-bin/pigean/gencode.gene.map .
sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_set_list_mouse.txt .
sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_set_list_msigdb_nohp.txt .
sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_set_list_string_notext_medium_processed.txt .
sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_set_list_pops_sparse_small.txt .
sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_set_list_mesh_processed.txt .
sudo aws s3 cp s3://dig-analysis-bin/pigean/priors.py .
sudo aws s3 cp s3://dig-analysis-bin/pigean/refGene_hg19_TSS.subset.loc .

# install dependencies
sudo yum install -y python3-devel
sudo pip3 install -U Cython
sudo pip3 install -U pybind11
sudo pip3 install -U pythran
sudo pip3 install -U scipy
sudo pip3 install -U requests
