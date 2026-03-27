#!/bin/bash -xe

VEP_ROOT=/mnt/var/vep

# NOTE: This is performed as a STEP instead of a bootstrap step, because AWS
#       will timeout the cluster if the bootstrap takes over 1 hour to run.

# create a vep directory in /mnt/var to copy data locally
sudo mkdir -p $VEP_ROOT
sudo chmod 775 $VEP_ROOT

# install to the VEP directory
cd $VEP_ROOT

sudo aws s3 cp s3://dig-analysis-bin/vep/vep-v115.zip .
sudo unzip vep-v115.zip
sudo rm vep-v115.zip

sudo aws s3 cp s3://dig-analysis-bin/vep/vep-homo_sapiens-115-GRCh37.zip .
sudo unzip vep-homo_sapiens-115-GRCh37.zip
sudo rm vep-homo_sapiens-115-GRCh37.zip

sudo aws s3 cp s3://dig-analysis-bin/vep/vep-dbNSFP5.3a_grch37.zip .
sudo unzip vep-dbNSFP5.3a_grch37.zip
sudo rm vep-dbNSFP5.3a_grch37.zip

sudo aws s3 cp s3://dig-analysis-bin/vep/vep-loftee-1.0.zip .
sudo unzip vep-loftee-1.0.zip
sudo rm vep-loftee-1.0.zip

sudo aws s3 cp s3://dig-analysis-bin/vep/vep-fasta-GRCh37.zip .
sudo unzip vep-fasta-GRCh37.zip
sudo rm vep-fasta-GRCh37.zip

sudo aws s3 cp s3://dig-analysis-bin/vep/vep-revel-GRCh37.zip .
sudo unzip vep-revel-GRCh37.zip
sudo rm vep-revel-GRCh37.zip

sudo aws s3 cp s3://dig-analysis-bin/vep/vep-splice_ai-GRCh37.zip .
sudo unzip vep-splice_ai-GRCh37.zip
sudo rm vep-splice_ai-GRCh37.zip
