#!/bin/bash -xe

METAL_ROOT=/mnt/var/metal

# create a metal directory in /mnt/var to copy data locally
sudo mkdir -p "${METAL_ROOT}"
sudo chmod 775 "${METAL_ROOT}"

# install to the metal directory
cd "${METAL_ROOT}"

# make sure a bin folder exists for the hadoop user
sudo mkdir -p /home/hadoop/bin
sudo mkdir -p /home/hadoop/scripts

# get a pre-built version of metal from S3
sudo aws s3 cp s3://dig-analysis-bin/metaanalysis/metal-2022-09-16.tgz /home/hadoop
sudo tar zxf /home/hadoop/metal-2022-09-16.tgz -C /home/hadoop

# install it to the bin folder for use and make it executable
sudo ln -s /home/hadoop/metal/build/metal/metal /home/hadoop/bin/metal

# build MR-MEGA
sudo mkdir -p /home/hadoop/bin/MR-MEGA
cd /home/hadoop/bin/MR-MEGA
sudo aws s3 cp s3://dig-analysis-bin/metaanalysis/MR-MEGA_v0.2.zip .
sudo unzip MR-MEGA_v0.2.zip
sudo make
cd "${METAL_ROOT}"

# copy the getmerge-strip-headers shell script from S3
sudo aws s3 cp "s3://dig-analysis-bin/metaanalysis/getmerge-strip-headers.sh" /home/hadoop/bin
sudo chmod +x /home/hadoop/bin/getmerge-strip-headers.sh

# copy the runMETAL script from S3
sudo aws s3 cp "s3://dig-analysis-bin/metaanalysis/runMETAL.sh" /home/hadoop/bin
sudo chmod +x /home/hadoop/bin/runMETAL.sh

# copy the runMRMEGA script from S3
sudo aws s3 cp "s3://dig-analysis-bin/metaanalysis/runMRMEGA.sh" /home/hadoop/bin
sudo chmod +x /home/hadoop/bin/runMRMEGA.sh

# install jq to convert from json to tsv
sudo yum install -y jq
sudo yum install -y zstd
