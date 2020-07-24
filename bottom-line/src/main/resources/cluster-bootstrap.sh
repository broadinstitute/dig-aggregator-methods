#!/bin/bash -xe

METAL_ROOT=/mnt/var/metal

# create a gregor directory in /mnt/var to copy data locally
mkdir -p "${METAL_ROOT}"
chmod 775 "${METAL_ROOT}"

# install to the metal directory
cd "${METAL_ROOT}"

# make sure a bin folder exists for the hadoop user
mkdir -p /home/hadoop/bin
mkdir -p /home/hadoop/scripts

# get a pre-built version of metal from S3
aws s3 cp s3://dig-analysis-data/bin/metal/metal-2018-08-28.tgz /home/hadoop
tar zxf /home/hadoop/metal-2018-08-28.tgz -C /home/hadoop

# install it to the bin folder for use and make it executable
ln -s /home/hadoop/metal/build/metal/metal /home/hadoop/bin/metal

# copy the getmerge-strip-headers shell script from S3
aws s3 cp "s3://dig-analysis-data/resources/scripts/getmerge-strip-headers.sh" /home/hadoop/bin
chmod +x /home/hadoop/bin/getmerge-strip-headers.sh

# copy the runMETAL script from S3
aws s3 cp "s3://dig-analysis-data/resources/pipeline/metaanalysis/runMETAL.sh" /home/hadoop/bin
chmod +x /home/hadoop/bin/runMETAL.sh

# install jq to convert from json to tsv
sudo yum install -y jq
