#!/bin/bash -xe

sudo pip3 install matplotlib
sudo pip3 install pandas
sudo pip3 install statsmodels

# copy the getmerge script
aws s3 cp s3://dig-analysis-data/resources/BioIndex/plots/getmerge-strip-headers.sh /home/hadoop

# make it executable
chmod +x /home/hadoop/getmerge-strip-headers.sh
