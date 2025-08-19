#!/bin/bash -xe

# Bootstrap scripts can either be run as a...
#
#  bootstrapScript
#  bootstrapStep
#
# A bootstrap script is run while the machine is being provisioned by
# AWS, are run as a different user, and must complete within 60 minutes
# or the provisioning fails. This can be a good thing, as it prevents
# accidentally creating scripts that never terminate (e.g. waiting for
# user input).
#
# A bootstrap step is a "step" like any other job step. It can take as
# long as needed. It is run as the hadoop user and is run in the step's
# directory (e.g. /mnt/var/lib/hadoop/steps/s-123456789).
#
# Most of the time, it's best to user a bootstrap script and not step.

#sudo yum groups mark convert
#
## check if GCC, make, etc. are installed already
#DEVTOOLS=$(sudo yum grouplist | grep 'Development Tools')
#
#if [ -z "$DEVTOOLS" ]; then
#    sudo yum groupinstall -y 'Development Tools'
#fi

WORK_DIR="/mnt/var/claussnitzerlab"
mkdir -p "${WORK_DIR}"

# install the python libraries
sudo pip3 install torch==1.5.1
sudo pip3 install twobitreader
sudo pip3 install numpy
sudo pip3 install sklearn

# copy the basset python files and the model weights file
aws s3 cp s3://dig-analysis-data/bin/regionpytorch/nasa_labels.txt "${WORK_DIR}"
aws s3 cp s3://dig-analysis-data/bin/regionpytorch/hg19.2bit "${WORK_DIR}"
aws s3 cp s3://dig-analysis-data/bin/regionpytorch/nasa_ampt2d_cnn_900_best_p041.pth "${WORK_DIR}"