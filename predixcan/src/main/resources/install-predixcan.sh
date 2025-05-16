#!/bin/bash -xe

# susie method
## Developed with python 3 and R

predixcan_ROOT=/mnt/var/predixcan

# install to the root directory
sudo mkdir -p "$predixcan_ROOT"
cd "$predixcan_ROOT"

# install yum dependencies
sudo yum install -y python3-devel
sudo yum update -y
sudo yum install -y jq
sudo yum install -y zstd

# Install parallel
# sudo yum -y install epel-release
sudo amazon-linux-extras install epel -y
sudo yum install -y parallel

# pull down model files
sudo mkdir -p ./model_files
sudo aws s3 cp s3://dig-analysis-bin/predixcan/model_db_cov_files/ ./model_files/ --recursive

# fetch snps for mapping
sudo aws s3 cp "s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv" ./snps.csv

# install python dependencies
pip3 install -U absl-py
pip3 install -U astunparse
pip3 install -U certifi
pip3 install -U charset-normalizer
pip3 install -U contourpy
pip3 install -U cycler
pip3 install -U filelock
pip3 install -U flatbuffers
pip3 install -U fonttools
pip3 install -U fsspec
pip3 install -U gast
pip3 install -U google-pasta
pip3 install -U grpcio
pip3 install -U h5py
pip3 install -U idna
pip3 install -U jinja2
pip3 install -U joblib
pip3 install -U keras
pip3 install -U kiwisolver
pip3 install -U libclang
pip3 install -U markdown
pip3 install -U markdown-it-py
pip3 install -U markupsafe
pip3 install -U matplotlib
pip3 install -U mdurl
pip3 install -U ml-dtypes
pip3 install -U mpmath
pip3 install -U namex
pip3 install -U networkx
pip3 install -U nextflow
pip3 install -U numpy

# pip3 install -U nvidia-cublas-cu12
# pip3 install -U nvidia-cuda-cupti-cu12
# pip3 install -U nvidia-cuda-nvrtc-cu12
# pip3 install -U nvidia-cuda-runtime-cu12
# pip3 install -U nvidia-cudnn-cu12
# pip3 install -U nvidia-cufft-cu12
# pip3 install -U nvidia-curand-cu12
# pip3 install -U nvidia-cusolver-cu12
# pip3 install -U nvidia-cusparse-cu12
# pip3 install -U nvidia-nccl-cu12
# pip3 install -U nvidia-nvjitlink-cu12
# pip3 install -U nvidia-nvtx-cu12

pip3 install -U opt-einsum
pip3 install -U optree
pip3 install -U pandas
pip3 install -U pillow
pip3 install -U protobuf
pip3 install -U pyparsing
pip3 install -U pytz
pip3 install -U requests
pip3 install -U rich
pip3 install -U scikit-learn
pip3 install -U scipy
pip3 install -U sympy
pip3 install -U tensorboard
pip3 install -U tensorboard-data-server
pip3 install -U tensorflow
pip3 install -U tensorflow-io-gcs-filesystem
pip3 install -U termcolor
pip3 install -U threadpoolctl
pip3 install -U torch
pip3 install -U triton
pip3 install -U tzdata
pip3 install -U urllib3
pip3 install -U werkzeug
pip3 install -U wrapt



sudo wget https://github.com/hakyimlab/MetaXcan/archive/refs/heads/master.zip -O MetaXcan-master.zip
sudo unzip MetaXcan-master.zip

echo COMPLETE

echo "Setup completed successfully. The 'finemap' environment is ready to use."  
