#!/bin/bash -xe

# susie method
## Developed with python 3 and R

finemap_ROOT=/mnt/var/cojo

# install to the root directory
sudo mkdir -p "$finemap_ROOT"
cd "$finemap_ROOT"

# install yum dependencies
sudo yum install -y python3-devel
sudo yum update -y
sudo yum install -y jq
sudo yum install -y zstd

# Install parallel
sudo yum install -y python3 git epel-release
sudo yum install -y parallel

# pull down LD bfiles
sudo mkdir -p ./bfiles
sudo aws s3 cp s3://dig-analysis-bin/cojo/bfiles/ ./bfiles/ --recursive

# pull down finemap dir
sudo mkdir -p ./finemapping
sudo aws s3 cp s3://dig-analysis-bin/cojo/finemapping/ ./finemapping/ --recursive

sudo chmod 777 ./finemapping/combine_results.sh
sudo chmod 777 ./finemapping/run_finemap_pipeline.sh

# fetch snps for mapping
sudo aws s3 cp "s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv" ./snps.csv

# install python dependencies
pip3 install -U pandas
pip3 install -U dask
pip3 install -U gcsfs
pip3 install -U fastparquet
pip3 install -U pyarrow
pip3 install -U pyyaml
pip3 install -U scipy
pip3 install -U numpy
pip3 install -U python-snappy
pip3 install -U pyspark
pip3 install -U jq

# Install GCTA
cd $finemap_ROOT
sudo mkdir -p ~/software/gcta
cd ~/software/gcta


sudo wget https://yanglab.westlake.edu.cn/software/gcta/bin/gcta-1.94.3-linux-kernel-3-x86_64.zip
sudo unzip gcta-1.94.3-linux-kernel-3-x86_64.zip
cd gcta-1.94.3-linux-kernel-3-x86_64
sudo chown -R hadoop:hadoop ~/software/gcta/gcta-1.94.3-linux-kernel-3-x86_64
chmod +x ~/software/gcta/gcta-1.94.3-linux-kernel-3-x86_64/gcta64
echo export PATH="$PWD:\$PATH" >> ~/.profile
. ~/.profile

# Install plink
sudo mkdir -p ~/software/plink
cd ~/software/plink
sudo wget http://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20201019.zip
sudo unzip plink_linux_x86_64_20201019.zip
sudo chown -R hadoop:hadoop ~/software/plink
sudo chmod +x ~/software/plink/plink
echo export PATH="$PWD:\$PATH" >> ~/.profile
. ~/.profile

# Install FINEMAP
sudo mkdir -p ~/software/finemap
cd ~/software/finemap
sudo wget http://www.christianbenner.com/finemap_v1.4_x86_64.tgz
sudo tar -zxf finemap_v1.4_x86_64.tgz
sudo ln -s finemap_v1.4_x86_64/finemap_v1.4_x86_64 finemap
sudo chown -R hadoop:hadoop ~/software/finemap/finemap_v1.4_x86_64
chmod +x ~/software/finemap/finemap_v1.4_x86_64/finemap_v1.4_x86_64
sudo chmod +x ~/software/finemap/finemap
sudo yum install -y libgomp # Not present by default it seems
echo export PATH="$PWD:\$PATH" >> ~/.profile
. ~/.profile

# Install JRE
# sudo yum install -y openjdk-8-jre-headless openjdk-8-jdk
sudo yum install -y java-1.8.0-openjdk-devel
# sudo update-java-alternatives --list
# sudo update-java-alternatives --set java-1.8.0-openjdk-amd64

# Activate software path
echo "$(cat ~/.profile)"
source ~/.profile 

echo COMPLETE

echo "Setup completed successfully. The 'finemap' environment is ready to use."  
