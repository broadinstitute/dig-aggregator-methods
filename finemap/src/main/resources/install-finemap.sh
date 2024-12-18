#!/bin/bash -xe

finemap_ROOT=/mnt/var/cojo

# install to the root directory
sudo mkdir -p "$finemap_ROOT"
cd "$finemap_ROOT"

# install yum dependencies
sudo yum install -y python3-devel


# Install conda
cd $finemap_ROOT
sudo wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
sudo bash miniconda.sh -b -p $finemap_ROOT/miniconda
echo export PATH="$finemap_ROOT/miniconda/bin:\$PATH" >> ~/.profile
. ~/.profile

# Install GCTA
cd $finemap_ROOT
sudo mkdir -p ~/software/gcta
cd ~/software/gcta
# Note that this URL may change - old versions aren't accessible at the same URL
sudo wget https://cnsgenomics.com/software/gcta/bin/gcta_1.93.2beta.zip
sudo unzip gcta_1.93.2beta.zip
cd gcta_1.93.2beta
echo export PATH="$PWD:\$PATH" >> ~/.profile
. ~/.profile

# Install plink
sudo mkdir -p ~/software/plink
cd ~/software/plink
sudo wget http://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20201019.zip
sudo unzip plink_linux_x86_64_20201019.zip
echo export PATH="$PWD:\$PATH" >> ~/.profile
. ~/.profile

# Install FINEMAP
sudo mkdir -p ~/software/finemap
cd ~/software/finemap
sudo wget http://www.christianbenner.com/finemap_v1.4_x86_64.tgz
sudo tar -zxf finemap_v1.4_x86_64.tgz
sudo ln -s finemap_v1.4_x86_64/finemap_v1.4_x86_64 finemap
sudo apt-get install libgomp1 # Not present by default it seems
echo export PATH="$PWD:\$PATH" >> ~/.profile
. ~/.profile

# Install JRE
sudo apt install -yf openjdk-8-jre-headless openjdk-8-jdk
# sudo update-java-alternatives --list
# sudo update-java-alternatives --set java-1.8.0-openjdk-amd64

# Install parallel
sudo apt install -yf parallel

echo COMPLETE


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
