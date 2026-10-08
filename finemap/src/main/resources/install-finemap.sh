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

# install parallel
if ! sudo yum install -y parallel; then
    echo "Parallel not found in repos, installing manually from GNU source..."
    wget https://ftpmirror.gnu.org/parallel/parallel-latest.tar.bz2
    tar -xjf parallel-latest.tar.bz2
    cd parallel-*
    ./configure && make && sudo make install
    cd "$finemap_ROOT"
fi

# pull down LD bfiles
sudo mkdir -p ./bfiles
sudo aws s3 cp s3://dig-analysis-bin/cojo/bfiles/ ./bfiles/ --recursive

# pull down finemap dir
sudo mkdir -p ./finemapping
sudo aws s3 cp s3://dig-analysis-bin/cojo/finemapping/ ./finemapping/ --recursive

sudo chmod 777 ./finemapping/combine_results.sh
sudo chmod 777 ./finemapping/run_finemap_pipeline.sh

# Single source of truth for binary versions (shared with gcta.py via
# utils.load_binary_versions()) - keeps this script and the pipeline code
# from ever installing/expecting different GCTA versions again.
source ./finemapping/versions.env

# fetch snps for mapping
sudo aws s3 cp "s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv" ./snps.csv

# install python dependencies
#
# Pinned to exact versions from versions.env instead of always installing
# latest (`-U` with no pin) - previously, every new cluster could silently
# get a different pandas/dask/pyarrow/etc. than the last one, with no
# record of which version actually produced a given result.
#
# gcsfs, fastparquet, python-snappy, and pyspark are intentionally left
# unpinned/on latest here - they're not used anywhere in the current
# finemapping/ codebase as of this audit, so no verified version to pin
# them to. Flagged in PROJECT_NOTES.md as possibly-dead dependencies worth
# removing rather than chasing a pin for.
pip3 install "pandas==$PANDAS_VERSION"
pip3 install "dask[dataframe]==$DASK_VERSION"
pip3 install -U gcsfs
pip3 install -U fastparquet
pip3 install "pyarrow==$PYARROW_VERSION"
pip3 install "pyyaml==$PYYAML_VERSION"
pip3 install "scipy==$SCIPY_VERSION"
pip3 install "numpy==$NUMPY_VERSION"
pip3 install -U python-snappy
pip3 install -U pyspark
# pip3 install -U jq removed (2026-07-30) - this is the PyPI Python-binding
# package, not the yum-installed jq CLI (line ~15 above) that every shell
# pipeline in this codebase actually calls. Confirmed no .py file anywhere
# in crosshair/ or finemap/ imports it - genuinely dead, same pattern as
# gcsfs/fastparquet/python-snappy/pyspark above. Installing it via pip
# --user (default for a non-root pip3 install) risked shadowing the real
# jq CLI on PATH order alone, a plausible explanation for a run failing on
# a live EMR node while an identical local reproduction of the same
# zstdcat|jq pipeline against the same real data succeeded cleanly.

# Install GCTA
cd "$finemap_ROOT"
sudo mkdir -p ~/software/gcta
cd ~/software/gcta


sudo wget "$GCTA_URL"
sudo unzip "$(basename "$GCTA_URL")"
cd "$GCTA_DIRNAME"
sudo chown -R hadoop:hadoop ~/software/gcta/"$GCTA_DIRNAME"
chmod +x ~/software/gcta/"$GCTA_DIRNAME"
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
#sudo yum install -y java-1.8.0-openjdk-devel
# sudo update-java-alternatives --list
# sudo update-java-alternatives --set java-1.8.0-openjdk-amd64

# Activate software path
echo "$(cat ~/.profile)"
source ~/.profile

echo COMPLETE

echo "Setup completed successfully. The 'finemap' environment is ready to use."
