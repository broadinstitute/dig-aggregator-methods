#!/bin/bash -xe

DATA_ROOT=/mnt/var/falcon/ref
BIN_ROOT=/mnt/var/falcon

sudo yum install -y zstd

sudo mkdir -p "$DATA_ROOT"
cd "$DATA_ROOT"

sudo aws s3 cp s3://dig-analysis-bin/falcon/falcon.ini ./

sudo aws s3 cp s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv ./snp.csv

sudo aws s3 cp s3://dig-analysis-bin/falcon/genes.zip ./
sudo unzip genes.zip -d ./genes
sudo rm genes.zip

sudo aws s3 cp s3://dig-analysis-bin/falcon/LD.zip ./
sudo unzip LD.zip -d ./LD
sudo rm LD.zip

sudo aws s3 cp s3://dig-analysis-bin/falcon/V2G.zip ./
sudo unzip V2G.zip -d ./V2G
sudo rm V2G.zip

sudo aws s3 cp s3://dig-analysis-bin/falcon/annotations.zip ./
sudo unzip annotations.zip -d ./annotations
sudo rm annotations.zip

sudo aws s3 cp s3://dig-analysis-bin/falcon/dummy.coeff.tsv ./coeff/

cd "$BIN_ROOT"
# Generated from (in the falcon repo):
#cd falcon && tar czf falcon-src.tar.gz --exclude=target --exclude=.git .

sudo aws s3 cp s3://dig-analysis-bin/falcon/falcon-src.tar.gz ./
sudo mkdir -p falcon_src
sudo tar xzf falcon-src.tar.gz -C falcon_src

#curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sudo sh -s -- -y
#sudo /root/.cargo/bin/cargo build --release --locked --manifest-path falcon_src/falcon-rs/Cargo.toml
#
#sudo mkdir -p "$BIN_ROOT"
#sudo cp falcon_src/falcon-rs/target/release/falcon "$BIN_ROOT/falcon"

sudo pip3.11 install numba
sudo pip3.11 install tabulate
sudo pip3.11 install plotly
sudo pip3.11 install pandas
sudo pip3.11 install scikit-learn

sudo pip3 install scipy
