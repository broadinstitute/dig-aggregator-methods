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

cd "$HOME"
sudo aws s3 cp s3://dig-analysis-bin/rust_builds/falcon-src.tar.gz ./
sudo mkdir -p falcon_src
sudo tar xzf falcon-src.tar.gz -C falcon_src

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sudo sh -s -- -y
sudo /root/.cargo/bin/cargo build --release --locked --manifest-path falcon_src/falcon-rs/Cargo.toml

sudo mkdir -p "$BIN_ROOT"
sudo cp falcon_src/falcon-rs/target/release/falcon "$BIN_ROOT/falcon"
