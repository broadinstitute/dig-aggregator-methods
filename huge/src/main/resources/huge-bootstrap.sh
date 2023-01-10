#!/bin/bash -xe

echo "Yay, starting to bootstrap!"
uname -a
sudo yum install -y python3-devel
python3 --version
wget https://repo.anaconda.com/miniconda/Miniconda3-py37_22.11.1-1-Linux-x86_64.sh
bash Miniconda3-py37_22.11.1-1-Linux-x86_64.sh -b
/home/hadoop/miniconda3/bin/conda install scipy
echo "Yoohoo, done bootstrapping."
