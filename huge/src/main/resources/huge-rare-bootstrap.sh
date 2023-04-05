#!/bin/bash -xe

echo "Yay, starting to bootstrap!"
uname -a
if pip3 list; then echo "pip3 seems happy"; else echo "pip3 has an issue: $?"; fi
sudo pip3 install -U scipy
if pip3 list; then echo "pip3 seems happy"; else echo "pip3 has an issue: $?"; fi
#sudo yum install -y python3-devel
#python3 --version
#wget https://repo.anaconda.com/miniconda/Miniconda3-py37_22.11.1-1-Linux-x86_64.sh
#bash Miniconda3-py37_22.11.1-1-Linux-x86_64.sh -b
#/home/hadoop/miniconda3/bin/conda install scipy --yes
echo "Yoohoo, done bootstrapping."
