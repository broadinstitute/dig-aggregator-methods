#!/bin/bash -xe

ROOT_DIR=/mnt/var/single_cell

sudo mkdir -p "$ROOT_DIR"
cd "$ROOT_DIR"

# install yum dependencies
sudo yum groupinstall -y "Development Tools"
sudo yum install -y python3-devel libcurl-devel gmp-devel R

# maybe
sudo wget https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.4.3/hdf5-1.14.4-3.tar.gz
sudo tar zxvf hdf5-1.14.4-3.tar.gz
cd hdf5-1.14.4-3
sudo ./configure -prefix=/usr
sudo make -j -l6
sudo make install
cd ../
sudo rm hdf5-1.14.4-3.tar.gz
sudo rm -r hdf5-1.14.4-3

# also maybe
sudo wget https://github.com/Kitware/CMake/releases/download/v3.24.0/cmake-3.24.0-linux-x86_64.tar.gz
sudo tar -zxvf cmake-3.24.0-linux-x86_64.tar.gz
sudo cp -r cmake-3.24.0-linux-x86_64/* /usr/
sudo rm cmake-3.24.0-linux-x86_64.tar.gz
sudo rm -r cmake-3.24.0-linux-x86_64

# install R dependencies
sudo R -e "install.packages('BiocManager', repos='http://cran.rstudio.com/')"
sudo R -e "BiocManager::install('DelayedArray')"
sudo R -e "BiocManager::install('HDF5Array')"
sudo R -e "install.packages('http://cran.r-project.org/src/contrib/Archive/Matrix/Matrix_1.6-5.tar.gz', repos=NULL, type='source')"
sudo R -e "install.packages('dplyr', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('purrr', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('clue', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('proxy', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('reticulate', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('anndata', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('http://cran.r-project.org/src/contrib/Archive/ggrepel/ggrepel_0.9.0.tar.gz', repos=NULL, type='source')"
sudo R -e "install.packages('Seurat', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('rliger', repos='http://cran.rstudio.com/')"

sudo aws s3 cp s3://dig-analysis-bin/single_cell/inmf_liger_mod.R ./
