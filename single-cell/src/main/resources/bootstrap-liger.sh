#!/bin/bash -xe

ROOT_DIR=/mnt/var/single_cell

sudo mkdir -p "$ROOT_DIR"
cd "$ROOT_DIR"

# install yum dependencies
sudo yum install -y python3-devel
sudo yum install -y R


# install R dependencies
sudo R -e "install.packages('rliger', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('Matrix', repos='http://cran.rstudio.com/', version='1.6-5')"
sudo R -e "install.packages('dplyr', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('purrr', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('clue', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('proxy', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('Seurat', repos='http://cran.rstudio.com/')"

sudo aws s3 cp s3://dig-analysis-bin/single_cell/inmf_liger.R ./
sudo chmod 777 ./inmf_liger.R
