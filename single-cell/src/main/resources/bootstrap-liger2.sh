#!/bin/bash -xe

ROOT_DIR=/mnt/var/single_cell

cd "$ROOT_DIR"

sudo R -e "install.packages('Seurat', repos='http://cran.rstudio.com/')" # 5.4.0 when generated
sudo R -e "install.packages('rliger', repos='http://cran.rstudio.com/')" # 2.2.1 when generated
