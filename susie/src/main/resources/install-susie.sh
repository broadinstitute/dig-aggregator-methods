#!/bin/bash -xe

# susie method
## Developed with python 3 and R

SuSiE_ROOT=/mnt/var/susie

# install to the root directory
sudo mkdir -p "$SuSiE_ROOT"
cd "$SuSiE_ROOT"

# install yum dependencies
sudo yum install -y python3-devel
sudo yum install -y R

# Install R-4.1.0
# sudo wget https://cdn.rstudio.com/r/centos-7/pkgs/R-4.1.0-1-1.x86_64.rpm
# sudo yum install -y R-4.1.0-1-1.x86_64.rpm
# sudo rm R-4.1.0-1-1.x86_64.rpm

# # find R directory 
# for cmd in $(ls /usr/bin); do
#     if echo "$cmd" | grep -qi "R"; then
#         whereis $cmd
#     fi
# done


# install R dependencies
sudo R -e "install.packages('dplyr', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('tidyr', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('base', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('stats', repos='http://cran.rstudio.com/')"
# sudo R -e "install.packages('https://cran.r-project.org/src/contrib/Archive/coloc/coloc_5.1.0.tar.gz', repos = NULL, type = 'source')"
sudo R -e "install.packages('coloc', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('sjmisc', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('susieR', repos='http://cran.rstudio.com/')"
# sudo R -e "install.packages('https://cran.hafro.is/contrib/main/00Archive/susieR/susieR_0.11.42.tar.gz', repos = NULL, type = 'source')"
sudo R -e "install.packages('stringr', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('Matrix', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('jsonlite', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('data.table', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('parallel', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('strengejacke', repos='http://cran.rstudio.com/')"
sudo R -e "install.packages('http://www.well.ox.ac.uk/~gav/resources/rbgen_v1.1.5.tgz', repos = NULL, type = 'source')"

# install python dependencies
pip3 install -U pandas
pip3 install -U numpy
pip3 install -U fsspec

# pull down LD bfiles
sudo mkdir -p ./1000G_EUR_plink
sudo aws s3 cp s3://dig-analysis-bin/susie/1000G_EUR_plink/ ./1000G_EUR_plink/ --recursive

# fetch snps for mapping
sudo aws s3 cp "s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv" ./snps.csv

sudo aws s3 cp s3://dig-analysis-bin/susie/SuSiE.r ./
sudo aws s3 cp s3://dig-analysis-bin/susie/plink ./
sudo aws s3 cp s3://dig-analysis-bin/susie/plink_ld_snp_list.sh ./
sudo chmod 777 ./plink_ld_snp_list.sh
sudo chmod 777 ./plink