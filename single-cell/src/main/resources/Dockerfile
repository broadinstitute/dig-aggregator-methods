FROM public.ecr.aws/amazonlinux/amazonlinux:2023.10.20260120.4

WORKDIR opt/

RUN yum groupinstall -y "Development Tools"
RUN yum install -y python3-devel libcurl-devel gmp-devel R wget

RUN wget https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.4.3/hdf5-1.14.4-3.tar.gz
RUN tar zxvf hdf5-1.14.4-3.tar.gz
WORKDIR hdf5-1.14.4-3
RUN ./configure -prefix=/usr
RUN make -j -l6
RUN make install
WORKDIR ../
RUN rm hdf5-1.14.4-3.tar.gz
RUN rm -r hdf5-1.14.4-3

# I think I need to do some platform shenanigans to get this container to run in Amazon
# But for local development this should work (changed from x86_64 to aarch64)
RUN wget https://github.com/Kitware/CMake/releases/download/v3.24.0/cmake-3.24.0-linux-aarch64.tar.gz
RUN tar -zxvf cmake-3.24.0-linux-aarch64.tar.gz
RUN cp -r cmake-3.24.0-linux-aarch64/* /usr/
RUN rm cmake-3.24.0-linux-aarch64.tar.gz
RUN rm -r cmake-3.24.0-linux-aarch64

RUN R -e "install.packages('BiocManager', repos='http://cran.rstudio.com/')"
RUN R -e "BiocManager::install('DelayedArray')"
RUN R -e "BiocManager::install('HDF5Array')"
RUN R -e "install.packages('http://cran.r-project.org/src/contrib/Archive/Matrix/Matrix_1.6-5.tar.gz', repos=NULL, type='source')"
RUN R -e "install.packages('dplyr', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('purrr', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('clue', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('proxy', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('reticulate', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('anndata', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('ggplot2', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('http://cran.r-project.org/src/contrib/Archive/ggrepel/ggrepel_0.9.0.tar.gz', repos=NULL, type='source')"
RUN R -e "install.packages('Seurat', repos='http://cran.rstudio.com/')" # 5.4.0 when generated
RUN R -e "install.packages('rliger', repos='http://cran.rstudio.com/')" # 2.2.1 when generated

COPY inmf_liger_mod.R .
