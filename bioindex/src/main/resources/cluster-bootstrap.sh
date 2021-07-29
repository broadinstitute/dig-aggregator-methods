#!/bin/bash -xe

sudo yum install -y git

# need to upgrade pip
sudo python3 -m pip install --upgrade pip

# upgrading pip moves it to a location root doesn't have in its PATH
sudo ln -s /usr/local/bin/pip /bin/pip
sudo ln -s /usr/local/bin/pip3 /bin/pip3

# bioindex to get access to shared library functions
sudo pip3 install git+git://github.com/broadinstitute/dig-bioindex.git@master#egg=bioindex

# other python libs used by various stages
#
#Uninstall numpy so any system-wide numpy version won't conflict with the one Pandas pulls in
sudo pip3 uninstall numpy
#Fix versions to make this more deterministic
sudo pip3 install matplotlib==3.4.2
sudo pip3 install pandas==1.2.5
sudo pip3 install statsmodels==0.12.2
