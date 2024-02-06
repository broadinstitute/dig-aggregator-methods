#!/bin/bash -xe

sudo yum groups mark convert

# check if GCC, make, etc. are installed already
DEVTOOLS=$(sudo yum grouplist | grep 'Development Tools')

# VEP requires GCC, make
if [ -z "$DEVTOOLS" ]; then
    sudo yum groupinstall -y 'Development Tools'
fi

# install CPAN + utils
sudo yum install -y perl-CPAN
sudo yum install -y perl-DBD-MySQL
sudo yum install -y htop
sudo yum install -y curl

# install perlbrew
curl -L https://install.perlbrew.pl | bash

# add perlbrew installed apps to path (for cpanm)
export PATH=$PATH:$HOME/perl5/perlbrew/bin

# install cpanm
perlbrew install-cpanm

# install required perl modules
cpanm --sudo --force Archive::Zip
cpanm --sudo --force DBI
cpanm --sudo --force JSON
cpanm --sudo --force PerlIO::gzip
cpanm --sudo --force Try::Tiny
cpanm --sudo --force autodie
cpanm --sudo --force Set::IntervalTree
cpanm --sudo --force ExtUtils::Command::MM
cpanm --sudo --force Bio::RNA::SpliceSites::Scoring::MaxEntScan
