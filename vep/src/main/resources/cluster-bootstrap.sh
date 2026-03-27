#!/bin/bash -xe

# install CPAN + utils
sudo yum install -y make gcc g++ perl-CPAN

# install perlbrew
curl -L https://install.perlbrew.pl | bash

# add perlbrew installed apps to path (for cpanm)
export PATH=$PATH:$HOME/perl5/perlbrew/bin

# install cpanm
perlbrew install-cpanm

# install required perl modules
cpanm --sudo --force Archive::Zip
cpanm --sudo --force DBI
cpanm --sudo --force DBD::SQLite
cpanm --sudo --force JSON
cpanm --sudo --force Set::IntervalTree
cpanm --sudo --force List::MoreUtils
cpanm --sudo --force LWP::Simple
