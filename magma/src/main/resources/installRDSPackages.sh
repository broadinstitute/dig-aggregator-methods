#!/bin/bash -xe

# install needed python
sudo yum install -y python3-devel
sudo pip3 install -U boto3
sudo pip3 install -U sqlalchemy
sudo pip3 install -U pymysql
