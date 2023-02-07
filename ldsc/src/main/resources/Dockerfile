FROM ubuntu:18.04

RUN apt-get update && apt-get install -y python3.8 python3.8-dev python3-distutils gcc g++ curl wget unzip make libbz2-dev liblzma-dev

WORKDIR /opt
RUN curl https://bootstrap.pypa.io/get-pip.py > get-pip.py
RUN python3.8 get-pip.py

RUN pip3 install -U bitarray
RUN pip3 install -U boto3
RUN pip3 install -U sqlalchemy
RUN pip3 install -U pymysql
RUN pip3 install -U scipy
RUN pip3 install -U pandas
RUN pip3 install -U pybedtools

RUN ln -s /usr/bin/python3.8 /usr/bin/python
RUN ln -s /usr/bin/python3.8 /usr/bin/python3
RUN wget https://github.com/arq5x/bedtools2/releases/download/v2.30.0/bedtools-2.30.0.tar.gz
RUN tar -zxvf bedtools-2.30.0.tar.gz
WORKDIR /opt/bedtools2
RUN make
RUN cp bin/* /usr/local/bin/
WORKDIR /opt

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

COPY regionsToLD.py ./
