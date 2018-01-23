
FROM ubuntu:16.04

MAINTAINER Kenichi Chiba <kchiba@hgc.jp>

RUN apt-get update && \
    apt-get install -y \
            git \
            wget \
            bzip2 \
            make \
            gcc \
            curl \
            python \
            python-pip \
            zlib1g-dev

# bwa
RUN wget http://sourceforge.net/projects/bio-bwa/files/bwa-0.7.15.tar.bz2 && \
    tar xjvf bwa-0.7.15.tar.bz2 && \
    cd bwa-0.7.15 && \
    make && \
    cd .. && \
    mv bwa-0.7.15 /bin

# upload results to storage
RUN pip install azure-storage==0.32.0

# install the main script
RUN git clone https://github.com/ken0-1n/AzureBatchTutorialBwa.git && \
    cp AzureBatchTutorialBwa/python_bwa_task.py /bin && \
    chmod 755 /bin/python_bwa_task.py

