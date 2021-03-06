# python_tutorial_task.py - Batch Python SDK tutorial sample
#
# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import print_function
import argparse
import collections
import os
import string
import subprocess
import sys

import azure.storage.blob as azureblob

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--storageaccount')
    parser.add_argument('--storagecontainer')
    parser.add_argument('--sastoken')
    parser.add_argument('--bwapath')
    parser.add_argument('--fastq1')
    parser.add_argument('--fastq2')
    parser.add_argument('--refgenome')
    parser.add_argument('--samplename')
    args = parser.parse_args()

    bwa = os.path.realpath(args.bwapath)
    fastq1 = os.path.realpath('/mnt/' + args.fastq1)
    fastq2 = os.path.realpath('/mnt/' + args.fastq2)
    ref_fa = os.path.realpath('/mnt/' + args.refgenome)
    samplename = args.samplename

    output_sam = samplename +'.sam'
    error_log = samplename +'.error.log'

    sam =  open(output_sam, 'w')
    error =  open(error_log, 'w')
    proc = subprocess.Popen([bwa, 'mem', ref_fa, fastq1, fastq2], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # proc = subprocess.Popen(['ls','-l',ref_fa+'.amb'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = proc.stdout
    for line in output:
        sam.write(line)
    output = proc.stderr
    for line in output:
        error.write(line)

    sam.close()
    error.close()

    output_sam_path = os.path.realpath(output_sam)

    blob_client = azureblob.BlockBlobService(account_name=args.storageaccount,
                                             sas_token=args.sastoken)

    print('Uploading file {} to container [{}]...'.format(
        output_sam_path,
        args.storagecontainer))

    blob_client.create_blob_from_path(args.storagecontainer,
                                      output_sam,
                                      output_sam_path)

    error_log_path = os.path.realpath(error_log)

    print('Uploading file {} to container [{}]...'.format(
        error_log_path,
        args.storagecontainer))

    blob_client.create_blob_from_path(args.storagecontainer,
                                      error_log,
                                      error_log_path)

