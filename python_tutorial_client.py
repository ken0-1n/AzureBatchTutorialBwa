# python_tutorial_client.py - Batch Python SDK tutorial sample
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
import datetime
import os
import sys
import time
import ConfigParser
import client_util

try:
    input = raw_input
except NameError:
    pass

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import common.helpers  # noqa

_TUTORIAL_TASK_FILE = 'python_bwa_task.py'

if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    conf = ConfigParser.SafeConfigParser()

    sample_list = []
    fastq_container = []
    fastq1_list = []
    fastq2_list = []
    samplesheet = sys.argv[1]
    output_container_name = sys.argv[2]
    conf.read(sys.argv[3])
    with open(samplesheet, 'r') as f:
        for line in f:
            line = line.rstrip("\n")
            print("line : " + line)
            F = line.split(',') 
            sample_list.append(F[0])
            fastq_container.append(F[1])
            fastq1_list.append(F[2])
            fastq2_list.append(F[3])

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_client = azureblob.BlockBlobService(
        account_name=conf.get("client","STORAGE_ACCOUNT_NAME"),
        account_key=conf.get("client","STORAGE_ACCOUNT_KEY"))

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.
    blob_client.create_container(output_container_name, fail_on_exist=False)

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batchauth.SharedKeyCredentials(
        conf.get("client","BATCH_ACCOUNT_NAME"),
        conf.get("client","BATCH_ACCOUNT_KEY"))

    batch_client = batch.BatchServiceClient(
        credentials,
        base_url=conf.get("client","BATCH_ACCOUNT_URL"))

    # Create the pool that will contain the compute nodes that will execute the
    # tasks. The resource files we pass in are used for configuring the pool's
    # start task, which is executed each time a node first joins the pool (or
    # is rebooted or re-imaged).
    task_commands = [
        # Install pip
        'curl -fSsL https://bootstrap.pypa.io/get-pip.py | python',
        # Install the azure-storage module so that the task script can access
        # Azure Blob storage, pre-cryptography version
        'pip install azure-storage==0.32.0',
        #
        'apt-get update && apt-get install -y wget bzip2 make gcc zlib1g-dev',
        # install bwa
        'wget http://sourceforge.net/projects/bio-bwa/files/bwa-0.7.15.tar.bz2',
        'tar xjvf bwa-0.7.15.tar.bz2',
        'cd bwa-0.7.15',
        'make',
        'cd ..',
        'cp -rp bwa-0.7.15 $AZ_BATCH_NODE_SHARED_DIR',
        # Copy the python_tutorial_task.py script to the "shared" directory
        'wget https://github.com/ken0-1n/AzureBatchTutorialBwa/archive/v0.1.0.tar.gz',
        'tar xzvf v0.1.0.tar.gz',
        'cp -p AzureBatchTutorialBwa-0.1.0/{} $AZ_BATCH_NODE_SHARED_DIR'.format(_TUTORIAL_TASK_FILE)
        ]
    client_util.create_pool(batch_client,
        conf.get("batch","POOL_ID"),
        conf.get("batch","NODE_OS_PUBLISHER"),
        conf.get("batch","NODE_OS_OFFER"),
        conf.get("batch","NODE_OS_SKU"),
        conf.get("batch","POOL_VM_SIZE"),
        conf.get("batch","POOL_NODE_COUNT"),
        task_commands)

    # Create the job that will run the tasks.
    client_util.create_job(batch_client,
        conf.get("batch","JOB_ID"),
        conf.get("batch","POOL_ID"))

    # Get the strage file information.
    input_container_name = 'fastq'
    input_files1_resources = [
        client_util.get_resourcefile(blob_client, fastq_container[idx], fastq1_list[idx])
        for idx in range(len(fastq1_list))]
    input_files2_resources = [
        client_util.get_resourcefile(blob_client, fastq_container[idx], fastq2_list[idx])
        for idx in range(len(fastq2_list))]

    # get reference genome
    ref_container_name = conf.get("ref", "REF_CONTAINER_NAME")
    ref_file_paths = conf.get("ref", "REF_FILE_PATH").split(",")
    ref_file_resources = [
        client_util.get_resourcefile(blob_client, ref_container_name, ref_file_path)
        for ref_file_path in ref_file_paths]

    # Add the tasks to the job. We need to supply a container shared access
    # signature (SAS) token for the tasks so that they can upload their output
    # to Azure Storage.
    upload_option = client_util.get_output_option(blob_client,
                    batch_client,
                    output_container_name,
                    conf.get("client","STORAGE_ACCOUNT_NAME"))

    tasks = list()
    for idx in range(len(input_files1_resources)):
        command = ['python $AZ_BATCH_NODE_SHARED_DIR/{} '
                   '--bwapath $AZ_BATCH_NODE_SHARED_DIR/{} '
                   '--refgenome {} --samplename {} '
                   '--fastq1 {} --fastq2 {} '.format(
                       _TUTORIAL_TASK_FILE,
                       'bwa-0.7.15/bwa',
                       ref_file_resources[0].file_path,
                       sample_list[idx],
                       input_files1_resources[idx].file_path,
                       input_files2_resources[idx].file_path)]
        command_upload = command[0] + upload_option[0]

        print('command: ' + command_upload)

        r_files = [input_files1_resources[idx], input_files2_resources[idx]]
        r_files.extend(ref_file_resources)
        
        tasks.append(batch.models.TaskAddParameter(
                'topNtask{}'.format(idx),
                common.helpers.wrap_commands_in_shell('linux', [command_upload]),
                resource_files=r_files
                )
        )
    batch_client.task.add_collection(conf.get("batch","JOB_ID"), tasks)


    # Pause execution until tasks reach Completed state.
    client_util.wait_for_tasks_to_complete(batch_client,
                               conf.get("batch","JOB_ID"),
                               datetime.timedelta(minutes=int(conf.get("batch","TIME_OUT"))))

    print("  Success! All tasks reached the 'Completed' state within the "
          "specified timeout period.")

    # Print out some timing info
    end_time = datetime.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # Clean up Batch resources (if the user so chooses).
    batch_client.job.delete(conf.get("batch","JOB_ID"))
    batch_client.pool.delete(conf.get("batch","POOL_ID"))




