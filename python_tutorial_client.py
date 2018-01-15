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

try:
    input = raw_input
except NameError:
    pass

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

sys.path.append('.')
sys.path.append('..')
import common.helpers  # noqa

_TUTORIAL_TASK_FILE = 'python_bwa_task.py'


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')


# custom def
def get_resourcefile(block_blob_client, container_name, blob_name):
    """
    get Target Resource File.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    sas_url = block_blob_client.make_blob_url(container_name,
                                              blob_name,
                                              sas_token=sas_token)

    return batchmodels.ResourceFile(file_path=blob_name,
                                    blob_source=sas_url)


def create_pool(batch_service_client, pool_id,
    publisher, offer, sku, pool_vm_size, pool_node_count, task_commands):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param list resource_files: A collection of resource files for the pool's
    start task.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print('Creating pool [{}]...'.format(pool_id))

    # Get the node agent SKU and image reference for the virtual machine
    # configuration.
    # For more information about the virtual machine configuration, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    sku_to_use, image_ref_to_use = \
        common.helpers.select_latest_verified_vm_image_with_node_agent_sku(
            batch_service_client, publisher, offer, sku)
    user = batchmodels.AutoUserSpecification(
        scope=batchmodels.AutoUserScope.pool,
        elevation_level=batchmodels.ElevationLevel.admin)
    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            node_agent_sku_id=sku_to_use),
        vm_size=pool_vm_size,
        target_dedicated_nodes=pool_node_count,
        start_task=batch.models.StartTask(
            command_line=common.helpers.wrap_commands_in_shell('linux',
                                                               task_commands),
            user_identity=batchmodels.UserIdentity(auto_user=user),
            wait_for_success=True)
    )

    try:
        batch_service_client.pool.add(new_pool)
    except batchmodels.batch_error.BatchErrorException as err:
        print_batch_exception(err)
        raise


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        job_id,
        batch.models.PoolInformation(pool_id=pool_id))

    try:
        batch_service_client.job.add(job)
    except batchmodels.batch_error.BatchErrorException as err:
        print_batch_exception(err)
        raise


def get_output_option(batch_service_client,
              output_container_name,strage_account_name):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_name: The ID of an Azure Blob storage container to
    which the tasks will upload their results.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """

    # Obtain a shared access signature that provides write access to the output
    # container to which the tasks will upload their output.
    output_container_sas_token = \
        blob_client.generate_container_shared_access_signature(
            output_container_name,
            permission=azureblob.BlobPermissions.WRITE,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    upload_command = ['--storageaccount {} '
                      '--storagecontainer {} '
                      '--sastoken "{}" '.format(
                      strage_account_name,
                      output_container_name,
                      output_container_sas_token)]
  
    return upload_command


def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


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
    create_pool(batch_client,
        conf.get("batch","POOL_ID"),
        conf.get("batch","NODE_OS_PUBLISHER"),
        conf.get("batch","NODE_OS_OFFER"),
        conf.get("batch","NODE_OS_SKU"),
        conf.get("batch","POOL_VM_SIZE"),
        conf.get("batch","POOL_NODE_COUNT"),
        task_commands)

    # Create the job that will run the tasks.
    create_job(batch_client,
        conf.get("batch","JOB_ID"),
        conf.get("batch","POOL_ID"))

    # Get the strage file information.
    input_container_name = 'fastq'
    input_files1_resources = [
        get_resourcefile(blob_client, fastq_container[idx], fastq1_list[idx])
        for idx in range(len(fastq1_list))]
    input_files2_resources = [
        get_resourcefile(blob_client, fastq_container[idx], fastq2_list[idx])
        for idx in range(len(fastq2_list))]

    # get reference genome
    ref_container_name = conf.get("ref", "REF_CONTAINER_NAME")
    ref_file_paths = conf.get("ref", "REF_FILE_PATH").split(",")
    ref_file_resources = [
        get_resourcefile(blob_client, ref_container_name, ref_file_path)
        for ref_file_path in ref_file_paths]

    # Add the tasks to the job. We need to supply a container shared access
    # signature (SAS) token for the tasks so that they can upload their output
    # to Azure Storage.
    upload_option = get_output_option(batch_client,
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
    wait_for_tasks_to_complete(batch_client,
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
