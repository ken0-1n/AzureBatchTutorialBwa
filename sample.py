import os
import sys

import azure.storage.blob as azureblob

if __name__ == '__main__':

    hogehoge = sys.argv[1]
    san = sys.argv[2]
    sat = sys.argv[3]
    testlog = open("test.log", 'w')
    testlog.write(hogehoge)
    testlog.close()

    container = "output"
    testlog_path = os.path.realpath("test.log")

    print('Uploading file {} to container [{}]...'.format(
        testlog_path,
        container))

    blob_client = azureblob.BlockBlobService(account_name=san,
                                             sas_token=sat)
    blob_client.create_blob_from_path(container,
                                      "test.log",
                                      testlog_path)

