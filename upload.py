import os
import sys

import azure.storage.blob as azureblob

if __name__ == '__main__':

    hogehoge = sys.argv[1]
    san = sys.argv[2]
    sat = sys.argv[3]
    testlog = open("/tmp/test.log", 'w')
    testlog.write(hogehoge)
    testlog.close()






    testlog_path = os.path.realpath("/tmp/test.log")

    container = "output"
    blob_client = azureblob.BlockBlobService(account_name=san,
                                             sas_token=sat)

    print(os.getcwd())
    print('Uploading file {} to container [{}]...'.format(
        testlog_path,
        container))

    blob_client.create_blob_from_path(container,
                                      "test.log",
                                      testlog_path)

