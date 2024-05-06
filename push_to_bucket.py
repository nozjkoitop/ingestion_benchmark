import os

import boto3

if __name__ == '__main__':
    files_dir = './json'
    access_key = 'AKIAIOSFODNN7EXAMPLE'
    secret_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    bucket_name = 'test'
    endpoint = 'http://localhost:9444/'

    client = boto3.client(
        service_name='s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint
    )

    for file in os.listdir(files_dir):
        print(f'Uploading {file}')
        path_to_file = os.path.join(files_dir, file)
        response = client.upload_file(path_to_file, bucket_name, file)
        print(f'Uploading {path_to_file} finished')
        # print('sleeping 60 seconds')
        # time.sleep(60)
