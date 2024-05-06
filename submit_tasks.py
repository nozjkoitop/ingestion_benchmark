import time
from itertools import islice

import boto3
import requests


def check_task_status(t_id, injection_start_time, timeout=5):
    task_status_url = f'http://localhost:8888/druid/indexer/v1/task/{t_id}/status'

    get_response = requests.get(task_status_url)
    if get_response.status_code == 200:
        task_status = get_response.json()['status']['status']
        if task_status == "PENDING":
            print(f"Task {t_id} is still pending. Waiting for completion...")
            time.sleep(timeout)
            check_task_status(t_id, injection_start_time)
        elif task_status == "RUNNING":
            print(f"Task {t_id} is still running. Waiting for completion...")
            time.sleep(timeout)
            check_task_status(t_id, injection_start_time)
        elif task_status == "SUCCESS":
            print(f"File {key} submitted successfully! Time: {time.time() - injection_start_time:.2f} seconds")
            return
        else:
            print(f"Task {t_id} failed with status: {task_status}")
            return
    else:
        print(f"Error checking task status: {get_response.text}")
        return


def batched(iterable, n):
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while t_batch := tuple(islice(it, n)):
        yield "\n".join(t_batch)


def submit_task(batch):
    ingestion_url = "http://localhost:8888/druid/indexer/v1/task"
    # cred = ("root", "root")
    # Set content type headers
    headers = {"Content-Type": "application/json"}
    # Define task spec for the chunk
    task_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "http_push_benchmark_1",
                "timestampSpec": {
                    "column": "timestamp",
                    "format": "auto"
                },
                "dimensionsSpec": {
                    "dimensions": [
                        "value1",
                        "value2"
                    ]
                },
                "granularitySpec": {
                    "segmentGranularity": "ALL",
                    "queryGranularity": "none",
                    "rollup": False
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "inline",
                    "data": batch
                },
                "inputFormat": {
                    "type": "json"
                },
                "appendToExisting": True,
                "dropExisting": False
            }
        }
    }

    # Send POST request to submit task for the chunk
    start_task_time = time.time()
    response = requests.post(ingestion_url, headers=headers, json=task_spec)
    if response.status_code == 200:
        task_id = response.json()['task']
        check_task_status(task_id, injection_start_time=start_task_time)
    else:
        print(f"Error submitting file with key - {key}: {response.text}")


if __name__ == '__main__':

    # Total start time for benchmark
    start_time = time.time()

    # Loop through generating and submitting data chunks
    resource = boto3.resource(service_name='s3',
                              aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
                              aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                              endpoint_url='http://localhost:9444/')

    resource_bucket = resource.Bucket('test')
    count = 1
    for obj in resource_bucket.objects.all():
        iter_start = time.time()
        key = obj.key
        # if key == 'test_object0.jsonl' or key == 'test_object1.jsonl':
        #     print('skipping')
        #     continue

        print(f'Task initiated for {key} ingestion ({count} of 100)')
        count += 1
        body = obj.get()['Body'].read()
        data = body.decode("utf-8")

        lines = data.strip().split("\n")
        b_count = 1
        for batch in batched(lines, 50000):
            print(f'submitting {b_count} batch for {key}')
            b_count += 1
            submit_task(batch)

        submit_task(data)
        print(f'Iteration time = {time.time() - iter_start:.2f} seconds')

    end_time = time.time()

    # Print total benchmark time
    print(f"Total benchmark time: {end_time - start_time:.2f} seconds")
