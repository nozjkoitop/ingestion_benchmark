import json
import random
import sys
import time
import uuid


def generate_data_point():
    timestamp = "2024-03-13T" + f"{time.time() % 1:.6f}Z"  # Generate unique timestamps
    value1 = round(random.random() * 100, 2)  # Random value between 0 and 100 (2 decimals)
    value2 = "data_" + str(uuid.uuid4())[:8]  # Random string identifier
    return {"timestamp": timestamp, "value1": value1, "value2": value2}


# Function to generate a chunk of data points
def generate_data_chunk(chunk_size):
    data = ""
    while sys.getsizeof(data) < chunk_size:
        data_point = generate_data_point()
        json_string = json.dumps(data_point)
        data += json_string + "\n"
    return data


if __name__ == '__main__':
    file_size = 1 * 1024 * 1024
    amount = 1
    for i in range(0, amount):
        file_name = f'test_object{str(i)}.jsonl'
        chunk = generate_data_chunk(file_size)
        f = open(f'duplicated/{file_name}', "x")
        f.write(chunk)
        print(f'File {file_name} was created')
