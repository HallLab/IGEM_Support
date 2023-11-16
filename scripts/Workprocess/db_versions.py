from datetime import datetime

import requests

v_file_url = 'https://github.com/HallLab/igem/blob/main/load/database/connector.csv.gz'

# Get file source header to check new versions
response = requests.head(v_file_url)

# Check response status
if response.status_code != 200:
    print("ERROR")

# Get header data to version control
v_date = ""
v_etag = ""
v_length = ""
if "ETag" in response.headers:
    v_etag = response.headers["ETag"]

# Create version control syntax
v_version = (
    "|etag:"
    + str(v_etag)
)
print(v_version)


import os

# Start download
if os.path.exists(v_source_file):
    os.remove(v_source_file)
    logger(log, "s", f"{qs.connector}: Source file deleted {v_source_file}")  # noqa E501

r = requests.get(v_file_url, stream=True)
with open(v_source_file, "wb") as f:
    # total_length = int(r.headers.get('content-length'))
    # for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1):  # noqa E501
    # TODO: Create a Variable System to control the Chunk Size.
    for chunk in r.iter_content(chunk_size=1000000):
        if chunk:
            f.write(chunk)
            f.flush()