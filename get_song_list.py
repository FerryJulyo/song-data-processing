import oss2
import os
from datetime import datetime, timezone

# Replace with your actual OSS credentials and details
access_key_id = 'LTAI4G6PxEhqoLDJLvuzdoT6'
access_key_secret = 'P8FJKWmXJImUFGdbHUWKaekDcpJctt'
endpoint = 'oss-ap-southeast-5.aliyuncs.com'
bucket_name = 'happypuppy-prod'

# Initialize the OSS bucket
auth = oss2.Auth(access_key_id, access_key_secret)
bucket = oss2.Bucket(auth, endpoint, bucket_name)

# Function to format file information with ; separator
def format_file_info(obj):
    last_modified = datetime.fromtimestamp(obj.last_modified, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    size = obj.size
    storage_class = obj.storage_class
    etag = obj.etag
    object_name = f'oss://{bucket_name}/{obj.key}'
    file_name = os.path.basename(obj.key)
    return f'{last_modified};{size};{storage_class};{etag};{object_name};{file_name}'

# Function to list all objects in a folder with paging
def list_all_objects(bucket, prefix, output_file_path):
    output_lines = []
    output_lines.append('LastModifiedTime;Size(B);StorageClass;ETAG;ObjectName;file_name')
    
    marker = ''
    while True:
        objs = bucket.list_objects(prefix=prefix, marker=marker)
        for obj in objs.object_list:
            output_lines.append(format_file_info(obj))
        if not objs.next_marker:
            break
        marker = objs.next_marker
    
    with open(output_file_path, 'w') as f:
        for line in output_lines:
            f.write(line + '\n')
    
    print(f'File information has been written to {output_file_path}')

# List all objects and save to files
list_all_objects(bucket, 'OKE/song-file/', 'D:\\SONG\\20250324_OKE.txt')
list_all_objects(bucket, 'song-file/', 'D:\\SONG\\20250324.txt')
