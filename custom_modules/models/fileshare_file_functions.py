from azure.storage.fileshare import (
    ShareFileClient ,ShareDirectoryClient
)
import io

import logging 

LOGGER = logging.getLogger(__name__)

def download_file_content(connection_string, share_name, file_path):
    with ShareFileClient.from_connection_string(conn_str=connection_string, share_name=share_name, file_path=file_path) as file_client:
        temp_buffer = io.BytesIO()
        file_client.download_file().readinto(temp_buffer)
        temp_buffer.seek(0)
    return temp_buffer.getvalue()

def upload_file_to_file_share(connection_string, share_name, file_path, data):
    file_name = file_path.split("/")[-1]
    LOGGER.info(f"Archiving file : {file_name} to fileshare")
    
    with ShareFileClient.from_connection_string(conn_str=connection_string, share_name=share_name, file_path=file_path) as archive_client:
        archive_client.upload_file(data)

def delete_file_from_file_share(connection_string, share_name, file_path):
    file_name = file_path.split("/")[-1]
    LOGGER.info(f"Archiving file : {file_name} to fileshare")
    
    with ShareFileClient.from_connection_string(conn_str=connection_string, share_name=share_name, file_path=file_path) as archive_client:
        archive_client.delete_file()