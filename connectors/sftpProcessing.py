
import re
from io import BytesIO
import pysftp



class SFTPProcessing():
    def __init__(self, hostname, port, username, password,remote_directory):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None 
        self.connection = pysftp.Connection(hostname, port=port, username=username, password=password,cnopts=cnopts)
        self.config = {"hostname" : hostname , "port" : port , "username" : port , "password" : password , "remote_directory" : remote_directory}

    def read_files(self, regex_pattern=r'.*'):
        with self.connection as sftp : 
            files_content = []
            sftp.chdir(self.config['remote_directory'])
            files_to_download = [file for file in sftp.listdir() if re.match(regex_pattern, file)]
            
            for file_to_download in files_to_download:
                remote_file_path = f"{self.config['remote_directory']}/{file_to_download}"
                
                # Check if the file exists before attempting to download
                if sftp.exists(remote_file_path):
                    # Create a BytesIO object to act as a file-like object
                    file_content = BytesIO()
                    sftp.getfo(remote_file_path, file_content)
                    file_content.seek(0)  # Move the file cursor to the beginning
                    
                    files_content.append((file_to_download, file_content))

                else:
                    print(f"File {remote_file_path} does not exist.")
            print(len(files_content) , "file content len")
            return files_content

    def push_files(self, files_info):
        with self.connection as sftp:
            for file_info in files_info:
                file_name = file_info['file_name']
                file_content = file_info['file_content']
                remote_file_path = f"{self.sftp_config['remote_directory']}/{file_name}"
                sftp.putfo(file_content, remote_file_path)
                yield f"File '{file_name}' successfully pushed to SFTP server."

