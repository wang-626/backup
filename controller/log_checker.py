import os
from controller.logger import create_log


class LogChecker:
    '''control local log file '''

    def __init__(self, log_path):
        self.log_path = log_path

    def check_log_files(self):
        file_list = os.listdir(self.log_path)
        return len(file_list) > 0

    def get_log_files(self):
        res = {}
        files = self.get_log_files_names(True)
        for file_name in files:
            path = os.path.join(self.log_path, file_name)
            with open(path, 'r') as file:
                lines = file.readlines()
                data = {}
                for line in lines:
                    key, value = line.strip().split(":")
                    data[key] = value
                res[file_name.split(".")[0]] = data
        return res

    def get_log_files_names(self, extension=False):
        file_list = os.listdir(self.log_path)
        if extension:
            return file_list
        return map(lambda file: file.split('.')[0], file_list)

    def remove_logs(self):
        files = self.get_log_files_names(extension=True)
        for file in files:
            file_path = os.path.join(self.log_path, file)
            try:
                os.remove(file_path)
            except OSError as e:
                create_log(f"Error removing file {file_path}: {e}")
