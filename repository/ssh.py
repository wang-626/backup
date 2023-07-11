import paramiko


class Ssh:

    def __init__(self, ssh_set):
        print(123)
        self.ssh_set = ssh_set
        self.sftp = None
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print(456)

    def connect(self, test=False) -> bool:
        '''ssh connect return true or false'''
        try:
            self.client.connect(**self.ssh_set)
            return True
        except Exception as e:
            return False

    def open_sftp(self):
        '''open sftp'''
        self.sftp = self.client.open_sftp()

    def disconnect(self):
        '''disconnect SSH'''
        self.client.close()

    def __del__(self):
        '''close sql if sql not close'''
        if self.client.get_transport() is not None:
            if self.client.get_transport().is_active():
                self.disconnect()
