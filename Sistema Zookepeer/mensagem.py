import os

class Mensagem:

    def __init__(self, request, key = None, value = None) -> None:
        self.request = request
        self.key = key
        self.value = value
        self.timestamp = ''
        self.client_addrs = ''
        self.client_conn = ''
        self.server_addrs = ''