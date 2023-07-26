import os

class Mensagem:

    def __init__(self, request='', key = '', value = '', timestamp = '', client_addrs = ''):
        self.request = request
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.client_addrs = client_addrs
    
    def __dict__(self):
        return {
            'request': self.request,
            'key': self.key,
            'value': self.value,
            'timestamp': self.timestamp,
            'client_addrs': self.client_addrs
        }

Mensagem()