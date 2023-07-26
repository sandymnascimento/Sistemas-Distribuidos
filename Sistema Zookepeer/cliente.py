from mensagem import Mensagem
from datetime import datetime
import random
import socket
import json
import time
import os

class Cliente:
    def __init__(self) -> None:
        self.servers = []
        self.store = []
        self.start()

    def start(self):
        i = 0
        while True:
            try:
                print(f"Digite a operacao que deseja realizar [INIT | PUT | GET]:")
                action = input()

                if (i == 0 and action.upper() == "INIT"):
                    #Ler os servidores do teclado e adicionar a lista, no formato ('ip', port)
                    for i in range(3):
                        self.servers.append((input(), int(input())))
                        pass
                elif (action.upper() == "PUT" or action.upper() == "GET"):
                    #Cria um socket para conexão com um servidor aleatório
                    peer = socket.socket()
                    server = random.choice(self.servers)                     
                    peer.connect(server)

                    #Aciona o método responsável por executar a ação escolhida.
                    if (action.upper() == "PUT"):
                        self.put_actions(peer, server)
                    elif(action.upper() == "GET"):
                        self.get_actions(peer, server)
                    peer.close()

                
            except KeyboardInterrupt: 
                break 
    
    def put_actions(self, peer, server):
        key = input()
        value = input()
        timestamp = self.timestamp(key)
        #Captura do teclado key e value
        msg = Mensagem("PUT", key, value, timestamp) 
        msg.server_addrs = server

        #Envia a key e value para o servidor escolhido aleatóriamente.
        peer.send(json.dumps(msg.__dict__()).encode()) 

        #Aguarda o retorno "Put_OK"
        response = peer.recv(1024).decode()
        mensagem = json.loads(response)

        if(mensagem['request'] == "PUT_OK"):
            print(f"PUT_OK key: [{mensagem['key']}] value: [{mensagem['value']}] timestamp: [{mensagem['timestamp']}] realizada no servidor {server}")
            #Aciona a função que armazena a key, value e timestamp no dicionário store
            self.add_store(mensagem)

    def get_actions(self, peer, server):
        #Captura do teclado key 
        key = input()
        
        #Verifica se a key está armazenada para capturar o timestamp
        timestamp = self.timestamp(key)

        #Solicita o GET ao servidor escolhido aleatoriamente.
        msg = Mensagem("GET", key, '', timestamp)
        peer.send(json.dumps(msg.__dict__()).encode())

        response = peer.recv(1024).decode()
        mensagem = json.loads(response)
        
        #Trata o retorno do servidor a partir do request.
        if(mensagem['request'] == "GET_OK" and mensagem['value']):
            print(f"GET key: [{mensagem['key']}] value: [{mensagem['value']}] obtido do servidor {server}, meu timestamp [{timestamp}] e do servidor [{mensagem['timestamp']}]")
            mensagem['timestamp'] = datetime.now().isoformat()
            self.add_store(mensagem)
        elif(mensagem['request'] == "TRY_OTHER_SERVER_OR_LATER"):
            print(f"GET key: [{mensagem['key']}] value: [{mensagem['request']}] obtido do servidor {server}, meu timestamp [{timestamp}] e do servidor [{mensagem['timestamp']}]")
        else:
            print(f"GET key: [{mensagem['key']}] value: [NULL] obtido do servidor {server}, meu timestamp [{timestamp}] e do servidor [{mensagem['timestamp']}]")
    
    def add_store(self, mensagem):
        #Verifica se a key já está armazenada no store
        check = any(pairs['key'] == mensagem['key'] for pairs in self.store)
        if(check):
            #Atualiza o value e timestamp da key armazenada com o retorno do servidor.
            for msg in self.store:
                if(msg['key'] == mensagem['key']):
                    msg['value'] = mensagem['value']
                    msg['timestamp'] = mensagem['timestamp']
        else:
            #Cria dicionario com key e value recebidos, associando um timestamp para essa key.
            dict = {"key": mensagem['key'], "value": mensagem['value'], "timestamp": mensagem['timestamp']}
            self.store.append(dict)

    def timestamp(self, key):
        #Verifica se a key está armazenada para capturar o timestamp
        for i in self.store:
            if(i['key'] == key):
                return i['timestamp']
        return '0'
    
Cliente()