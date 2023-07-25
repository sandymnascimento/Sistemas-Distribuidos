import random
import threading
import socket
from mensagem import Mensagem
from datetime import datetime
import json
import os

class cliente:
    def __init__(self, mensagem) -> None:
        self.servers = []
        self.store = []
        self.start()


    def start(self):
        
        i == 0
        while True:
            try:
                print(f"Digite a operacao que deseja realizar [INIT | PUT | GET]:")
                action = input()

                if (i == 0 and action.upper() == "INIT"):
                    #Ler os servidores do teclado e adicionar a lista, no formato ('ip', port)
                    for i in range(3):
                        self.servers.append((input(), int(input())))
                else:
                    #Cria um socket para conexão com um servidor aleatório
                    peer = socket.socket()
                    port = random.choice(list(range(11000, 60000)))
                    peer.bind(('', port))
                    server = random.choice(self.servers)                     
                    peer.connect(server)
                    #Aciona o método responsável por executar a ação escolhida.
                    if (action.upper() == "PUT"):
                        self.put_actions(peer, port, server)
                    elif(action.upper() == "GET"):
                        self.get_actions(peer, server)
                    peer.close()

                
            except KeyboardInterrupt: 
                break 
    
    def put_actions(self, peer, port, server):
        #Captura do teclado key e value
        msg = Mensagem("PUT", input(), input()) 
        msg.client_addrs = (("127.0.0.1"),port)
        msg.server_addrs = server
        #Envia a key e value para um servidor aleatório
        peer.send(json.dumps(msg).encode())
        #Aguarda o retorno "Put_OK"
        response = peer.recv(1024).decode()
        mensagem = json.loads(response)

        if(mensagem.request == "PUT_OK"):
            print(f"PUT_OK key: [{mensagem.key}] value [{mensagem.value}] timestamp [{mensagem.timestamp}] realizada no servidor [{server}]")

            check = any(pairs['key'] == mensagem.key for pairs in self.store)
            if(check):
                for msg in self.store:
                    if(msg['key'] == mensagem.key):
                        msg['value'] = mensagem.value
                        msg['timestamp'] = mensagem.timestamp
            else:
                #Cria dicionario com key e value recebidos, associando um timestamp para essa key.
                dict = {"key": mensagem.key, "value": mensagem.value, "timestamp": mensagem.timestamp}
                self.store.append(dict)

    def get_actions(self, peer, server):
        timestamp = datetime.now().isoformat()
        #Captura do teclado key 
        key = input()
        
        for i in self.store:
            if(i['key'] == key):
                timestamp = i.timestamp
        msg = Mensagem("GET", key, timestamp)
        peer.send(json.dumps(msg).encode())

        response = peer.recv(1024).decode()
        mensagem = json.loads(response)

        if(mensagem.request == "GET_OK"):
            print(f"GET key: [{mensagem.key}] value: [{mensagem.valor}] obtido do servidor [{server}], meu timestamp [{timestamp}] e do servidor [{mensagem.timestamp}]")
        