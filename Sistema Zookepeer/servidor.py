import datetime
import threading
import socket
import json
import time
from mensagem import Mensagem

class servidor:
    def __init__(self):
        self.ip_server = input() 
        self.port_server = int(input()) 
        self.ip_lider = input()
        self.port_lider = int(input())
        self.lider = False
        self.server_connections = []
        if(self.ip_server == self.ip_lider and self.port_server == self.port_lider):
            self.lider = True
        else:
            self.addrs_lider = (self.ip_lider, self.port_lider)

        #lista de Mensagens
        self.hash_storage = [] 
        self.start()


    def start(self):
        #Criação do socket, bind do endereço e abertura do server aguardando conexões.
        server = socket.socket()
        server.bind(('', self.port_server))
        s = 0
        if(self.lider):
            while True:
                try:
                    server.listen()
                    #Aceita conexão e inicia uma thread para conseguir atender múltiplos servidores.
                    peer, addr = server.accept()

                    if(s >= 2):
                        self.server_connections.append(peer)
                        s += 1
                    threading.Thread(target=self.lider_connection, args=(peer, addr,)).start()
                    
                except KeyboardInterrupt:
                    server.close()
                    break
        else:
            #O servidor atual não é líder e deve ficar tentando se conectar até que o líder seja inciado.
            while True:
                try:
                    #Recebe conexão com o líder.
                    lider_conn = self.try_connect_leader()
                    
                    #Caso a conexão seja bem sucedida, inicia a thread de conexão.
                    threading.Thread(target=self.servers_connection, args=(lider_conn, )).start()
                    break
                except ConnectionRefusedError:
                    time.sleep(5)
            while True:
                #Socket aguarda para receber conexões de clientes.
                server.listen()
                #Aceita conexão e inicia uma thread para conseguir atender múltiplos clientes.
                peer, addr = server.accept()
                threading.Thread(target=self.client_connection, args=(lider_conn, peer, addr,)).start()
    
    def try_connect_leader(self):
        #Tenta se conectar ao líder.
        peer = socket.socket()
        peer.connect(self.addrs_lider)
        #peer.send("Conectado".encode())
        #Caso a conexão seja bem sucedida, retorna o socket.
        return peer

    #Função que atende conexões de outros servidores e de clientes. 
    def lider_connection(self, conn, addr):    
        while True:   
            try:
                #Aguarda mensagem informando qual operação deseja realizar
                action = conn.recv(1024).decode() 
                action = json.loads(action)

                #REALIZA APENAS UM PUT POR VEZ, ADICIONAR SEMAFORO AQUI
                if(action.request.upper() == "PUT"):
                    print(f"Cliente {action.client_addrs} PUT key: {action.key} value: {action.value}")
                    self.put_actions(action, conn, addr)
                elif(action.request.upper() == "GET"):
                    self.get_actions(action, conn)
            except KeyboardInterrupt:
                raise

    def put_actions(self, mensagem, conn, addr):        
        #Se a key já existir, atualize tanto o value quanto o timestamp associado.
        check = any(msg.key == mensagem.key for msg in self.hash_storage)
        mensagem.timestamp = datetime.now().isoformat()
        if(check):
            for msg in self.hash_storage:
                if(msg['key'] == mensagem.key):
                    msg.value = mensagem.value
                    msg.timestamp = mensagem.timestamp
        else:
            #Cria dicionario com key e value recebidos, associando um timestamp para essa key.
            dict = {"key": mensagem.key, "value": mensagem.value, "timestamp": mensagem.timestamp}
            self.hash_storage.append(dict)

        #Replique a informação (key, value, timestamp) nos outros servidores, enviando-a na mensagem REPLICATION
        mensagem.request = "REPLICATION"
        #Envia mensagem para os servidores que abriram conexão com o atual.
        for server in self.server_connections:
            server.send(json.dumps(mensagem).encode())
            ok = json.loads(server.recv(1024).decode())
            if(ok.request.upper() == "REPLICATION_OK"):
                pass
        
        if(addr == mensagem.client_addrs):
            print(f"Enviando PUT_OK ao Cliente {mensagem.client_addrs} da key: {mensagem.key} ts: {mensagem.timestamp}")
        conn.send(json.dumps(Mensagem("PUT_OK", mensagem.key, mensagem.value, mensagem.timestamp)).encode())
        

    def get_actions(self, mensagem, client):
        #Caso a chave não exista, o value devolvido será NULL
        check = any(msg.key == mensagem.key for msg in self.hash_storage)
        if(not check):
            client.send(json.dumps(Mensagem("GET_OK")).encode())
            print(f"Cliente [{mensagem.client_addrs}] GET key:[{mensagem.key}] ts:[{mensagem.timestamp}]. Nao possuo a key, portanto devolvendo [NULL]")
        else:
            i = 0
            #Checa se o timestamp do cliente é mais antigo ou igual ao que a hash_storage possui
            for msg in self.hash_storage:
                if(msg['key'] == mensagem.key):
                    if(msg['timestamp'] >= mensagem.timestamp):
                        client.send(json.dumps(Mensagem("GET_OK", msg['value'], msg['timestamp'])).encode())
                        print(f"Cliente [{mensagem.client_addrs}] GET key:[{mensagem.key}] ts:[{mensagem.timestamp}]. Meu ts é [{msg['timestamp']}], portanto devolvendo [{msg.value}]")
                        break
                    ts = msg['timestamp']
                i += 1
            if(i == len(self.hash_storage)):
                client.send(json.dumps(Mensagem("TRY_OTHER_SERVER_OR_LATER")).encode())
                print(f"Cliente [{mensagem.client_addrs}] GET key:[{mensagem.key}] ts:[{mensagem.timestamp}]. Meu ts é [{ts}], portanto devolvendo [TRY_OTHER_SERVER_OR_LATER]")

    def servers_connection(self, lider_conn):
        while True:
            try:
                #Aguarda mensagem do líder informando qual operação deseja realizar
                message = lider_conn.recv(1024).decode() 
                message = json.loads(message)
                if(message.request.upper() == "REPLICATION"):
                    #Se a key já existir, atualize tanto o value quanto o timestamp associado.
                    check = any(msg.key == message.key for msg in self.hash_storage)
                    if(check):
                        for msg in self.hash_storage:
                            if(msg['key'] == message.key):
                                msg['value'] = message.value
                                msg['timestamp'] = datetime.now().isoformat()
                                print(f"REPLICATION key: {msg['key']} value: {msg['value']} ts:{msg['timestamp']}.")
                    else:
                        #Cria dicionario com key e value recebidos, associando um timestamp para essa key.
                        dict = {"key": message.key, "value": message.value, "timestamp": datetime.now().isoformat()}
                        self.hash_storage.append(dict)
                        print(f"REPLICATION key: {dict['key']} value: {dict['value']} ts:{dict['timestamp']}.")

                    #Envia resposta para o líder
                    lider_conn.send(json.dumps(Mensagem("REPLICATION_OK")).encode())
            except KeyboardInterrupt:
                raise
                 
    def client_connection(self, lider_conn, client, port):
        while True:
            try:
                #Aguarda mensagem do cliente informando qual operação deseja realizar
                action = client.recv(1024).decode() 
                action = json.loads(action)
                #PENSAR NO SEMAFORO, PARA REALIZAR APENAS UM PUT POR VEZ
                if(action.request.upper() == "PUT"):
                    print(f"Encaminhando PUT key: [{action.key}] value:[{action.value}]")                
                    #Envia requisição para o líder
                    lider_conn.send(json.dumps(action).encode())

                    #Aguarda resposta do líder
                    message = lider_conn.recv(1024).decode()
                    message = json.loads(message)

                    if(message.request.upper() == "PUT_OK"):
                        for msg in self.hash_storage:
                            if(msg['key'] == message.key):
                                print(f"Enviando PUT_OK ao Cliente {msg['client_addrs']} da key: {msg['key']} ts: {msg['timestamp']}")                                
                                #Envie para o cliente a mensagem PUT_OK junto com o timestamp associado à key.
                                message.timestamp = msg['timestamp']
                                client.send(message)
  
                if(action.request.upper() == "GET"):
                    self.get_actions(action, client)

            except KeyboardInterrupt:
                raise    
