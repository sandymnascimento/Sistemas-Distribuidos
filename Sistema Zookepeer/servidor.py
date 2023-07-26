from datetime import datetime
import threading
import socket
import json
import time
from mensagem import Mensagem

class Servidor:
    def __init__(self):
        #Leitura do endereço e porta do servidor atual e do líder.
        self.ip_server = input() 
        self.port_server = int(input()) 

        self.ip_lider = input()
        self.port_lider = int(input())
        
        self.server_connections = []
        self.lider = False

        #Verifica se o servidor atual é o líder.
        if(self.ip_server == self.ip_lider and self.port_server == self.port_lider):
            self.lider = True
        else:
            self.addrs_lider = (self.ip_lider, self.port_lider)

        #lista de Mensagens
        self.hash_storage = [] 
        self.replication = []
        self.semaforo_put = threading.Semaphore(1)
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
                    peer, addr = server.accept()

                    if(s < 2):
                        #Armazena a conexão com os servidores e cria um dicionario que servirá como flag de replicação.
                        self.server_connections.append(peer)
                        self.replication.append({'addr' : addr, 'ok' : False})
                        s += 1

                    #Inicia uma thread para conseguir atender múltiplos servidores e clientes.
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
                #Socket que aguarda para receber conexões de clientes.
                server.listen()

                #Aceita conexão e inicia uma thread para conseguir atender múltiplos clientes.
                peer, addr = server.accept()
                threading.Thread(target=self.client_connection, args=(lider_conn, peer, addr,)).start()
    
    def try_connect_leader(self):
        #Tenta se conectar ao líder.
        peer = socket.socket()
        peer.connect(self.addrs_lider)

        #Caso a conexão seja bem sucedida, retorna o socket.
        return peer

    #Função que atende conexões de outros servidores e de clientes. 
    def lider_connection(self, conn, addr):    
        while True:   
            try:
                #Aguarda mensagem informando qual operação deseja realizar
                action = conn.recv(1024).decode() 
                action = json.loads(action)

                if(not action['client_addrs']):
                    action['client_addrs'] = addr  

                if(action['request'] == "PUT"):
                    self.put_actions(action, conn, addr)

                elif(action['request'] == "GET"):
                    self.get_actions(action, conn)

                elif(action['request'] == "REPLICATION_OK"):
                    for addrs in self.replication:
                        if(addrs['addr'] == addr):
                            addrs['ok'] = True

                if(addr == action['client_addrs'] and action['request'] != "REPLICATION_OK"):
                    break
            except KeyboardInterrupt:
                raise
    
    #Função que realiza o put e replica a mensagem nos outros servidores.
    def put_actions(self, mensagem, conn, addr): 
        #Inicia semaforo para que ocorra apenas um put por vez.
        self.semaforo_put.acquire()
        try:     
            print(f"Cliente {mensagem['client_addrs']} PUT key: {mensagem['key']} value: {mensagem['value']}")
            
            check = any(msg['key'] == mensagem['key'] for msg in self.hash_storage)
            mensagem['timestamp'] = datetime.now().isoformat()
            if(check):
                #Só atualiza se o valor for mais recente.
                if(mensagem['timestamp'] != '0'):
                    #Se a key já existir, atualize o value e o timestamp associado.
                    for msg in self.hash_storage:
                        if(msg['key'] == mensagem['key']):
                            msg['value'] = mensagem['value']
                            msg['timestamp'] = mensagem['timestamp']
            else:
                #Cria dicionario com key e value recebidos, associando um novo timestamp para essa key.
                dict = {"key": mensagem['key'], "value": mensagem['value'], "timestamp": mensagem['timestamp']}
                self.hash_storage.append(dict)  

            #Replique a informação (key, value, timestamp) nos outros servidores, enviando-a na mensagem REPLICATION
            mensagem['request'] = "REPLICATION"

            #Envia mensagem para os servidores que abriram conexão com o líder.
            for server in self.server_connections:
                server.send(json.dumps(mensagem).encode())
            
            #Verificação para saber se a resposta do servidor vem da thread cliente ou servidor.
            if(addr != mensagem['client_addrs']):  
                #Capta resposta do servidor que solicitou o put.           
                action = conn.recv(1024).decode()
                action = json.loads(action)

                #Confirma que a mensagem foi replicada com sucesso e atualiza flag de replicação.
                if(action['request'] == "REPLICATION_OK"):
                    for addrs in self.replication:
                        if(addrs['addr'] == addr):
                            addrs['ok'] = True
            while True:               
                #Verifica se todos os servidores replicaram a mensagem.
                if(not any(addrs['ok'] == False for addrs in self.replication)):
                    #Se o put foi solicitado por um cliente, envie para o cliente a mensagem PUT_OK junto com o timestamp associado à key.
                    if(addr == mensagem['client_addrs']):
                        print(f"Enviando PUT_OK ao Cliente {mensagem['client_addrs']} da key: [{mensagem['key']}] ts: [{mensagem['timestamp']}]")

                    #Envie para o cliente a mensagem PUT_OK junto com o timestamp associado à key. 
                    conn.send(json.dumps(Mensagem("PUT_OK", mensagem['key'], mensagem['value'], mensagem['timestamp'], mensagem['client_addrs']).__dict__()).encode())

                    #Reseta as flags de replicação para que possa ser feito um novo put.
                    for addrs in self.replication:
                        addrs['ok'] = False
                    break                    
        finally:
            self.semaforo_put.release()
            
    #Função que realiza o get e envia mensagem aos clientes.
    def get_actions(self, mensagem, client):        
        check = any(msg['key'] == mensagem['key'] for msg in self.hash_storage)
        if(not check):
            #Caso a chave não exista, o value devolvido será NULL
            client.send(json.dumps(Mensagem("GET_OK", mensagem['key'],'','0').__dict__()).encode())
            print(f"Cliente [{mensagem['client_addrs']}] GET key:[{mensagem['key']}] ts:[{mensagem['timestamp']}]. Meu ts é [0], portanto devolvendo [NULL]")
        else:
            i = 0
            #Checa se o timestamp do cliente é mais antigo ou igual ao que a hash_storage possui
            for msg in self.hash_storage:
                if(msg['key'] == mensagem['key']):
                    if(msg['timestamp'] >= mensagem['timestamp']):
                        client.send(json.dumps(Mensagem("GET_OK", msg['key'], msg['value'], msg['timestamp']).__dict__()).encode())
                        print(f"Cliente [{mensagem['client_addrs']}] GET key:[{mensagem['key']}] ts:[{mensagem['timestamp']}]. Meu ts é [{msg['timestamp']}], portanto devolvendo [{msg['value']}]")
                        break
                    ts = msg['timestamp']
                i += 1
            #Se o cliente possuir o value mais atualizado, retornar o erro.
            if(i == len(self.hash_storage)):
                client.send(json.dumps(Mensagem("TRY_OTHER_SERVER_OR_LATER", mensagem['key'], '', mensagem['timestamp']).__dict__()).encode())
                print(f"Cliente [{mensagem['client_addrs']}] GET key:[{mensagem['key']}] ts:[{mensagem['timestamp']}]. Meu ts é [{ts}], portanto devolvendo [TRY_OTHER_SERVER_OR_LATER]")

    def servers_connection(self, lider_conn):
        while True:
            try:
                #Aguarda mensagem do líder informando qual operação deseja realizar
                message = lider_conn.recv(1024).decode() 
                message = json.loads(message)

                if(message['request'].upper() == "REPLICATION"):                    
                    check = any(msg['key'] == message['key'] for msg in self.hash_storage)
                    if(check):
                        #Se a key já existir, atualize tanto o value quanto o timestamp associado.
                        for msg in self.hash_storage:
                            if(msg['key'] == message['key']):
                                msg['value'] = message['value']
                                msg['timestamp'] = datetime.now().isoformat()
                                print(f"REPLICATION key: [{msg['key']}] value: [{msg['value']}] ts: [{msg['timestamp']}].")
                    else:
                        #Cria dicionario com key e value recebidos, associando um timestamp para essa key.
                        dict = {"key": message['key'], "value": message['value'], "timestamp": datetime.now().isoformat()}
                        self.hash_storage.append(dict)
                        print(f"REPLICATION key: {dict['key']} value: {dict['value']} ts:{dict['timestamp']}.")
                    
                    #Envia resposta para o líder
                    lider_conn.send(json.dumps(Mensagem("REPLICATION_OK").__dict__()).encode())         
            except KeyboardInterrupt:
                raise
                 
    def client_connection(self, lider_conn, client, port):
        while True:
            try:
                #Aguarda mensagem do cliente informando qual operação deseja realizar
                action = client.recv(1024).decode() 
                action = json.loads(action)
                action['client_addrs'] = port

                if(action['request'].upper() == "PUT"):
                    print(f"Encaminhando PUT key: [{action['key']}] value:[{action['value']}]")                
                    #Envia requisição para o líder
                    lider_conn.send(json.dumps(action).encode())

                    #Aguarda resposta do líder
                    message = lider_conn.recv(1024).decode()
                    message = json.loads(message)

                    if(message['request'].upper() == "PUT_OK"):
                        for msg in self.hash_storage:
                            if(msg['key'] == message['key']):
                                print(f"Enviando PUT_OK ao Cliente {message['client_addrs']} da key: {msg['key']} ts: {msg['timestamp']}")  

                                #Envie para o cliente a mensagem PUT_OK junto com o timestamp associado à key.
                                message['timestamp'] = msg['timestamp']
                                client.send(json.dumps(message).encode())
  
                elif(action['request'].upper() == "GET"):
                    #Aciona o método responsável por fazer as ações do get.
                    self.get_actions(action, client)
                break
            except KeyboardInterrupt:
                raise    

Servidor()