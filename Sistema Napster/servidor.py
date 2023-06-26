import threading
import socket
import json

class servidor:
    def __init__(self):
        self.ip_server = input() 
        self.port_server = int(input()) 
        self.infos_peers = []
        self.start()


    def start(self):
        #Criação do socket, bind do endereço e abertura do server aguardando conexões.
        server = socket.socket()
        server.bind(('', self.port_server))
        server.listen()

        while True:
            try:
                #Aceita conexão e inicia uma thread para conseguir atender múltiplos clientes.
                peer, addr = server.accept()
                threading.Thread(target=self.start_connection, args=(peer, addr,)).start()
                
            except KeyboardInterrupt:
                server.close()
                break
        
        
    def start_connection(self, peer, addr):    
        while True:   
            try:
                #Aguarda mensagem do cliente informando qual operação deseja realizar
                action = peer.recv(1024).decode()
                    
                if(action.upper() == "JOIN"):
                    self.join_actions(peer, addr)

                elif(action.upper() == "SEARCH"):
                    self.search_actions(peer, addr)

                elif(action.upper() == "UPDATE"):
                    self.update_actions(peer, addr)

            except KeyboardInterrupt:
                raise

    def join_actions(self, peer, addr):
        #Aguarda o recebimento das informações do cliente.
        response = json.loads(peer.recv(1024).decode())
        #Armazena o endereço gerado pela conexão cliente-servidor junto das informações do cliente.
        response['addrs_server'] = addr
        response['addrs_peer'] = f"{response['addrs_peer'][0]}:{response['addrs_peer'][1]}"
        #Armazena as informações do cliente.
        self.infos_peers.append(response)
        print(f"Peer {response['addrs_peer']} adicionado com arquivos {' '.join(response['files'])}")
        #Confirma que o Join foi bem sucedido.
        peer.send("JOIN_OK".encode())
    
    def search_actions(self, peer, addr):
        #Aguarda o cliente dizer qual arquivo está buscando
        response = peer.recv(1024).decode()
        lista = []
        # Percorrendo a lista de dicionários
        for client in self.infos_peers:
            #IF utilizado para exibir a mensagem que informa o peer e o arquivo solicitado
            if(client['addrs_server'] == addr):
                print(f"Peer {client['addrs_peer']} solicitou arquivo {response}")
            #IF que verifica quais peers possuem o arquivo solicitado e adiciona a uma lista
            if(response in client['files']):
                lista.append(client['addrs_peer'])
        #Envia lista com todos os peers que possuem o arquivo para o cliente que realizou a solicitação
        peer.send(json.dumps(lista).encode())

    def update_actions(self, peer, addr):
        #Aguarda mensagem informando qual o nome do arquivo que foi adicionado a pasta do peer.
        response = peer.recv(1024).decode()
        for client in self.infos_peers:
            #Realiza a busca do peer dentre os disponíveis e adiciona na sua lista de arquivos o novo conteúdo.
            if(client['addrs_server'] == addr):
                client['files'].append(response)
                break
        #Resposta ao cliente informando que o Update foi bem sucedido.
        peer.send("UPDATE_OK".encode())
        

servidor()