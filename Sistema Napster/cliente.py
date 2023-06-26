import threading
import socket
import json
import os

class cliente:
    def __init__(self) -> None:
        self.ip = input()
        self.port = int(input())
        self.data = {
            'addrs_server' : (), # Endereço de conexão com o servidor.
            'addrs_peer': (self.ip, self.port), #f"{self.ip}:{str(self.port)}", # Endereço disponível para outros clientes.
            'folder_path' : input(), #Caminho para a pasta do cliente, onde os arquivos estão armazenados.
            'files' : [] #Lista com o nome dos arquivos sob posse do cliente.
        }
        self.data['files'] = self.find_files()
        self.search_peers = {}
        self.start()


    def start(self):
        peer = socket.socket()
        peer.connect(('127.0.0.1', 1099)) 

        #Thread que irá permitir conexão entre clientes para download de arquivos.
        threading.Thread(target=self.peer_server, args=()).start()
        join = 0
        while True:
            try:
                print(f"Digite a operacao que deseja realizar [JOIN | SEARCH | DOWNLOAD]:")
                action = input()

                #Aciona o método responsável por executar a ação escolhida.
                if (action.upper() == "JOIN" and join == 0):
                    #Envia a ação desejada ao servidor e inicializa o método de ação no cliente.
                    peer.send(action.encode())
                    rt = self.join_actions(peer)
                    #Faz a checagem para garantir que o Join só será realizado uma vez.
                    if(rt == "JOIN_OK"):
                        join += 1
                elif(action.upper() == "SEARCH" and join > 0):
                    #Envia a ação desejada ao servidor e inicializa o método de ação no cliente.
                    peer.send(action.encode())       
                    self.search_actions(peer)
                elif(action.upper() == 'DOWNLOAD' and join > 0):
                    #Envia a ação desejada ao servidor e inicializa o método de ação no cliente.
                    peer.send(action.encode())
                    self.download_actions(peer)

            except KeyboardInterrupt: 
                peer.close()
                break 

    #Realiza a busca dos arquivos na pasta indicada        
    def find_files(self):
        folder = self.data['folder_path']
        files = []
        if os.path.exists(folder):
            for name_file in os.listdir(folder):
                # Verifica se é um arquivo
                if os.path.isfile(os.path.join(folder, name_file)):
                    # Adiciona à lista de arquivos
                    files.append(name_file)
        else:
            #Cria a pasta caso não exista
            os.mkdir(folder)
        #Retorna uma lista com todos os arquivos.extensões exitentes na pasta do cliente.
        return files   

    #Método acionado para realizar as ações quando o cliente seleciona a opção "JOIN" no menu.         
    def join_actions(self, peer):
        #Envia ao servidor um dicionario com as informações do peer.
        json_package = json.dumps(self.data)
        peer.send(json_package.encode())
        #Aguarda o retorno do "Join_OK"
        response = peer.recv(1024).decode()
        #Retorno solicitado.
        if(response.upper() == "JOIN_OK"):
            print(f"Sou peer {self.data['addrs_peer'][0]}:{self.data['addrs_peer'][1]} com arquivos {' '. join(self.data['files'])}")
            return "JOIN_OK"

    def search_actions(self, peer):
        file = input()
        #Solicita o arquivo ao servidor
        peer.send(file.encode())
        #Espera a resposta com quais servidores possuem o arquivo
        peers = json.loads(peer.recv(1024).decode())
        print(f"peers com arquivo solicitado: {' '.join(peers)}")
        #Armazena a informação obtida no search para realizar o download
        if(len(peers) > 0): 
            dicti = {
                'name'  : file,
                'peers' : peers
            }
            self.search_peers = dicti
        return

    def download_actions(self, peer):
        ip = input()
        port = input()
        server = f"{ip}:{port}"
        
        try:
            #Verifica se o endereço informado é válido como fonte do arquivo desejado
            if(server in self.search_peers['peers']):
                s = socket.socket()
                s.connect((ip,int(port)))
                
                if(s.recv(1024).decode() == "Connect_OK"):
                    s.send(self.search_peers['name'].encode())
                    # Abra o arquivo em modo de escrita binária
                    with open(self.data['folder_path'] + "\\" + self.search_peers['name'], 'wb') as arquivo:
                        while True:
                            # Recebe uma parte do arquivo
                            dados = s.recv(1024)
                            if not dados:
                                # Todos os dados foram lidos
                                break
                            #Escreve o dado no arquivo
                            arquivo.write(dados)
                    print(f"Arquivo {self.search_peers['name']} baixado com sucesso na pasta {os.path.basename(self.data['folder_path'])}")
                    peer.send("UPDATE".encode())
                    peer.send(self.search_peers['name'].encode())
                    peer.recv(1024).decode()
                    self.data['files'] = self.find_files()
        except KeyError:
            return


    def peer_server(self):
        server = socket.socket()
        server.bind((self.data['addrs_peer'][0], self.data['addrs_peer'][1]))
        server.listen()
        
        while True:
            try:
                peer, addr = server.accept()
                peer.send("Connect_OK".encode())
                file = peer.recv(1024).decode()
                if(file in self.data['files']):
                    # Abra o arquivo em modo de leitura binária
                    with open(self.data['folder_path'] + "\\" + file, 'rb') as arquivo:
                        while True:
                            # Leia uma parte do arquivo
                            dados = arquivo.read(1024)
                            if not dados:
                                # Todos os dados foram lidos
                                break
                            # Envie os dados para o servidor
                            peer.send(dados)

                peer.close()
            except KeyboardInterrupt:
                break
        server.close()

cliente()            