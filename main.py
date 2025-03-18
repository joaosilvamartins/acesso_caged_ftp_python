from ftplib import FTP
import os
import re
import py7zr
import pandas as pd
import csv

# Configurações do servidor FTP
host = "ftp.mtps.gov.br"
diretorio_base = "/pdet/microdados/NOVO CAGED/"

# Lista de ocupações permitidas
ocupacoes_permitidas = {"223505", "233125", "234415", "322205", "322210", "322215", 
                        "322220", "322230", "322235", "322245", "322250", "515110"}

# Nome do arquivo de histórico
historico_csv = "historico_arquivos.csv"

# Carregar histórico
historico = {}
if os.path.exists(historico_csv):
    with open(historico_csv, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader, None)  # Pular cabeçalho
        for linha in reader:
            historico[linha[0]] = linha[1]  # {arquivo: data_modificacao}

# Conectar ao FTP
ftp = FTP(host)
ftp.encoding = "latin-1"
ftp.login()
ftp.cwd(diretorio_base)

# Criar diretório local
os.makedirs("NOVO_CAGED", exist_ok=True)

# Listar anos disponíveis
anos = [item for item in ftp.nlst() if re.fullmatch(r"\d{4}", item)]

for ano in anos:
    caminho_ano = f"{diretorio_base}{ano}/"
    ftp.cwd(caminho_ano)
    meses = [item for item in ftp.nlst() if re.fullmatch(r"\d{6}", item)]

    os.makedirs(f"NOVO_CAGED/{ano}", exist_ok=True)

    for mes in meses:
        caminho_mes = f"{caminho_ano}{mes}/"
        ftp.cwd(caminho_mes)
        arquivos = [arq for arq in ftp.nlst() if arq.endswith(".7z") and "MOV" in arq]

        if arquivos:
            arquivos.sort()
            arquivo_mais_recente = arquivos[-1]
            data_modificacao = ftp.sendcmd(f"MDTM {arquivo_mais_recente}")[4:]  # Obtém a data YYYYMMDDHHMMSS

            # Se o arquivo já foi processado, pular
            if arquivo_mais_recente in historico and historico[arquivo_mais_recente] == data_modificacao:
                print(f"Arquivo {arquivo_mais_recente} já processado. Pulando...")
                continue

            os.makedirs(f"NOVO_CAGED/{ano}/{mes}", exist_ok=True)
            caminho_local_7z = f"NOVO_CAGED/{ano}/{mes}/{arquivo_mais_recente}"

            # Baixar o arquivo .7z
            print(f"Baixando: {arquivo_mais_recente} para {caminho_local_7z}")
            with open(caminho_local_7z, "wb") as f:
                ftp.retrbinary(f"RETR {arquivo_mais_recente}", f.write)

            # Descompactar o arquivo .7z
            with py7zr.SevenZipFile(caminho_local_7z, mode='r') as archive:
                archive.extractall(f"NOVO_CAGED/{ano}/{mes}")
            
            # Atualizar histórico
            historico[arquivo_mais_recente] = data_modificacao
            with open(historico_csv, "w", encoding="utf-8-sig", newline='') as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(["Arquivo", "Data_Modificacao"])
                for arquivo, data in historico.items():
                    writer.writerow([arquivo, data])
            
            print(f"Processamento concluído e histórico atualizado para {arquivo_mais_recente}")

ftp.quit()
print("Conexão encerrada. Processamento concluído.")