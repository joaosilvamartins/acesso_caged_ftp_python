from ftplib import FTP
import os
import re
import py7zr  # Biblioteca para extrair arquivos .7z
import pandas as pd  # Para manipulação de dados
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

# Listar apenas diretórios de anos (YYYY)
anos = [item for item in ftp.nlst() if re.fullmatch(r"\d{4}", item)]
print(f"Pastas de anos encontradas: {anos}")

# Criar diretório local
os.makedirs("NOVO_CAGED", exist_ok=True)

for ano in anos:
    caminho_ano = f"{diretorio_base}{ano}/"
    try: 
        ftp.cwd(caminho_ano)
        meses = [item for item in ftp.nlst() if re.fullmatch(r"\d{6}", item)]
        print(f"Acessando {caminho_ano} -> Pastas de meses encontradas: {meses}")

        os.makedirs(f"NOVO_CAGED/{ano}", exist_ok=True)

        # Percorrer os meses
        for mes in meses:
            caminho_mes = f"{caminho_ano}{mes}/"
            try:
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
                    print(f"Download concluído: {caminho_local_7z}")

                    # Atualizar histórico
                    historico[arquivo_mais_recente] = data_modificacao
                    with open(historico_csv, "w", encoding="utf-8-sig", newline='') as f:
                        writer = csv.writer(f, delimiter=";")
                        writer.writerow(["Arquivo", "Data_Modificacao"])
                        for arquivo, data in historico.items():
                            writer.writerow([arquivo, data])

                    print(f"Histórico atualizado para {arquivo_mais_recente}")

                    # Descompactar o arquivo .7z
                    with py7zr.SevenZipFile(caminho_local_7z, mode='r') as archive:
                        archive.extractall(f"NOVO_CAGED/{ano}/{mes}")
                    
                    # Localizar o arquivo .txt extraído
                    arquivos_extraidos = os.listdir(f"NOVO_CAGED/{ano}/{mes}")
                    arquivo_txt = next((arq for arq in arquivos_extraidos if arq.endswith(".txt")), None)

                    if arquivo_txt:
                        caminho_txt = f"NOVO_CAGED/{ano}/{mes}/{arquivo_txt}"
                        print(f"Processando {caminho_txt}...")

                        dados = []

                        # Ler o arquivo .txt e coletar os dados
                        with open(caminho_txt, "r", encoding="latin-1") as txtfile:
                            reader = txtfile.readlines()

                            for linha in reader[1:]:  # Ignorar cabeçalho
                                colunas = linha.strip().split(";")
                                if len(colunas) > 16:
                                    cbo2002ocupacao = colunas[7]
                                    tipomovimentacao = colunas[16]

                                    if cbo2002ocupacao in ocupacoes_permitidas:
                                        dados.append([ano, mes, tipomovimentacao, cbo2002ocupacao, 1])

                        # Criar DataFrame e agrupar os dados
                        df = pd.DataFrame(dados, columns=["ano", "mês", "código movimentação", "código ocupação", "quantidade"])
                        df = df.groupby(["ano", "mês", "código movimentação", "código ocupação"]).sum().reset_index()

                        # Salvar como CSV
                        csv_saida_path = f"NOVO_CAGED/{ano}/{mes}/dados_extraidos.csv"
                        df.to_csv(csv_saida_path, index=False, encoding="utf-8-sig", sep=";")

                        print(f"Dados extraídos e salvos em {csv_saida_path}")

                else:
                    print(f"Nenhum arquivo .7z encontrado em {caminho_mes}")

                ftp.cwd(caminho_ano)  # Voltar ao diretório do ano

            except Exception as e:
                print(f"Erro ao acessar {caminho_mes}: {e}")

        ftp.cwd(diretorio_base)  # Voltar ao diretório base
    except Exception as e:
        print(f"Erro ao acessar {caminho_ano}: {e}")

ftp.quit()
print("Conexão encerrada. Processamento concluído.")