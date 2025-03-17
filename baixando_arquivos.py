from ftplib import FTP
import os
import re
import py7zr  # Biblioteca para extrair arquivos .7z
import csv

# Configurações do servidor FTP
host = "ftp.mtps.gov.br"
diretorio_base = "/pdet/microdados/NOVO CAGED/"

# Conectar ao FTP
ftp = FTP(host)
ftp.encoding = "latin-1"  # Define a codificação para evitar erros de Unicode
ftp.login()  # Acesso anônimo
ftp.cwd(diretorio_base)

# Listar apenas os diretórios que representam anos (YYYY)
itens = ftp.nlst()
anos = [item for item in itens if re.fullmatch(r"\d{4}", item)]
print(f"Pastas de anos encontradas: {anos}")

# Criar diretório base local
os.makedirs("NOVO_CAGED", exist_ok=True)

# Percorrer os anos
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
                arquivos = [arq for arq in ftp.nlst() if arq.endswith(".7z")]

                if arquivos:
                    arquivos.sort()  # Ordena os arquivos por nome
                    arquivo_mais_recente = arquivos[-1]

                    # Criar diretório para o mês
                    os.makedirs(f"NOVO_CAGED/{ano}/{mes}", exist_ok=True)
                    caminho_local_7z = f"NOVO_CAGED/{ano}/{mes}/{arquivo_mais_recente}"

                    # Baixar o arquivo .7z
                    print(f"Baixando: {arquivo_mais_recente} para {caminho_local_7z}")
                    with open(caminho_local_7z, "wb") as f:
                        ftp.retrbinary(f"RETR {arquivo_mais_recente}", f.write)
                    print(f"Download concluído: {caminho_local_7z}")

                    # Descompactar o arquivo .7z
                    with py7zr.SevenZipFile(caminho_local_7z, mode='r') as archive:
                        archive.extractall(f"NOVO_CAGED/{ano}/{mes}")
                    
                    # Localizar o arquivo .txt extraído
                    arquivos_extraidos = os.listdir(f"NOVO_CAGED/{ano}/{mes}")
                    arquivo_txt = next((arq for arq in arquivos_extraidos if arq.endswith(".txt")), None)

                    if arquivo_txt:
                        caminho_txt = f"NOVO_CAGED/{ano}/{mes}/{arquivo_txt}"
                        print(f"Processando {caminho_txt}...")

                        # Nome do arquivo CSV específico para o mês
                        csv_file_path = f"NOVO_CAGED/{ano}/{mes}/dados_extraidos{mes}.csv"

                        # Ler o arquivo .txt e extrair os campos necessários
                        with open(caminho_txt, "r", encoding="latin-1") as txtfile, open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
                            reader = txtfile.readlines()
                            writer = csv.writer(csvfile)

                            # Escrever cabeçalhos no CSV
                            writer.writerow(["CBO2002 Ocupação", "Tipo Movimentação"])

                            # Ler cada linha e extrair os dados necessários
                            for linha in reader[1:]:  # Ignorar cabeçalho
                                colunas = linha.strip().split(";")
                                if len(colunas) > 16:
                                    cbo2002ocupacao = colunas[7]
                                    tipomovimentacao = colunas[16]
                                    writer.writerow([cbo2002ocupacao, tipomovimentacao])

                        print(f"Dados extraídos de {arquivo_txt} e salvos em {csv_file_path}")

                else:
                    print(f"Nenhum arquivo .7z encontrado em {caminho_mes}")

                ftp.cwd(caminho_ano)  # Voltar ao diretório do ano

            except Exception as e:
                print(f"Erro ao acessar {caminho_mes}: {e}")

        ftp.cwd(diretorio_base)  # Voltar ao diretório base

    except Exception as e:
        print(f"Erro ao acessar {caminho_ano}: {e}")

# Fechar conexão com FTP
ftp.quit()
print("Conexão encerrada. Processamento concluído.")
