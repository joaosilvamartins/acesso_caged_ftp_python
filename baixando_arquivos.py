from ftplib import FTP
import os
import re

# Configurações do servidor FTP
host = "ftp.mtps.gov.br"
diretorio_base = "/pdet/microdados/NOVO CAGED/"

# Conectando ao servidor FTP
ftp = FTP(host)
ftp.encoding = "latin-1"  # Define a codificação para evitar erros de Unicode
ftp.login()  # Acesso anônimo

# Acessando a pasta "NOVO CAGED"
ftp.cwd(diretorio_base)

# Listar arquivos e pastas dentro de "NOVO CAGED"
itens = ftp.nlst()

# Filtrar apenas os diretórios que são anos (YYYY)
anos = [item for item in itens if re.fullmatch(r"\d{4}", item)]
print(f"Pastas de anos encontradas: {anos}")

# Criar diretório local para salvar os arquivos, se não existir
os.makedirs("NOVO_CAGED", exist_ok=True)

# Percorrer os anos
for ano in anos:
    caminho_ano = f"{diretorio_base}{ano}/"
    try:
        ftp.cwd(caminho_ano)
        meses = [item for item in ftp.nlst() if re.fullmatch(r"\d{6}", item)]  # Apenas pastas no formato YYYYMM
        print(f"Acessando {caminho_ano} -> Pastas de meses encontradas: {meses}")

        # Criar diretório local para o ano
        os.makedirs(f"NOVO_CAGED/{ano}", exist_ok=True)

        # Percorrer os meses
        for mes in meses:
            caminho_mes = f"{caminho_ano}{mes}/"
            try:
                ftp.cwd(caminho_mes)
                arquivos = [arq for arq in ftp.nlst() if arq.endswith(".7z")]  # Apenas arquivos .7z
                
                if arquivos:
                    # Selecionar o arquivo mais recente (o último na lista ordenada)
                    arquivos.sort()  # A ordenação é feita pelo nome do arquivo
                    arquivo_mais_recente = arquivos[-1]

                    # Criar diretório local para o mês
                    os.makedirs(f"NOVO_CAGED/{ano}/{mes}", exist_ok=True)

                    # Caminho local do arquivo
                    caminho_local = f"NOVO_CAGED/{ano}/{mes}/{arquivo_mais_recente}"
                    print(f"Baixando: {arquivo_mais_recente} para {caminho_local}")

                    with open(caminho_local, "wb") as f:
                        ftp.retrbinary(f"RETR {arquivo_mais_recente}", f.write)
                    print(f"Download concluído: {caminho_local}")

                else:
                    print(f"Nenhum arquivo .7z encontrado em {caminho_mes}")

                # Retornar ao diretório do ano
                ftp.cwd(caminho_ano)

            except Exception as e:
                print(f"Erro ao acessar {caminho_mes}: {e}")

        # Retornar ao diretório base
        ftp.cwd(diretorio_base)

    except Exception as e:
        print(f"Erro ao acessar {caminho_ano}: {e}")

# Fechando a conexão
ftp.quit()
print("Conexão encerrada.")
