from ftplib import FTP
import os
import re
import py7zr  # Biblioteca para extrair arquivos .7z
import pandas as pd  # Para leitura do Excel

# Configurações do servidor FTP
host = "ftp.mtps.gov.br"
diretorio_base = "/pdet/microdados/NOVO CAGED/"

# Lista de ocupações permitidas
ocupacoes_permitidas = {"223505", "233125", "234415", "322205", "322210", "322215", 
                        "322220", "322230", "322235", "322245", "322250", "515110"}

# Carregar o mapeamento Código -> Descrição do Excel
layout_path = "Layout Não-identificado Novo Caged Movimentação.xlsx"
tabela_mov = pd.read_excel(layout_path, sheet_name="tipomovimentação", usecols=["Código", "Descrição"])
codigo_para_descricao = dict(zip(tabela_mov["Código"].astype(str), tabela_mov["Descrição"]))

# Conectar ao FTP
ftp = FTP(host)
ftp.encoding = "latin-1"
ftp.login()
ftp.cwd(diretorio_base)

# Listar apenas diretórios de anos (YYYY)
anos = [item for item in ftp.nlst() if re.fullmatch(r"\d{4}", item)]
print(f"Pastas de anos encontradas: {anos}")

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
                    arquivos.sort()
                    arquivo_mais_recente = arquivos[-1]

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

                        # Dicionário para contar as ocorrências de cada tipomovimentacao
                        contagem_movimentacao = {}

                        # Ler o arquivo .txt e contar os registros de interesse
                        with open(caminho_txt, "r", encoding="latin-1") as txtfile:
                            reader = txtfile.readlines()

                            for linha in reader[1:]:  # Ignorar cabeçalho
                                colunas = linha.strip().split(";")
                                if len(colunas) > 16:
                                    cbo2002ocupacao = colunas[7]
                                    tipomovimentacao = colunas[16]

                                    # Filtrar apenas as ocupações desejadas
                                    if cbo2002ocupacao in ocupacoes_permitidas:
                                        if tipomovimentacao in contagem_movimentacao:
                                            contagem_movimentacao[tipomovimentacao] += 1
                                        else:
                                            contagem_movimentacao[tipomovimentacao] = 1

                        # Criar o arquivo "dados_extraidos.txt"
                        txt_saida_path = f"NOVO_CAGED/{ano}/{mes}/dados_extraidos.txt"
                        with open(txt_saida_path, "w", encoding="utf-8") as out_txt:
                            for codigo, quantidade in contagem_movimentacao.items():
                                descricao = codigo_para_descricao.get(codigo, "Descrição não encontrada")
                                out_txt.write(f"{descricao}: {quantidade}\n")

                        print(f"Dados extraídos e salvos em {txt_saida_path}")

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
