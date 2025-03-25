import os
import pandas as pd
import random
import glob
from pathlib import Path

def criar_amostra_consolidada():
    # Definir o caminho raiz
    caminho_raiz = r"C:\Users\AU355EE\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database\2014"
    caminho_destino = r"C:\Users\AU355EE\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Amostras"

    # Garantir que a pasta de destino exista
    Path(caminho_destino).mkdir(parents=True, exist_ok=True)
    print(f"Pasta de destino criada ou já existe: {caminho_destino}")

    # Lista para armazenar todos os dataframes de amostras
    amostras_consolidadas = []

    # Iterar sobre os anos de 2014 a 2024
    for ano in range(2014, 2025):
        pasta_ano = os.path.join(caminho_raiz, str(ano))
        print(f"Verificando pasta do ano: {ano}")

        # Verificar se a pasta do ano existe
        if not os.path.exists(pasta_ano):
            print(f"Pasta para o ano {ano} não encontrada. Pulando...")
            continue

        # Encontrar todos os arquivos parquet na pasta do ano
        arquivos_parquet = glob.glob(os.path.join(pasta_ano, "*.parquet"))
        print(f"Arquivos encontrados para o ano {ano}: {len(arquivos_parquet)}")

        for arquivo in arquivos_parquet:
            print(f"Processando arquivo: {arquivo}")

            try:
                # Ler o arquivo parquet
                df = pd.read_parquet(arquivo)
                print(f"Arquivo {arquivo} lido com sucesso. Total de linhas: {len(df)}")

                # Verificar se há pelo menos 100 linhas (após o cabeçalho)
                if len(df) <= 100:
                    print(f"Arquivo {arquivo} possui menos de 100 linhas. Adicionando todas as linhas à amostra.")
                    amostras_consolidadas.append(df)
                else:
                    # Selecionar aleatoriamente 100 linhas
                    amostra = df.sample(n=100, random_state=42)
                    amostras_consolidadas.append(amostra)
                    print(f"Selecionadas 100 linhas aleatórias do arquivo {arquivo}")

            except Exception as e:
                print(f"Erro ao processar arquivo {arquivo}: {str(e)}")

    # Verificar se temos amostras para consolidar
    if not amostras_consolidadas:
        print("Nenhuma amostra foi coletada. Verifique os caminhos e arquivos.")
        return

    # Consolidar todas as amostras em um único DataFrame
    df_consolidado = pd.concat(amostras_consolidadas, ignore_index=True)
    print(f"Total de amostras consolidadas: {len(df_consolidado)}")

    # Salvar o DataFrame consolidado como um arquivo parquet
    arquivo_destino = os.path.join(caminho_destino, "inadimplencia_amostra.parquet")
    df_consolidado.to_parquet(arquivo_destino, index=False)

    print(f"\nProcessamento concluído!")
    print(f"Arquivo consolidado salvo em: {arquivo_destino}")
    print(f"Total de linhas no arquivo consolidado: {len(df_consolidado)}")

if __name__ == "__main__":
    criar_amostra_consolidada()
