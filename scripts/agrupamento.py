import pandas as pd
import os

def processar_csv(lista_arquivos):
    for arquivo in lista_arquivos:
        if not os.path.exists(arquivo):
            print(f"Arquivo {arquivo} não encontrado. Pulando...")
            continue

        # Lê o CSV
        df = pd.read_csv(arquivo, sep=";" dtype=str)  # Lê como string para evitar problemas de tipo

        # Verifica se as colunas necessárias existem
        colunas_necessarias = ['E', 'I']
        if not all(col in df.columns for col in colunas_necessarias):
            print(f"Arquivo {arquivo} não contém todas as colunas necessárias. Pulando...")
            continue

        # Aplica os filtros
        df_filtrado = df[(df['E'] == 'PJ') & (df['I'] == 'PJ - Grande')]

        # Gera o novo nome do arquivo
        nome_original = os.path.splitext(os.path.basename(arquivo))[0]
        novo_nome = f"{nome_original}_PJ_PJ-Grande.csv"

        # Salva o novo arquivo CSV
        df_filtrado.to_csv(novo_nome, index=False, encoding='utf-8-sig')

        print(f"Arquivo salvo: {novo_nome}")

# Lista de arquivos CSV para processar
lista_csv = [r""]  # Substitua pelos seus arquivos

processar_csv(lista_csv) # teste pra subir