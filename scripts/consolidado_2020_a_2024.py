import pandas as pd
import os
import glob
from tqdm import tqdm

# Cole aqui apenas a função consolidar_arquivos_inadimplencia e o código principal abaixo:

def consolidar_arquivos_inadimplencia(caminho_base, pasta_saida, anos=range(2020, 2025)):
    """
    Consolida arquivos de inadimplência de PF e PJ para vários anos em um único arquivo parquet.
    
    Parâmetros:
    caminho_base (str): Caminho base onde estão os diretórios dos anos
    pasta_saida (str): Caminho onde o arquivo consolidado será salvo
    anos (range): Range dos anos a serem processados
    
    Retorna:
    DataFrame: DataFrame consolidado
    """
    print(f"Iniciando consolidação de dados de inadimplência para os anos {min(anos)} a {max(anos)}")
    
    # Lista para armazenar todos os DataFrames
    dfs = []
    
    # Total de arquivos para barra de progresso
    total_arquivos = len(anos) * 2  # PF e PJ para cada ano
    
    with tqdm(total=total_arquivos, desc="Processando arquivos") as pbar:
        # Para cada ano no intervalo especificado
        for ano in anos:
            # Caminhos para os arquivos PF e PJ do ano
            caminho_ano = os.path.join(caminho_base, str(ano))
            
            # Verificar se o diretório do ano existe
            if not os.path.exists(caminho_ano):
                print(f"Diretório para o ano {ano} não encontrado. Pulando.")
                pbar.update(2)  # Atualiza a barra para os 2 arquivos que seriam processados
                continue
            
            # Procurar arquivos de PF e PJ para o ano usando glob
            padrao_arquivo = os.path.join(caminho_ano, f"inadimplencia_P*_{ano}.parquet")
            arquivos_ano = glob.glob(padrao_arquivo)
            
            if not arquivos_ano:
                print(f"Nenhum arquivo de inadimplência encontrado para o ano {ano}. Pulando.")
                pbar.update(2)
                continue
            
            # Processar cada arquivo encontrado
            for arquivo in arquivos_ano:
                try:
                    # Obter o tipo (PF ou PJ) do nome do arquivo
                    tipo = "PF" if "PF" in os.path.basename(arquivo) else "PJ"
                    
                    # Ler o arquivo parquet
                    df = pd.read_parquet(arquivo)
                    
                    # Adicionar coluna para identificar o tipo (PF ou PJ) se ainda não existir
                    if 'tipo_cliente' not in df.columns:
                        df['tipo_cliente'] = tipo
                    
                    # Adicionar o DataFrame à lista
                    dfs.append(df)
                    
                    # Informações sobre o arquivo processado
                    print(f"Processado: {os.path.basename(arquivo)} - {len(df)} linhas")
                    
                    # Atualizar a barra de progresso
                    pbar.update(1)
                    
                except Exception as e:
                    print(f"Erro ao processar o arquivo {arquivo}: {str(e)}")
                    pbar.update(1)
    
    # Verificar se algum DataFrame foi carregado
    if not dfs:
        print("Nenhum arquivo foi processado com sucesso. Abortando.")
        return None
    
    # Consolidar todos os DataFrames
    print("\nConcatenando todos os DataFrames...")
    df_consolidado = pd.concat(dfs, ignore_index=True)
    print(f"DataFrame consolidado: {len(df_consolidado)} linhas, {df_consolidado.shape[1]} colunas")
    
    # Criar pasta de saída se não existir
    if not os.path.exists(pasta_saida):
        os.makedirs(pasta_saida)
        print(f"Pasta de saída criada: {pasta_saida}")
    
    # Caminho do arquivo de saída
    arquivo_saida = os.path.join(pasta_saida, 'consolidado_2020_a_2024.parquet')
    
    # Salvar o DataFrame consolidado
    print(f"Salvando DataFrame consolidado em {arquivo_saida}...")
    df_consolidado.to_parquet(arquivo_saida, index=False)
    print("Consolidação concluída com sucesso!")
    
    return df_consolidado

caminho_base = r"C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database"
pasta_saida = r"C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Consolidado"

# Executar a consolidação diretamente (sem o bloco if _name_)
df_consolidado = consolidar_arquivos_inadimplencia(caminho_base, pasta_saida)