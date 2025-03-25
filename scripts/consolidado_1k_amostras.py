import os
import pandas as pd
import random
import logging

# Configuração do log
logging.basicConfig(
    filename=r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Consolidado\consolidado_log.txt',
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Caminhos de diretório
base_path = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database'
consolidado_path = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Consolidado'

# Função para coletar uma amostra aleatória de 1000 linhas de cada arquivo Parquet
def consolidar_amostra():
    all_samples = []  # Lista para armazenar as amostras de todos os arquivos
    anos = range(2014, 2025)  # Intervalo de anos
    total_arquivos = len(anos) * 2  # 2 arquivos (PF e PJ) por ano

    logging.info('Início do processo de consolidação de amostras.')

    for ano in anos:
        # Caminho da pasta do ano
        ano_path = os.path.join(base_path, str(ano))

        # Nomes dos arquivos Parquet
        arquivos = [
            f'inadimplencia_PF_{ano}.parquet',
            f'inadimplencia_PJ_{ano}.parquet'
        ]
        
        for arquivo in arquivos:
            arquivo_path = os.path.join(ano_path, arquivo)
            
            logging.info(f'Processando o arquivo: {arquivo_path}')
            
            # Verificar se o arquivo existe
            if os.path.exists(arquivo_path):
                # Ler o arquivo Parquet
                df = pd.read_parquet(arquivo_path)
                
                # Selecionar 1000 linhas aleatórias
                sample = df.sample(n=1000, random_state=random.randint(0, 10000))
                
                # Adicionar à lista de amostras
                all_samples.append(sample)

                logging.info(f'Amostra de 1000 linhas do arquivo {arquivo} foi coletada.')
            else:
                logging.warning(f'Arquivo não encontrado: {arquivo_path}')
    
    # Concatenar todas as amostras
    df_consolidado = pd.concat(all_samples, ignore_index=True)
    logging.info('Todas as amostras foram coletadas e consolidadas.')

    # Caminho para salvar o arquivo consolidado
    consolidado_file = os.path.join(consolidado_path, 'consolidado_amostra.parquet')
    
    # Salvar o DataFrame consolidado em um arquivo Parquet
    df_consolidado.to_parquet(consolidado_file, index=False)
    logging.info(f'Arquivo consolidado salvo em: {consolidado_file}')
    
    # Print final para informar ao usuário
    print(f'Arquivo consolidado com 1000 amostras de cada arquivo foi salvo em: {consolidado_file}')
    logging.info('Processo de consolidação concluído.')

# Chamar a função de consolidação
consolidar_amostra()
