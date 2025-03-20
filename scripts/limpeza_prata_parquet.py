import pandas as pd
import os
import glob
import time
import logging
from datetime import datetime

# ===== CONFIGURAÇÃO DE LOGS =====
# Configurar logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"processamento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Configurar o logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Para mostrar logs no console também
    ]
)
logger = logging.getLogger()

# ===== CONFIGURAÇÕES (ajuste conforme necessário) =====
# Caminho onde estão seus arquivos CSV
pasta_entrada = "bases/2014"  
# Caminho onde serão salvos os arquivos Parquet
pasta_saida = "resultados/2014"
# Ano que deseja processar (formato: YYYY)
ano_a_processar = "2014"  # Ajustado para corresponder à pasta
# Padrão dos nomes de arquivos
padrao_arquivos = f"*.csv"  # Modificado para pegar qualquer arquivo CSV na pasta
# Tamanho dos chunks para processamento (ajuste conforme memória disponível)
tamanho_chunk = 100000

# ===== CONFIGURAÇÃO DE COLUNAS =====
# Colunas para EXCLUIR (ajuste conforme necessário)
# Deixe vazio para manter todas as colunas

colunas_para_excluir = [
    'tcb',                              # Exemplo de coluna para excluir
    'sr',                               # Exemplo de coluna para excluir
    # 'indexador',                        # Exemplo de coluna para excluir
    # 'origem',                           # Exemplo de coluna para excluir
    # 'cnae_subclasse',                   # Exemplo de coluna para excluir
    'a_vencer_de_91_ate_360_dias',      # Exemplo de coluna para excluir
    'a_vencer_de_361_ate_1080_dias',    # Exemplo de coluna para excluir
    'a_vencer_de_1081_ate_1800_dias',   # Exemplo de coluna para excluir
    'a_vencer_de_1801_ate_5400_dias',   # Exemplo de coluna para excluir
    'a_vencer_acima_de_5400_dias',      # Exemplo de coluna para excluir
    'vencido_acima_de_15_dias'          # Exemplo de coluna para excluir
]

# ===== FUNÇÕES =====

def processar_arquivo(caminho_arquivo):
    """Processa um arquivo CSV e retorna DataFrames para PF e PJ"""
    nome_arquivo = os.path.basename(caminho_arquivo)
    logger.info(f"Iniciando processamento do arquivo: {nome_arquivo}")
    
    # Verificar tamanho do arquivo
    tamanho_mb = os.path.getsize(caminho_arquivo) / (1024 * 1024)
    logger.info(f"Tamanho do arquivo: {tamanho_mb:.2f} MB")
    
    # DataFrames para armazenar resultados de PF e PJ
    df_pf_completo = pd.DataFrame()
    df_pj_completo = pd.DataFrame()
    
    # Contador de chunks processados
    contador_chunks = 0
    linhas_totais = 0
    linhas_pf = 0
    linhas_pj = 0
    
    inicio_arquivo = time.time()
    
    try:
        # Processar em chunks para economizar memória
        for chunk in pd.read_csv(caminho_arquivo, sep=';', chunksize=tamanho_chunk, 
                              encoding='utf-8', low_memory=False):
            
            inicio_chunk = time.time()
            contador_chunks += 1
            linhas_chunk = len(chunk)
            linhas_totais += linhas_chunk
            
            logger.info(f"Processando chunk {contador_chunks} do arquivo {nome_arquivo} - {linhas_chunk} linhas")
            
            # Mostrar as colunas disponíveis no primeiro chunk
            if contador_chunks == 1:
                logger.info(f"Colunas disponíveis: {', '.join(chunk.columns)}")
                logger.info(f"Colunas a excluir que existem no arquivo: {[col for col in colunas_para_excluir if col in chunk.columns]}")
            
            # Excluir as colunas indesejadas (se configuradas)
            if colunas_para_excluir:
                # Filtrar apenas as colunas que realmente existem no dataframe
                colunas_a_remover = [col for col in colunas_para_excluir if col in chunk.columns]
                if colunas_a_remover:
                    df_parcial = chunk.drop(columns=colunas_a_remover, errors='ignore').copy()
                    logger.debug(f"Removidas {len(colunas_a_remover)} colunas")
                else:
                    df_parcial = chunk.copy()
            else:
                df_parcial = chunk.copy()
            
            # Separar em PF e PJ
            if 'cliente' in df_parcial.columns:
                df_pf = df_parcial[df_parcial['cliente'] == 'PF'].copy()
                df_pj = df_parcial[df_parcial['cliente'] == 'PJ'].copy()
                
                linhas_pf_chunk = len(df_pf)
                linhas_pj_chunk = len(df_pj)
                linhas_pf += linhas_pf_chunk
                linhas_pj += linhas_pj_chunk
                
                logger.info(f"Chunk {contador_chunks}: {linhas_pf_chunk} registros PF, {linhas_pj_chunk} registros PJ")
                
                # Adicionar aos DataFrames completos
                if not df_pf.empty:
                    df_pf_completo = pd.concat([df_pf_completo, df_pf], ignore_index=True)
                
                if not df_pj.empty:
                    df_pj_completo = pd.concat([df_pj_completo, df_pj], ignore_index=True)
            else:
                logger.warning(f"Coluna 'cliente' não encontrada no chunk {contador_chunks} do arquivo {nome_arquivo}")
            
            tempo_chunk = time.time() - inicio_chunk
            logger.info(f"Chunk {contador_chunks} processado em {tempo_chunk:.2f} segundos")
        
        logger.info(f"Arquivo {nome_arquivo} processado com sucesso em {time.time() - inicio_arquivo:.2f} segundos")
        logger.info(f"Total de linhas lidas: {linhas_totais}")
        logger.info(f"Total de registros PF: {linhas_pf}")
        logger.info(f"Total de registros PJ: {linhas_pj}")
        
        memoria_pf = df_pf_completo.memory_usage(deep=True).sum() / (1024 * 1024)
        memoria_pj = df_pj_completo.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(f"Uso de memória - DataFrame PF: {memoria_pf:.2f} MB, DataFrame PJ: {memoria_pj:.2f} MB")
        
        return df_pf_completo, df_pj_completo
        
    except Exception as e:
        logger.error(f"Erro ao processar o arquivo {nome_arquivo}: {str(e)}")
        raise

def main():
    """Função principal que orquestra o processamento"""
    inicio_total = time.time()
    logger.info(f"=== Iniciando processamento dos arquivos de {ano_a_processar} ===")
    logger.info(f"Diretório de entrada: {pasta_entrada}")
    logger.info(f"Diretório de saída: {pasta_saida}")
    
    # Criar pasta de saída se não existir
    os.makedirs(pasta_saida, exist_ok=True)
    
    # Encontrar arquivos que correspondem ao padrão
    caminho_busca = os.path.join(pasta_entrada, padrao_arquivos)
    arquivos = glob.glob(caminho_busca)
    
    if not arquivos:
        logger.warning(f"Nenhum arquivo encontrado com o padrão {padrao_arquivos}")
        return
    
    logger.info(f"Encontrados {len(arquivos)} arquivos para processar:")
    for i, arq in enumerate(arquivos, 1):
        logger.info(f"  {i}. {os.path.basename(arq)}")
    
    # DataFrames para armazenar todos os resultados
    df_pf_completo = pd.DataFrame()
    df_pj_completo = pd.DataFrame()
    
    arquivos_com_erro = []
    arquivos_processados = 0
    
    # Processar cada arquivo
    for i, arquivo in enumerate(arquivos, 1):
        nome_arquivo = os.path.basename(arquivo)
        logger.info(f"\n=== Processando arquivo {i}/{len(arquivos)}: {nome_arquivo} ===")
        
        try:
            inicio_proc = time.time()
            df_pf, df_pj = processar_arquivo(arquivo)
            
            # Adicionar aos DataFrames completos
            if not df_pf.empty:
                logger.info(f"Concatenando {len(df_pf)} registros PF ao DataFrame principal")
                df_pf_completo = pd.concat([df_pf_completo, df_pf], ignore_index=True)
            else:
                logger.info("Nenhum registro PF encontrado neste arquivo")
            
            if not df_pj.empty:
                logger.info(f"Concatenando {len(df_pj)} registros PJ ao DataFrame principal")
                df_pj_completo = pd.concat([df_pj_completo, df_pj], ignore_index=True)
            else:
                logger.info("Nenhum registro PJ encontrado neste arquivo")
            
            tempo_proc = time.time() - inicio_proc
            logger.info(f"Arquivo {nome_arquivo} processado em {tempo_proc:.2f} segundos")
            arquivos_processados += 1
            
        except Exception as e:
            logger.error(f"FALHA ao processar o arquivo {nome_arquivo}: {str(e)}")
            arquivos_com_erro.append(nome_arquivo)
            logger.info("Continuando com o próximo arquivo...")
    
    logger.info("\n=== Resumo do processamento ===")
    logger.info(f"Total de arquivos: {len(arquivos)}")
    logger.info(f"Arquivos processados com sucesso: {arquivos_processados}")
    logger.info(f"Arquivos com erro: {len(arquivos_com_erro)}")
    
    if arquivos_com_erro:
        logger.info("Arquivos que falharam:")
        for arq in arquivos_com_erro:
            logger.info(f"  - {arq}")
    
    # Salvar resultados em arquivos Parquet
    if not df_pf_completo.empty:
        logger.info(f"\n=== Salvando arquivo Parquet para PF ===")
        logger.info(f"Total de registros PF: {len(df_pf_completo)}")
        memoria_pf = df_pf_completo.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(f"Uso de memória antes da compressão: {memoria_pf:.2f} MB")
        
        caminho_pf = os.path.join(pasta_saida, f"inadimplencia_PF_{ano_a_processar}.parquet")
        inicio_salvar = time.time()
        df_pf_completo.to_parquet(caminho_pf, index=False)
        tempo_salvar = time.time() - inicio_salvar
        
        tamanho_mb = os.path.getsize(caminho_pf) / (1024 * 1024)
        logger.info(f"Arquivo PF salvo em {caminho_pf}")
        logger.info(f"Tempo para salvar: {tempo_salvar:.2f} segundos")
        logger.info(f"Tamanho do arquivo Parquet: {tamanho_mb:.2f} MB")
        logger.info(f"Taxa de compressão: {memoria_pf/tamanho_mb:.2f}x")
    else:
        logger.warning("Nenhum dado de Pessoa Física encontrado para salvar.")
    
    if not df_pj_completo.empty:
        logger.info(f"\n=== Salvando arquivo Parquet para PJ ===")
        logger.info(f"Total de registros PJ: {len(df_pj_completo)}")
        memoria_pj = df_pj_completo.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(f"Uso de memória antes da compressão: {memoria_pj:.2f} MB")
        
        caminho_pj = os.path.join(pasta_saida, f"inadimplencia_PJ_{ano_a_processar}.parquet")
        inicio_salvar = time.time()
        df_pj_completo.to_parquet(caminho_pj, index=False)
        tempo_salvar = time.time() - inicio_salvar
        
        tamanho_mb = os.path.getsize(caminho_pj) / (1024 * 1024)
        logger.info(f"Arquivo PJ salvo em {caminho_pj}")
        logger.info(f"Tempo para salvar: {tempo_salvar:.2f} segundos")
        logger.info(f"Tamanho do arquivo Parquet: {tamanho_mb:.2f} MB")
        logger.info(f"Taxa de compressão: {memoria_pj/tamanho_mb:.2f}x")
    else:
        logger.warning("Nenhum dado de Pessoa Jurídica encontrado para salvar.")
    
    tempo_total = time.time() - inicio_total
    logger.info(f"\n=== Processamento concluído em {tempo_total:.2f} segundos ({tempo_total/60:.2f} minutos) ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Erro fatal no processamento: {str(e)}")
        raise