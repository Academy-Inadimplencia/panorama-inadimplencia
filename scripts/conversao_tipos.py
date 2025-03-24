import pandas as pd
import os

# Cole aqui apenas a função converter_tipos_dados e o código principal abaixo:

def converter_tipos_dados(caminho_arquivo):
    """
    Converte os tipos de dados de um arquivo parquet de inadimplência.
    
    Parâmetros:
    caminho_arquivo (str): Caminho para o arquivo parquet a ser processado
    
    Retorna:
    DataFrame: DataFrame com os tipos de dados convertidos
    """
    print(f"Carregando arquivo: {caminho_arquivo}")
    df = pd.read_parquet(caminho_arquivo)
    
    print("\nInformações do DataFrame antes da conversão:")
    print(f"Linhas: {len(df)}, Colunas: {df.shape[1]}")
    print("Tipos de dados antes da conversão:")
    print(df.dtypes)
    
    # Converter coluna de data
    if 'data_base' in df.columns:
        df['data_base'] = pd.to_datetime(df['data_base'], errors='coerce')
        print("\nColuna data_base convertida para datetime")
    
    # Colunas para converter para string
    colunas_string = ['uf', 'cliente', 'ocupacao', 'cnae_secao', 'porte', 'modalidade', 
                      'numero_de_operacoes', 'tipo_cliente']
    
    # Converter colunas para string
    print("\nConvertendo colunas para string:")
    colunas_string_convertidas = []
    for coluna in colunas_string:
        if coluna in df.columns:
            df[coluna] = df[coluna].astype('string')
            colunas_string_convertidas.append(coluna)
    print(f"Colunas convertidas para string: {', '.join(colunas_string_convertidas)}")
    
    # Colunas para converter para valores numéricos
    colunas_numericas = ['a_vencer_ate_90_dias', 'carteira_ativa', 
                         'carteira_inadimplida_arrastada', 'ativo_problematico']
    
    # Converter colunas para valores numéricos
    print("\nConvertendo colunas para valores numéricos:")
    colunas_numericas_convertidas = []
    for coluna in colunas_numericas:
        if coluna in df.columns:
            # Tratar possíveis strings com vírgulas
            if df[coluna].dtype == 'object' or df[coluna].dtype == 'string':
                df[coluna] = df[coluna].astype(str).str.replace(',', '.', regex=False)
            
            # Converter para float
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
            colunas_numericas_convertidas.append(coluna)
    print(f"Colunas convertidas para valores numéricos: {', '.join(colunas_numericas_convertidas)}")
    
    # Verificar os tipos de dados após a conversão
    print("\nTipos de dados após a conversão:")
    print(df.dtypes)
    
    # Salvar o DataFrame com os tipos atualizados
    print(f"\nSalvando DataFrame com tipos atualizados em {caminho_arquivo}...")
    df.to_parquet(caminho_arquivo, index=False)
    print("Conversão de tipos concluída com sucesso!")
    
    return df

arquivo_consolidado = r"C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Consolidado\consolidado_2020_a_2024.parquet"

# Executar a conversão diretamente (sem o bloco if _name_)
df_convertido = converter_tipos_dados(arquivo_consolidado)

# Mostrar as primeiras linhas do DataFrame final
print("\nPrimeiras 5 linhas do DataFrame final:")
print(df_convertido.head())