import pandas as pd

# Carregando o arquivo parquet consolidado
caminho_arquivo = r"C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Consolidado\consolidado.parquet"
df = pd.read_parquet(caminho_arquivo)

# Verificando os tipos de dados atuais
print("Tipos de dados antes da conversão:")
print(df.dtypes)

# Convertendo coluna de data para datetime (assumindo que seja 'data_referencia')
if 'data_base' in df.columns:
    df['data_base'] = pd.to_datetime(df['data_base'], errors='coerce')
    print("Coluna data_base convertida para datetime")

# Lista de colunas para converter para string
colunas_string = ['uf', 'cliente', 'ocupacao', 'cnae_secao', 'porte', 'modalidade', 'numero_de_operacoes']

# Converter cada coluna string
for coluna in colunas_string:
    if coluna in df.columns:
        df[coluna] = df[coluna].astype('string')
        print(f"Coluna {coluna} convertida para string")

# Lista de colunas para converter para valores numéricos (float)
colunas_numericas = ['a_vencer_ate_90_dias', 'carteira_ativa', 'carteira_inadimplida_arrastada', 'ativo_problematico']

# Converter cada coluna numérica
for coluna in colunas_numericas:
    if coluna in df.columns:
        # Remover possíveis caracteres não numéricos (como ',' no lugar de '.' para decimais)
        if df[coluna].dtype == 'object' or df[coluna].dtype == 'string':
            df[coluna] = df[coluna].str.replace(',', '.', regex=False)
        # Converter para float
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
        print(f"Coluna {coluna} convertida para float")

# Verificando se a conversão foi bem-sucedida
print("\nTipos de dados após a conversão:")
print(df.dtypes)

# Salvando o DataFrame com os tipos atualizados de volta para parquet
df.to_parquet(caminho_arquivo, index=False)
print("\nArquivo salvo com os tipos de dados atualizados!")