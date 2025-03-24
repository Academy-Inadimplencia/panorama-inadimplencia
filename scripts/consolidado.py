import pandas as pd
import os

# Definindo o caminho dos arquivos parquet
file1 = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database\2014\inadimplencia_PF_2014.parquet'
file2 = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database\2014\inadimplencia_PJ_2014.parquet'
file3 = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database\2015\inadimplencia_PF_2015.parquet'
file4 = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database\2015\inadimplencia_PJ_2015.parquet'
file5 = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database\2016\inadimplencia_PF_2016.parquet'
file6 = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Database\2016\inadimplencia_PJ_2016.parquet'

# Lendo os arquivos parquet
df1 = pd.read_parquet(file1)
df2 = pd.read_parquet(file2)
df3 = pd.read_parquet(file3)
df4 = pd.read_parquet(file4)
df5 = pd.read_parquet(file5)
df6 = pd.read_parquet(file6)

# Consolidando os DataFrames
consolidated_df = pd.concat([df1, df2, df3, df4, df5, df6], ignore_index=True)

# Salvando o DataFrame consolidado em uma nova pasta
output_path = r'C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Consolidado'
output_file = os.path.join(output_path, 'consolidado.parquet')

# Criando a pasta se não existir
if not os.path.exists(output_path):
    os.makedirs(output_path)

# Salvando o arquivo consolidado
consolidated_df.to_parquet(output_file)

# Mostrando as 5 primeiras linhas do DataFrame consolidado
print(consolidated_df.head())