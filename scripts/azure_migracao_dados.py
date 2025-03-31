import pandas as pd
import pyodbc
from azure_conexao_database import connect_to_sql_server

def migrate_parquet_to_database(parquet_file_path):
    # Conectar ao banco de dados
    conn = connect_to_sql_server()
    if conn is None:
        print("Não foi possível estabelecer a conexão com o banco de dados.")
        return

    try:
        # Ler o arquivo Parquet
        df = pd.read_parquet(parquet_file_path)
        print(f"Dados carregados do arquivo {parquet_file_path}:")
        print(df.head())

        # Criar um cursor para executar comandos SQL
        cursor = conn.cursor()

        # Inserir os dados no banco de dados
        for index, row in df.iterrows():
            insert_query = """
            INSERT INTO inadimplencia (coluna1, coluna2, coluna3, ...) 
            VALUES (?, ?, ?, ...);
            """
            cursor.execute(insert_query, row['coluna1'], row['coluna2'], row['coluna3'], ...)

        # Confirmar as alterações
        conn.commit()
        print("Dados migrados com sucesso para o banco de dados.")

    except pyodbc.Error as e:
        print("Erro ao migrar os dados:", e)

    finally:
        # Fechar a conexão
        conn.close()
        print("Conexão encerrada.")

# Exemplo de uso
if __name__ == "__main__":
    parquet_file_path = r"C:\Users\WM328RC\OneDrive - EY\Panorama Inadimplência - Projeto Academy\Database\Database BCB\Ouro\Consolidado\consolidado_2020_a_2024.parquet"
    migrate_parquet_to_database(parquet_file_path)
