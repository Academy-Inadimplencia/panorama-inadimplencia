import pyodbc
from azure_conexao_database import connect_to_sql_server

def read_table():
    # Conectar ao banco de dados
    conn = connect_to_sql_server()
    if conn is None:
        print("Não foi possível estabelecer a conexão com o banco de dados.")
        return

    try:
        # Criar um cursor para executar comandos SQL
        cursor = conn.cursor()

        # Definir a query para ler os dados da tabela
        read_table_query = "SELECT * FROM inadimplencia;"

        # Executar a query
        cursor.execute(read_table_query)

        # Obter o cabeçalho da tabela
        headers = [column[0] for column in cursor.description]
        print("Cabeçalho da tabela:", headers)

        # Obter os resultados
        rows = cursor.fetchall()

        # Exibir os resultados
        for row in rows:
            print(row)

    except pyodbc.Error as e:
        print("Erro ao ler a tabela:", e)

    finally:
        # Fechar a conexão
        conn.close()
        print("Conexão encerrada.")

# Exemplo de uso
if __name__ == "__main__":
    read_table()
