import pyodbc
from azure_conexao_database import connect_to_sql_server   
def create_table():
    # Conectar ao banco de dados
    conn = connect_to_sql_server()
    if conn is None:
        print("Não foi possível estabelecer a conexão com o banco de dados.")
        return

    try:
        # Criar um cursor para executar comandos SQL
        cursor = conn.cursor()

        # Definir a query de criação da tabela
        create_table_query = """
        CREATE TABLE inadimplencia (
            data_base DATE,
            uf NVARCHAR(100),
            cliente NVARCHAR(255),
            ocupacao NVARCHAR(255),
            cnae_secao NVARCHAR(100),
            porte NVARCHAR(100),
            modalidade NVARCHAR(100),
            numero_de_operacoes INT,
            a_vencer_ate_90_dias DECIMAL(18, 2),
            carteira_ativa DECIMAL(18, 2),
            carteira_inadimplida_arrastada DECIMAL(18, 2),
            ativo_problematico DECIMAL(18, 2)
        );
        """

        # Executar a query
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela 'inadimplencia' criada com sucesso!")

    except pyodbc.Error as e:
        print("Erro ao criar a tabela:", e)

    finally:
        # Fechar a conexão
        conn.close()
        print("Conexão encerrada.")

# Exemplo de uso
if __name__ == "__main__":
    create_table()