import pyodbc
 
def connect_to_sql_server():
    server = 'tcp:inadimplencia.database.windows.net,1433'
    database = 'inadimplencia'
    username = 'clarissa'
    password = '@ndreyXUXU0410'  
 
    try:
        # Define the connection string
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
 
        print("Tentando estabelecer conexão com o banco de dados...")
        # Establish the connection
        conn = pyodbc.connect(connection_string)
        print("Conexão bem-sucedida!")
        return conn
 
    except pyodbc.Error as e:
        print("Erro ao conectar ao SQL Server:", e)
        return None
 
# Example usage
if __name__ == "__main__":
    connection = connect_to_sql_server()
    if connection:
        print("Conexão estabelecida. Pronto para realizar operações no banco de dados.")
        # Perform database operations here
        connection.close()
        print("Conexão encerrada com sucesso.")
    else:
        print("Não foi possível estabelecer a conexão com o banco de dados.")