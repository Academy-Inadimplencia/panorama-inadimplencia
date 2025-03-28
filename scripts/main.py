from fastapi import FastAPI
import os
import pyodbc
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

driver = os.getenv("ODBC Driver 18 for SQL Server")
server = os.getenv("tcp:inadimplencia.database.windows.net,1433")
database = os.getenv("inadimplencia")
username = os.getenv("clarissa")
password = os.getenv("@ndreyXUXU0410")

def get_conexao():
    logger.info("Establishing database connection...")
    return pyodbc.connect(
        f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"
    )

@app.get("/dados")
async def obter_dados():
    logger.info("Endpoint '/dados' was called.")
    try:
        conn = get_conexao()
        cursor = conn.cursor()
        logger.info("Executing SQL query...")
        cursor.execute("SELECT TOP 10 * FROM inadimplencia")
        resultados = cursor.fetchall()
        logger.info("Query executed successfully. Fetching results...")
        dados = [
            dict(zip([column[0] for column in cursor.description], row))
            for row in resultados
        ]
        logger.info("Data fetched successfully.")
        return {"dados": dados}
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {"erro": str(e)}