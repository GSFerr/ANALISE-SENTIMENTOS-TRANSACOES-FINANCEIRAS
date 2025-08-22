import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Importações do pipeline
from src.ingestion.data_generator import generate_transactions, generate_reviews, save_data
from src.processing.data_processor import process_data
from src.reporting.data_aggregator import aggregate_data
from src.utils.aws_utils import upload_to_s3
from src.database.load_bronze_data import load_bronze_data
from src.database.load_silver_data import load_silver_data
from src.database.load_gold_data import load_gold_data
from config import DatabaseConfig, S3Config, SparkConfig

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define o caminho do Python para o Spark
os.environ['PYSPARK_PYTHON'] = r'C:\ANALISE-SENTIMENTO-TRANSACOES-FINANCEIRAS\.venv\Scripts\python.exe'

def create_spark_session() -> SparkSession:
    """Cria e retorna uma SparkSession."""
    logging.info("Criando SparkSession...")
    try:
        spark = SparkSession.builder \
            .appName("FinancialTransactionsPipeline") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.jars", "C:\\Users\\gilso\\Downloads\\postgresql-42.7.6.jar") \
            .getOrCreate()
        logging.info("SparkSession criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a SparkSession: {e}")
        raise

def run_ingestion_pipeline(upload_to_cloud: bool = True):
    """Executa a etapa de ingestão de dados."""
    logging.info("Iniciando o pipeline de ingestão...")

    try:
        transactions_df = generate_transactions(num_transactions=1000)
        reviews_df = generate_reviews(transactions_df)

        local_raw_path = os.path.join('data', 'raw')
        save_data(transactions_df, reviews_df, local_raw_path)

        if upload_to_cloud:
            s3_bucket = os.getenv('S3_BUCKET_NAME')
            if not s3_bucket:
                logging.warning("S3_BUCKET_NAME não definido. Pulando upload para S3.")
                return

            date_path = datetime.now().strftime("year=%Y/month=%m/day=%d")
            upload_to_s3(os.path.join(local_raw_path, 'transactions.csv'), s3_bucket, f"bronze/transactions/{date_path}/transactions.csv")
            upload_to_s3(os.path.join(local_raw_path, 'reviews.csv'), s3_bucket, f"bronze/reviews/{date_path}/reviews.csv")

        logging.info("Ingestão concluída.")
    except Exception as e:
        logging.error(f"Erro na etapa de ingestão: {e}")
        raise

def main():
    spark = None
    try:
        # Valida configurações
        DatabaseConfig.validate()
        S3Config.validate()

        # Usa as configurações
        os.environ['PYSPARK_PYTHON'] = SparkConfig.PYSPARK_PYTHON_PATH

        db_credentials = {
            "host": DatabaseConfig.DB_HOST,
            "port": DatabaseConfig.DB_PORT,
            "database": DatabaseConfig.DB_NAME,
            "user": DatabaseConfig.DB_USER,
            "password": DatabaseConfig.DB_PASSWORD
        }

        raw_data_path = "data/raw"

        # Etapa 1: Ingestão
        run_ingestion_pipeline(upload_to_cloud=True)

        # Etapa 2: SparkSession
        spark = create_spark_session()

        # Etapa 3: Bronze
        try:
            logging.info("Carregando dados na camada Bronze...")
            load_bronze_data(spark, db_credentials, raw_data_path)
        except Exception as e:
            logging.error(f"Erro na camada Bronze: {e}")
            raise

        # Etapa 4: Silver
        try:
            logging.info("Processando e carregando dados na camada Silver...")
            load_silver_data(spark, db_credentials)
        except Exception as e:
            logging.error(f"Erro na camada Silver: {e}")
            raise

        # Etapa 5: Gold
        try:
            logging.info("Agregando e carregando dados na camada Gold...")
            load_gold_data(spark, db_credentials)
        except Exception as e:
            logging.error(f"Erro na camada Gold: {e}")
            raise

        logging.info("--- Pipeline executado com sucesso! ---")

    except Exception as e:
        logging.error(f"Falha geral no pipeline: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("SparkSession encerrada.")

if __name__ == "__main__":
    main()
