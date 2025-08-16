import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Importa as funções de cada etapa do pipeline
from src.ingestion.data_generator import generate_transactions, generate_reviews, save_data
from src.processing.data_processor import process_data
from src.reporting.data_aggregator import aggregate_data
from src.utils.aws_utils import upload_to_s3

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configura o logger para a aplicação
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Variável de ambiente para o Spark encontrar o Python correto no ambiente virtual
# Certifique-se de que este caminho está correto para o seu ambiente!
os.environ['PYSPARK_PYTHON'] = r'C:\ANALISE-SENTIMENTO-TRANSACOES-FINANCEIRAS\.venv\Scripts\python.exe'

def create_spark_session() -> SparkSession:
    """Cria e retorna uma SparkSession."""
    logging.info("Criando SparkSession...")
    try:
        spark = SparkSession.builder \
            .appName("FinancialTransactionsPipeline") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        logging.info("SparkSession criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a SparkSession: {e}")
        raise e

def run_ingestion_pipeline():
    """
    Orquestra o pipeline de ingestão de dados.
    1. Gera os dados sintéticos.
    2. Salva os dados localmente.
    3. Faz o upload dos arquivos para o S3, camada Bronze.
    """
    logging.info("Iniciando o pipeline de ingestão de dados...")

    # --- 1. Geração de dados ---
    transactions_df = generate_transactions(num_transactions=1000)
    reviews_df = generate_reviews(transactions_df)

    # --- 2. Salvando dados na camada Bronze local ---
    local_raw_path = os.path.join('data', 'raw')
    save_data(transactions_df, reviews_df, local_raw_path)

    # --- 3. Upload para o S3 (Camada Bronze na Nuvem) ---
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    if not s3_bucket:
        logging.error("Variável de ambiente 'S3_BUCKET_NAME' não encontrada. Verifique o arquivo .env.")
        return

    transactions_file = os.path.join(local_raw_path, 'transactions.csv')
    reviews_file = os.path.join(local_raw_path, 'reviews.csv')
    date_path = datetime.now().strftime("year=%Y/month=%m/day=%d")
    transactions_s3_key = f"bronze/transactions/{date_path}/transactions.csv"
    reviews_s3_key = f"bronze/reviews/{date_path}/reviews.csv"
    
    upload_to_s3(transactions_file, s3_bucket, transactions_s3_key)
    upload_to_s3(reviews_file, s3_bucket, reviews_s3_key)
        
    logging.info("Pipeline de ingestão finalizado.")


def main():
    """Orquestra a execução completa do pipeline de dados."""
    spark = None
    try:
        logging.info("Iniciando o pipeline completo de processamento de dados...")

        # Etapa 1: Ingestão (Camada Bronze)
        run_ingestion_pipeline()
        
        # Criação da SparkSession para as próximas etapas
        spark = create_spark_session()
        
        # Etapa 2: Processamento (Camada Silver)
        processed_df = process_data(spark)
        
        # Etapa 3: Agregação (Camada Gold)
        if processed_df:
            agg_df = aggregate_data(spark)
            if agg_df:
                logging.info("Exibindo um exemplo dos dados agregados no data mart:")
                agg_df.show(5, truncate=False)

        logging.info("Pipeline completo executado com sucesso.")

    except Exception as e:
        logging.error(f"O pipeline falhou. Erro: {e}")
        raise e
    finally:
        if spark:
            spark.stop()
            logging.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
