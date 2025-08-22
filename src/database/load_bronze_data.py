import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql import DataFrame

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session():
    """Cria e retorna uma SparkSession com os drivers necessários para o PostgreSQL."""
    logging.info("Criando SparkSession...")
    try:
        spark = SparkSession.builder \
            .appName("BronzeDataLoad") \
            .config("spark.jars", r"C:\Users\gilso\Downloads\postgresql-42.7.6.jar") \
            .getOrCreate()
        logging.info("SparkSession criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a SparkSession: {e}")
        return None

def load_bronze_data(spark: SparkSession, db_params: dict, raw_path: str):
    """
    Lê os dados brutos (CSV) e carrega-os nas tabelas da camada Bronze do banco de dados.

    Args:
        spark (SparkSession): A sessão Spark.
        db_params (dict): Parâmetros de conexão com o banco de dados.
        raw_path (str): Caminho para o diretório de dados brutos.
    """
    transactions_path = os.path.join(raw_path, "transactions.csv")
    reviews_path = os.path.join(raw_path, "reviews.csv")

    # Esquemas para os DataFrames
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("transaction_amount", DoubleType(), True),
        StructField("merchant_name", StringType(), True)
    ])

    reviews_schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("review_text", StringType(), True)
    ])

    try:
        logging.info("Lendo arquivos de transações e avaliações...")
        transactions_df = spark.read.csv(transactions_path, header=True, schema=transactions_schema)
        reviews_df = spark.read.csv(reviews_path, header=True, schema=reviews_schema)
        logging.info("Arquivos lidos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao ler os arquivos CSV: {e}")
        return

    # Renomear colunas para corresponder ao esquema do banco de dados (se necessário)
    transactions_df = transactions_df.select(
        col("transaction_id"),
        col("transaction_date"),
        col("transaction_amount"),
        col("merchant_name")
    )

    reviews_df = reviews_df.select(
        col("review_id"),
        col("transaction_id"),
        col("review_text")
    )

    # Função para salvar no banco de dados
    def save_to_database(df: DataFrame, table_name: str):
        logging.info(f"Salvando dados na tabela {table_name}...")
        try:
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['database']}") \
                .option("dbtable", table_name) \
                .option("user", db_params['user']) \
                .option("password", db_params['password']) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logging.info(f"Dados salvos com sucesso na tabela {table_name}.")
        except Exception as e:
            logging.error(f"Erro ao salvar dados na tabela {table_name}: {e}")

    # Salvar os DataFrames no banco de dados
    save_to_database(transactions_df, "bronze_transactions")
    save_to_database(reviews_df, "bronze_reviews")

if __name__ == "__main__":
    spark_session = create_spark_session()
    
    if spark_session:
        # Configurações de banco de dados (ajuste conforme o seu ambiente)
        db_credentials = {
            "host": "localhost",
            "port": "5432",
            "database": "financial_data",
            "user": "postgres",
            "password": "Pos@2025app"
        }
        
        # Caminho para os arquivos CSV brutos
        raw_data_path = "data/raw"
        
        # Executar o carregamento
        load_bronze_data(spark_session, db_credentials, raw_data_path)
        
        spark_session.stop()
    else:
        logging.error("O processo de carregamento não pôde ser iniciado devido a uma falha na SparkSession.")
