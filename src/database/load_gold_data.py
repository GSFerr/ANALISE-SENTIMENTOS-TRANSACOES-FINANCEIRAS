import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when
import psycopg2
from psycopg2 import sql

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session():
    """Cria e retorna uma SparkSession com os drivers necessários para o PostgreSQL."""
    logging.info("Criando SparkSession...")
    try:
        # Lembre-se de substituir o caminho pelo seu driver JDBC do PostgreSQL
        spark = SparkSession.builder \
            .appName("GoldDataLoad") \
            .config("spark.jars", r"C:\Users\gilso\Downloads\postgresql-42.7.6.jar") \
            .getOrCreate()
        logging.info("SparkSession criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a SparkSession: {e}")
        return None

def load_gold_data(spark: SparkSession, db_params: dict):
    """
    Lê os dados da camada Silver, agrega e carrega na camada Gold (data mart).

    Args:
        spark (SparkSession): A sessão Spark.
        db_params (dict): Parâmetros de conexão com o banco de dados.
    """
    logging.info("Iniciando o processo de carregamento da camada Gold.")
    
    # 1. Leitura dos dados da camada Silver no banco de dados
    try:
        logging.info("Lendo dados da tabela silver_processed_data...")
        silver_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['database']}") \
            .option("dbtable", "silver_processed_data") \
            .option("user", db_params['user']) \
            .option("password", db_params['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        logging.info("Dados da camada Silver lidos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao ler dados do banco de dados: {e}")
        return
    
    # Validação dos dados lidos
    if silver_df.count() == 0:
        logging.warning("A tabela da camada Silver está vazia. Nenhuma agregação será realizada.")
        return

    # 2. Agregações por comerciante
    logging.info("Iniciando agregação de dados para o data mart...")
    agg_df = silver_df.groupBy("merchant_name").agg(
        count("*").alias("total_transactions"),
        sum("transaction_amount").alias("total_amount"),
        sum(when(col("review_sentiment") == "positive", 1).otherwise(0)).alias("positive_sentiment_count"),
        sum(when(col("review_sentiment") == "negative", 1).otherwise(0)).alias("negative_sentiment_count"),
        sum(when(col("review_sentiment") == "neutral", 1).otherwise(0)).alias("neutral_sentiment_count"),
        sum(when(col("review_sentiment") == "mixed", 1).otherwise(0)).alias("mixed_sentiment_count")
    )
    logging.info("Agregação concluída com sucesso.")

    # 3. Salvando o DataFrame agregado na tabela gold_merchant_sentiment_agg
    logging.info("Salvando dados agregados na tabela gold_merchant_sentiment_agg...")
    try:
        agg_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['database']}") \
            .option("dbtable", "gold_merchant_sentiment_agg") \
            .option("user", db_params['user']) \
            .option("password", db_params['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        logging.info("Dados salvos com sucesso na camada Gold.")
    except Exception as e:
        logging.error(f"Erro ao salvar dados na tabela gold_merchant_sentiment_agg: {e}")

if __name__ == "__main__":
    spark_session = create_spark_session()
    
    if spark_session:
        # Configurações de banco de dados (ajuste conforme o seu ambiente)
        db_credentials = {
            "host": "localhost",
            "port": "5432",
            "database": "financial_data",
            "user": "postgres",
            "password": "your_password"
        }
        
        # Executar o carregamento
        load_gold_data(spark_session, db_credentials)
        
        spark_session.stop()
    else:
        logging.error("O processo de carregamento não pôde ser iniciado devido a uma falha na SparkSession.")
