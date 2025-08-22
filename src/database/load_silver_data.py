import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
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
            .appName("SilverDataLoad") \
            .config("spark.jars", r"C:\Users\gilso\Downloads\postgresql-42.7.6.jar") \
            .getOrCreate()
        logging.info("SparkSession criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a SparkSession: {e}")
        return None

def load_silver_data(spark: SparkSession, db_params: dict):
    """
    Lê dados da camada Bronze, processa (análise de sentimento) e carrega na camada Silver.

    Args:
        spark (SparkSession): A sessão Spark.
        db_params (dict): Parâmetros de conexão com o banco de dados.
    """
    logging.info("Iniciando o processo de carregamento da camada Silver.")
    
    # 1. Leitura dos dados da camada Bronze no banco de dados
    try:
        logging.info("Lendo dados das tabelas bronze_transactions e bronze_reviews...")
        transactions_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['database']}") \
            .option("dbtable", "bronze_transactions") \
            .option("user", db_params['user']) \
            .option("password", db_params['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        reviews_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['database']}") \
            .option("dbtable", "bronze_reviews") \
            .option("user", db_params['user']) \
            .option("password", db_params['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        logging.info("Dados da camada Bronze lidos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao ler dados do banco de dados: {e}")
        return
    
    # Validação dos dados lidos
    if transactions_df.count() == 0 or reviews_df.count() == 0:
        logging.warning("Uma ou ambas as tabelas da camada Bronze estão vazias. O processo de carregamento será abortado.")
        return

    # 2. Junção dos DataFrames de transações e avaliações
    logging.info("Iniciando a junção dos DataFrames...")
    processed_df = transactions_df.join(reviews_df, "transaction_id", "inner")
    logging.info("Junção concluída com sucesso.")

    # 3. Análise de sentimento simplificada com base no texto da avaliação
    # Em um cenário real, você usaria um modelo de Machine Learning aqui.
    logging.info("Executando análise de sentimento (simplificada)...")
    processed_df = processed_df.withColumn(
        "review_sentiment",
        when(col("review_text").rlike("(?i)ótimo|excelente|bom|satisfeito|gostei|perfeito"), lit("positive"))
        .when(col("review_text").rlike("(?i)ruim|péssimo|horrível|insatisfeito|não gostei|problema"), lit("negative"))
        .when(col("review_text").rlike("(?i)pago|reembolso|compra|recebido"), lit("neutral"))
        .otherwise(lit("mixed"))
    )

    # 4. Seleção e reordenamento das colunas para o schema da camada Silver
    final_silver_df = processed_df.select(
        col("transaction_id"),
        col("transaction_date"),
        col("transaction_amount"),
        col("merchant_name"),
        col("review_text"),
        col("review_sentiment")
    )
    
    # 5. Salvando o DataFrame na tabela silver_processed_data
    logging.info("Salvando dados processados na tabela silver_processed_data...")
    try:
        final_silver_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['database']}") \
            .option("dbtable", "silver_processed_data") \
            .option("user", db_params['user']) \
            .option("password", db_params['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        logging.info("Dados salvos com sucesso na camada Silver.")
    except Exception as e:
        logging.error(f"Erro ao salvar dados na tabela silver_processed_data: {e}")

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
        load_silver_data(spark_session, db_credentials)
        
        spark_session.stop()
    else:
        logging.error("O processo de carregamento não pôde ser iniciado devido a uma falha na SparkSession.")
