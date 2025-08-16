import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when

# Adicione esta linha para garantir que o Spark use o Python do seu ambiente virtual
# Certifique-se de que este caminho está correto para o seu ambiente!
os.environ['PYSPARK_PYTHON'] = r'C:\ANALISE-SENTIMENTO-TRANSACOES-FINANCEIRAS\.venv\Scripts\python.exe'

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session():
    """Cria e retorna uma SparkSession configurada para o ambiente local."""
    logging.info("Criando SparkSession...")
    try:
        spark = SparkSession.builder \
            .appName("FinancialTransactionsDataMart") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        logging.info("SparkSession criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a SparkSession: {e}")
        return None

def aggregate_data(spark: SparkSession):
    """
    Agrega dados da camada Silver para criar a camada Gold (data mart).
    
    Args:
        spark (SparkSession): A sessão Spark.
    
    Returns:
        pyspark.sql.DataFrame: O DataFrame agregado para o data mart.
    """
    silver_path = os.path.join("data", "processed", "processed_data.parquet")
    gold_path = os.path.join("data", "mart", "merchant_sentiment_agg.parquet")
    
    if not os.path.exists(os.path.dirname(gold_path)):
        os.makedirs(os.path.dirname(gold_path))

    logging.info("Lendo dados da camada Silver...")
    try:
        processed_df = spark.read.parquet(silver_path)
        logging.info("Dados lidos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao ler os dados da camada Silver: {e}")
        return None
    
    logging.info("Iniciando agregação de dados para o data mart...")
    
    # Validações dos dados de entrada
    if processed_df.count() == 0:
        logging.warning("O DataFrame de entrada está vazio. Nenhuma agregação será realizada.")
        return processed_df
    
    # Agregações de sentimentos e valores
    agg_df = processed_df.groupBy("merchant_name").agg(
        count("*").alias("total_transactions"),
        sum("transaction_amount").alias("total_amount"),
        sum(when(col("review_sentiment") == "positive", 1).otherwise(0)).alias("positive_sentiment_count"),
        sum(when(col("review_sentiment") == "negative", 1).otherwise(0)).alias("negative_sentiment_count"),
        sum(when(col("review_sentiment") == "neutral", 1).otherwise(0)).alias("neutral_sentiment_count")
    )
    
    logging.info("Agregação concluída. Salvando dados na camada Gold...")
    
    # Salvando o DataFrame agregado
    try:
        agg_df.write.mode("overwrite").parquet(gold_path)
        logging.info("Dados agregados salvos com sucesso na camada Gold.")
    except Exception as e:
        logging.error(f"Erro ao salvar os dados na camada Gold: {e}")
        return None

    return agg_df

if __name__ == "__main__":
    spark_session = create_spark_session()
    
    if spark_session:
        data_mart_df = aggregate_data(spark_session)
        if data_mart_df:
            logging.info("Exibindo um exemplo dos dados agregados no data mart:")
            data_mart_df.show(5, truncate=False)
            logging.info("Processo concluído com sucesso.")
        spark_session.stop()
    else:
        logging.error("O processo não pôde ser concluído devido a uma falha na SparkSession.")