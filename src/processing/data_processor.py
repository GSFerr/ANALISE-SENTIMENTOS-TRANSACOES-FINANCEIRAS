import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, lit, trim
from pyspark.sql.types import DoubleType, StringType
os.environ['PYSPARK_PYTHON'] = r'C:\ANALISE-SENTIMENTO-TRANSACOES-FINANCEIRAS\.venv\Scripts\python.exe'

def create_spark_session():
    """
    Cria e retorna uma SparkSession.
    """
    logging.info("Criando SparkSession...")
    spark = SparkSession.builder \
        .appName("FinancialDataProcessing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    logging.info("SparkSession criada com sucesso.")
    return spark

def simple_sentiment_analyzer(text):
    """
    Função simples para analisar o sentimento de um texto.
    """
    if text is None:
        return 'neutral'
    
    text = text.lower()
    
    positive_keywords = ['excelente', 'bom', 'ótimo', 'adorei', 'recomendo', 'parabéns', 'superou']
    negative_keywords = ['horrível', 'péssima', 'danificado', 'insatisfeito', 'deixou a desejar', 'ruim']
    
    is_positive = any(word in text for word in positive_keywords)
    is_negative = any(word in text for word in negative_keywords)
    
    if is_positive and is_negative:
        return 'mixed'
    elif is_positive:
        return 'positive'
    elif is_negative:
        return 'negative'
    else:
        return 'neutral'

def process_data(spark):
    """
    Processa os dados brutos da camada Bronze e salva na camada Silver.
    """
    # Definir os caminhos de entrada e saída
    raw_path = os.path.join('data', 'raw')
    processed_path = os.path.join('data', 'processed')
    
    if not os.path.exists(raw_path):
        logging.error(f"Caminho de dados brutos não encontrado: {raw_path}")
        return None

    # --- 1. Leitura dos dados da camada Bronze ---
    logging.info("Lendo dados brutos da camada Bronze...")
    transactions_df = spark.read.csv(os.path.join(raw_path, "transactions.csv"), header=True, inferSchema=True)
    reviews_df = spark.read.csv(os.path.join(raw_path, "reviews.csv"), header=True, inferSchema=True)

    # Cache dos dataframes para melhor performance em operações repetidas
    transactions_df.cache()
    reviews_df.cache()

    logging.info("Dados lidos com sucesso.")

    # --- 2. Join (Junção) dos DataFrames ---
    logging.info("Realizando o join dos DataFrames...")
    # O 'how="left"' garante que todas as transações sejam mantidas, mesmo sem um review
    joined_df = transactions_df.join(reviews_df, on="transaction_id", how="left")

    # --- 3. Limpeza e Enriquecimento de Dados ---
    logging.info("Iniciando limpeza e enriquecimento de dados...")
    
    # Casting de tipos para garantir consistência
    joined_df = joined_df.withColumn("transaction_amount", col("transaction_amount").cast(DoubleType()))
    
    # Tratamento de Nulos
    processed_df = joined_df.na.drop(subset=['account_id'])
    
    # Tratamento de valores inválidos (e.g., amount negativo)
    processed_df = processed_df.withColumn(
        "transaction_amount",
        when(col("transaction_amount") < 0, lit(0.0)).otherwise(col("transaction_amount"))
    )

    # Análise de Sentimento (UDF - User Defined Function)
    # Registrando a função Python como uma UDF do Spark
    sentiment_udf = spark.udf.register("sentiment_analyzer", simple_sentiment_analyzer, StringType())
    
    # Aplicando a UDF ao DataFrame
    processed_df = processed_df.withColumn(
        "review_sentiment",
        sentiment_udf(col("review_text"))
    )

    # --- 4. Salvar na Camada Silver (Formato Parquet) ---
    logging.info("Salvando o DataFrame processado na camada Silver...")
    
    # Remove colunas que não são mais necessárias
    final_df = processed_df.drop("review_id", "review_text")

    # Garante que o diretório de saída existe
    if not os.path.exists(processed_path):
        os.makedirs(processed_path)
    
    # Salva o DataFrame no formato Parquet
    final_df.write.mode("overwrite").parquet(os.path.join(processed_path, "processed_data.parquet"))
    
    logging.info("Dados processados e salvos com sucesso na camada Silver.")
    return final_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    spark_session = create_spark_session()
    
    processed_data = process_data(spark_session)
    
    if processed_data:
        logging.info("Exibindo um exemplo dos dados processados:")
        processed_data.show(5, truncate=False)

    spark_session.stop()