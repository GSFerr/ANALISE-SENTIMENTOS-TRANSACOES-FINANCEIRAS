import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

class DatabaseConfig:
    """Configurações do banco de dados PostgreSQL."""
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "financial_data")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    @classmethod
    def validate(cls):
        missing = [var for var in ["DB_PASSWORD"] if not getattr(cls, var)]
        if missing:
            raise ValueError(f"Variáveis de ambiente ausentes: {', '.join(missing)}")

class S3Config:
    """Configurações do bucket S3."""
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    REGION = os.getenv("AWS_REGION", "us-east-1")

    @classmethod
    def validate(cls):
        if not cls.BUCKET_NAME:
            raise ValueError("S3_BUCKET_NAME não definido no .env")

class SparkConfig:
    """Configurações específicas do Spark."""
    PYSPARK_PYTHON_PATH = os.getenv("PYSPARK_PYTHON_PATH", r"C:\ANALISE-SENTIMENTO-TRANSACOES-FINANCEIRAS\.venv\Scripts\python.exe")
    JDBC_DRIVER_PATH = os.getenv("JDBC_DRIVER_PATH", r"C:\Users\gilso\Downloads\postgresql-42.7.6.jar")
