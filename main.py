import os
import logging
from src.ingestion.data_generator import generate_transactions, generate_reviews, save_data
from src.utils.aws_utils import upload_to_s3
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configura o logger para a aplicação
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_ingestion_pipeline():
    """
    Orquestra o pipeline de ingestão de dados.
    1. Gera os dados sintéticos.
    2. Salva os dados localmente.
    3. Faz o upload dos arquivos para o S3, camada Bronze.
    """
    logging.info("Iniciando o pipeline de ingestão de dados...")

    # --- 1. Geração de dados ---
    logging.info("Gerando dados sintéticos de transações e avaliações...")
    transactions_df = generate_transactions(num_transactions=1000)
    reviews_df = generate_reviews(transactions_df)

    # --- 2. Salvando dados na camada Bronze local ---
    local_raw_path = os.path.join('data', 'raw')
    logging.info(f"Salvando dados localmente na camada Bronze em '{local_raw_path}'...")
    save_data(transactions_df, reviews_df, local_raw_path)

    # --- 3. Upload para o S3 (Camada Bronze na Nuvem) ---
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    
    if not s3_bucket:
        logging.error("Variável de ambiente 'S3_BUCKET_NAME' não encontrada. Verifique o arquivo .env.")
        return

    logging.info(f"Iniciando upload para o S3 no bucket '{s3_bucket}'...")
    
    transactions_file = os.path.join(local_raw_path, 'transactions.csv')
    reviews_file = os.path.join(local_raw_path, 'reviews.csv')
    
    # Criamos o caminho de destino no S3, seguindo o padrão de data para particionamento
    date_path = datetime.now().strftime("year=%Y/month=%m/day=%d")
    
    transactions_s3_key = f"bronze/transactions/{date_path}/transactions.csv"
    reviews_s3_key = f"bronze/reviews/{date_path}/reviews.csv"
    
    # Upload do arquivo de transações
    if upload_to_s3(transactions_file, s3_bucket, transactions_s3_key):
        logging.info(f"Upload de transações para o S3 concluído: s3://{s3_bucket}/{transactions_s3_key}")
    else:
        logging.error("Falha no upload do arquivo de transações para o S3.")
        
    # Upload do arquivo de avaliações
    if upload_to_s3(reviews_file, s3_bucket, reviews_s3_key):
        logging.info(f"Upload de avaliações para o S3 concluído: s3://{s3_bucket}/{reviews_s3_key}")
    else:
        logging.error("Falha no upload do arquivo de avaliações para o S3.")
        
    logging.info("Pipeline de ingestão finalizado.")


if __name__ == "__main__":
    run_ingestion_pipeline()