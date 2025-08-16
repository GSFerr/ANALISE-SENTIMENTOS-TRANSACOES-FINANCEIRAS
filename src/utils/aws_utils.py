import boto3
import os
import logging
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

def get_s3_client():
    """
    Retorna um cliente do S3, lendo as credenciais do ambiente.
    """
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        return s3_client
    except Exception as e:
        logging.error(f"Erro ao criar cliente S3: {e}")
        return None

def upload_to_s3(file_path, bucket_name, object_name):
    """
    Faz o upload de um arquivo local para um bucket S3.
    
    Args:
        file_path (str): O caminho para o arquivo local.
        bucket_name (str): O nome do bucket S3.
        object_name (str): O caminho de destino do objeto no S3.
    """
    s3_client = get_s3_client()
    if s3_client:
        try:
            logging.info(f"Fazendo upload de '{file_path}' para s3://{bucket_name}/{object_name}")
            s3_client.upload_file(file_path, bucket_name, object_name)
            logging.info("Upload concluído com sucesso!")
            return True
        except Exception as e:
            logging.error(f"Erro ao fazer upload para o S3: {e}")
            return False
    return False

if __name__ == "__main__":
    # Exemplo de uso (apenas para teste local)
    # Lembre-se de criar um arquivo de teste na raiz do projeto
    test_file_path = "test_upload.txt"
    with open(test_file_path, "w") as f:
        f.write("Este é um arquivo de teste para upload no S3.")

    s3_bucket = os.getenv('S3_BUCKET_NAME')
    s3_object_key = "test/test_upload.txt"

    if upload_to_s3(test_file_path, s3_bucket, s3_object_key):
        logging.info("Teste de upload bem-sucedido.")
    else:
        logging.error("Teste de upload falhou.")
    
    os.remove(test_file_path)