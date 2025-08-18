import pytest
import pandas as pd
import os
from src.ingestion.data_generator import generate_transactions, generate_reviews, save_data

def test_save_data_functionality(tmp_path):
    """
    Testa se a função save_data salva os DataFrames corretamente em disco.
    O fixture tmp_path cria e remove um diretório temporário automaticamente.
    """
    # 1. Geração de dados simulados (mock)
    num_transactions = 50
    transactions_df = generate_transactions(num_transactions)
    reviews_df = generate_reviews(transactions_df)
    
    # 2. Execução: Chamamos a função save_data com o diretório temporário
    output_path = tmp_path / "raw"
    save_data(transactions_df, reviews_df, str(output_path))
    
    # 3. Verificação (Assertions):
    # Verificar se o diretório de saída foi criado
    assert os.path.exists(output_path)
    
    # Verificar se os arquivos CSV foram criados
    transactions_file = os.path.join(output_path, 'transactions.csv')
    reviews_file = os.path.join(output_path, 'reviews.csv')
    assert os.path.exists(transactions_file)
    assert os.path.exists(reviews_file)
    
    # Ler os arquivos salvos e verificar seu conteúdo
    saved_transactions_df = pd.read_csv(transactions_file)
    saved_reviews_df = pd.read_csv(reviews_file)
    
    # Comparar o número de linhas e colunas dos DataFrames salvos com os originais
    assert len(saved_transactions_df) == len(transactions_df)
    assert len(saved_transactions_df.columns) == len(transactions_df.columns)
    
    assert len(saved_reviews_df) == len(reviews_df)
    assert len(saved_reviews_df.columns) == len(reviews_df.columns)
