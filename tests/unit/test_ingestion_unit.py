import pytest
import pandas as pd
from src.ingestion.data_generator import generate_transactions, generate_reviews

def test_generate_transactions_output_structure():
    """
    Testa se a função generate_transactions cria um DataFrame com o esquema e a quantidade de linhas corretos.
    """
    num_transactions = 100
    df = generate_transactions(num_transactions)

    # 1. Verificar o tipo do objeto retornado
    assert isinstance(df, pd.DataFrame)
    
    # 2. Verificar o número de linhas (quantidade de transações)
    assert len(df) == num_transactions
    
    # 3. Verificar o nome e a ordem das colunas
    expected_columns = [
        'transaction_id', 'account_id', 'transaction_amount', 
        'transaction_type', 'transaction_date', 'merchant_name'
    ]
    assert list(df.columns) == expected_columns
    
    # 4. Verificar os tipos de dados das colunas
    assert df['transaction_id'].dtype == 'object'
    assert df['account_id'].dtype == 'object'
    assert df['transaction_amount'].dtype == 'float64'
    assert df['transaction_type'].dtype == 'object'
    assert df['transaction_date'].dtype == 'datetime64[ns]'
    assert df['merchant_name'].dtype == 'object'

def test_generate_transactions_anomalies_injection():
    """
    Testa se as anomalias (valores nulos e negativos) estão sendo injetadas.
    """
    df = generate_transactions(1000) # Usar um número grande para garantir a injeção
    
    # Verificar se há valores nulos injetados em account_id
    assert df['account_id'].isnull().sum() > 0
    
    # Verificar se há valores nulos injetados em merchant_name
    assert df['merchant_name'].isnull().sum() > 0
    
    # Verificar se há valores negativos em transaction_amount
    assert (df['transaction_amount'] < 0).any()
    
def test_generate_reviews_output_structure():
    """
    Testa se a função generate_reviews cria um DataFrame com o esquema esperado e a quantidade de linhas correta.
    """
    # Criar um DataFrame de transações fictício para ser usado como entrada
    transactions_df = pd.DataFrame({
        'transaction_id': [f'TXN{i:05d}' for i in range(100)]
    })
    
    reviews_df = generate_reviews(transactions_df)
    
    # 1. Verificar o tipo do objeto retornado
    assert isinstance(reviews_df, pd.DataFrame)
    
    # 2. Verificar se o número de reviews está dentro da expectativa (70% das transações)
    assert len(reviews_df) == 70
    
    # 3. Verificar o nome e a ordem das colunas
    expected_columns = ['review_id', 'transaction_id', 'review_text']
    assert list(reviews_df.columns) == expected_columns
    
    # 4. Verificar os tipos de dados das colunas
    assert reviews_df['review_id'].dtype == 'object'
    assert reviews_df['transaction_id'].dtype == 'object'
    assert reviews_df['review_text'].dtype == 'object'

def test_generate_reviews_anomalies_injection():
    """
    Testa se as anomalias (comentários vazios e IDs de transação inválidos) são injetadas.
    """
    transactions_df = pd.DataFrame({
        'transaction_id': [f'TXN{i:05d}' for i in range(1000)]
    })
    reviews_df = generate_reviews(transactions_df)

    # Verificar se há comentários vazios
    assert (reviews_df['review_text'] == '').any()
    
    # Verificar se há IDs de transação que não existem no DataFrame original
    invalid_ids = [
        tx_id for tx_id in reviews_df['transaction_id'] 
        if tx_id not in transactions_df['transaction_id'].values
    ]
    assert len(invalid_ids) > 0
