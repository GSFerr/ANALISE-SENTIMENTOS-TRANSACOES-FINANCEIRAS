import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def generate_transactions(num_transactions):
    """
    Gera dados sintéticos de transações financeiras.
    """
    # Criar um gerador de números aleatórios
    rng = np.random.default_rng()

    # Gerar IDs de transação únicos
    transaction_ids = [f'TXN{i:05d}' for i in range(num_transactions)]
    
    # Gerar IDs de conta
    account_ids = [f'ACC{i:03d}' for i in rng.integers(1, 101, num_transactions)]

    # Gerar valores de transação (incluindo valores negativos)
    amounts = rng.uniform(-1000, 5000, num_transactions)
    
    # Injetar alguns valores negativos para simular anomalias
    amounts[rng.choice(num_transactions, 20, replace=False)] = rng.uniform(-500, -1, 20)
    
    # Gerar tipos de transação
    transaction_types = rng.choice(['purchase', 'deposit', 'withdrawal'], num_transactions)
    
   # Gerar datas de transação
    start_date = datetime.now() - timedelta(days=90)
    dates = [start_date + timedelta(seconds=int(rng.integers(0, 90*24*60*60))) for _ in range(num_transactions)]

    # Converter a lista de datas para um array temporário para injeção de anomalias
    dates_temp = np.array(dates, dtype=object)
    
    # Injetar algumas datas futuras
    future_dates_indices = rng.choice(num_transactions, 5, replace=False)
    dates_temp[future_dates_indices] = [datetime.now() + timedelta(days=int(rng.integers(1, 10))) for _ in range(5)]

     # Converter de volta para lista
    dates = dates_temp.tolist()
    
    # Gerar nomes de estabelecimentos
    merchants = rng.choice(['Amazon', 'Starbucks', 'Netflix', 'Shell', 'Supermarket', 'Restaurant'], num_transactions)

    # Injetar alguns valores nulos
    account_ids_temp = np.array(account_ids, dtype=object)  # Converter para tipo objeto
    account_ids_temp[rng.choice(num_transactions, 15, replace=False)] = None
    account_ids = account_ids_temp.tolist()

    merchants_temp = np.array(merchants, dtype=object)
    merchants_temp[rng.choice(num_transactions, 10, replace=False)] = None
    merchants = merchants_temp.tolist()

    transactions_df = pd.DataFrame({
        'transaction_id': transaction_ids,
        'account_id': account_ids,
        'transaction_amount': amounts,
        'transaction_type': transaction_types,
        'transaction_date': dates,
        'merchant_name': merchants
    })

    return transactions_df

def generate_reviews(transactions_df):
    """
    Gera dados de avaliações/comentários de transações, relacionando-os com os IDs de transação.
    """
    # Criar um gerador de números aleatórios
    rng = np.random.default_rng()

    num_transactions = len(transactions_df)
    
    # Usar uma parte dos IDs de transação para as avaliações
    # Isso simula que nem toda transação tem um comentário
    transaction_ids_with_reviews = transactions_df['transaction_id'].sample(n=int(num_transactions * 0.7), replace=False, random_state=rng).tolist()
    
    # Gerar textos de avaliações
    review_texts = rng.choice([
        "Serviço horrível no restaurante.",
        "Excelente compra, chegou super rápido!",
        "Qualidade muito boa. Recomendo.",
        "Experiência de compra péssima, não voltarei.",
        "Atendimento ao cliente deixou a desejar.",
        "Melhorou muito a experiência, parabéns.",
        "Produto danificado na entrega, insatisfeito.",
        "Não tenho reclamações, tudo ok.",
        "Muito caro para o que oferece.",
        "Adorei o produto, superou as expectativas."
    ], len(transaction_ids_with_reviews))

    # Injetar alguns comentários vazios
    review_texts[rng.choice(len(review_texts), 5, replace=False)] = ''
    
    # Gerar IDs de avaliação únicos
    review_ids = [f'REV{i:05d}' for i in range(len(transaction_ids_with_reviews))]

    reviews_df = pd.DataFrame({
        'review_id': review_ids,
        'transaction_id': transaction_ids_with_reviews,
        'review_text': review_texts
    })

    # Injetar alguns transaction_id que não existem nas transações
    reviews_df.loc[rng.choice(len(reviews_df), 3, replace=False), 'transaction_id'] = [f'TXN9999{i}' for i in range(3)]
    
    return reviews_df

def save_data(transactions_df, reviews_df, output_path):
    """
    Salva os DataFrames em arquivos CSV na pasta de saída.
    """
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    transactions_df.to_csv(os.path.join(output_path, 'transactions.csv'), index=False)
    reviews_df.to_csv(os.path.join(output_path, 'reviews.csv'), index=False)
    print(f"Arquivos salvos em {output_path}")

if __name__ == "__main__":
    # Definir o caminho de saída (camada bronze)
    output_directory = os.path.join('data', 'raw')
    
    # Gerar dados
    transactions_data = generate_transactions(num_transactions=1000)
    reviews_data = generate_reviews(transactions_data)
    
    # Salvar arquivos
    save_data(transactions_data, reviews_data, output_directory)