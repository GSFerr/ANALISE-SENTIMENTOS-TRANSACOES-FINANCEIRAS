--
-- SQL Script para a criação do esquema de banco de dados para o projeto de Análise de Sentimento
--
-- Este script cria três tabelas, uma para cada camada do pipeline:
-- 1. bronze_transactions: Dados brutos de transações.
-- 2. bronze_reviews: Dados brutos de avaliações.
-- 3. silver_processed_data: Dados unificados e limpos.
-- 4. gold_merchant_sentiment_agg: Dados agregados (Data Mart).
--

-- -------------------------------------------------------------------
-- Camada Bronze: Dados Brutos
-- -------------------------------------------------------------------

-- Tabela de Transações Brutas
CREATE TABLE IF NOT EXISTS bronze_transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    transaction_date TIMESTAMP,
    transaction_amount DOUBLE PRECISION,
    merchant_name VARCHAR(255)
);

-- Tabela de Avaliações Brutas
CREATE TABLE IF NOT EXISTS bronze_reviews (
    review_id VARCHAR(255) PRIMARY KEY,
    transaction_id VARCHAR(255),
    review_text TEXT,
    FOREIGN KEY (transaction_id) REFERENCES bronze_transactions(transaction_id)
);

-- -------------------------------------------------------------------
-- Camada Silver: Dados Processados e Unificados
-- -------------------------------------------------------------------

-- Tabela de Dados Processados
CREATE TABLE IF NOT EXISTS silver_processed_data (
    transaction_id VARCHAR(255) PRIMARY KEY,
    transaction_date TIMESTAMP,
    transaction_amount DOUBLE PRECISION,
    merchant_name VARCHAR(255),
    review_text TEXT,
    review_sentiment VARCHAR(255)
);

-- -------------------------------------------------------------------
-- Camada Gold: Data Mart Agregado
-- -------------------------------------------------------------------

-- Tabela de Agregações de Sentimento por Comerciante
CREATE TABLE IF NOT EXISTS gold_merchant_sentiment_agg (
    merchant_name VARCHAR(255) PRIMARY KEY,
    total_transactions INTEGER,
    total_amount DOUBLE PRECISION,
    positive_sentiment_count INTEGER,
    negative_sentiment_count INTEGER,
    neutral_sentiment_count INTEGER,
    mixed_sentiment_count INTEGER
);

-- Fim do script
