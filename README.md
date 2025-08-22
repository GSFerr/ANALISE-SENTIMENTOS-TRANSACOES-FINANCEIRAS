Pipeline de Análise de Sentimento de Transações Financeiras

Este projeto implementa um pipeline de dados para simular a ingestão, processamento e análise de sentimento de transações financeiras e avaliações de clientes. O pipeline utiliza Python, PySpark e PostgreSQL para processar e armazenar os dados em camadas (Bronze, Silver e Gold).

Tecnologias Utilizadas
Python 3.10+: Linguagem de programação principal.

PySpark: Para o processamento distribuído de dados.

Pandas: Para a manipulação de dados em memória.

PostgreSQL: Como banco de dados para persistência das camadas de dados.

NLTK & TextBlob: Para a análise de sentimento.

Estrutura do Projeto
.
├── data/
│   ├── raw/                  # Dados brutos
│   └── ...
├── src/
│   ├── database/             # Scripts para carregamento no DB
│   ├── ingestion/            # Scripts de ingestão de dados
│   ├── processing/           # Scripts de processamento e limpeza
│   ├── reporting/            # Scripts de agregação e relatórios
│   └── utils/                # Utilitários, como a função de upload para S3
├── tests/
│   └── ...                   # Arquivos de testes unitários
├── .env.example              # Exemplo de arquivo de variáveis de ambiente
├── .gitignore                # Arquivo com itens a serem ignorados pelo Git
├── main.py                   # Orquestrador principal do pipeline
├── requirements.txt          # Dependências do projeto
└── README.md                 # Documentação do projeto

Configuração do Ambiente
Clone o repositório:

git clone https://github.com/seu-usuario/seu-projeto.git
cd seu-projeto

Crie e ative o ambiente virtual (.venv):

python -m venv .venv
# No Windows:
.venv\Scripts\activate
# No macOS/Linux:
source .venv/bin/activate

Instale as dependências:

pip install -r requirements.txt

Configure o banco de dados e as variáveis de ambiente:

Instale o PostgreSQL.

Crie um banco de dados chamado financial_data.

Crie um arquivo .env na raiz do projeto, a partir do .env.example, e preencha com suas credenciais.

Execução do Pipeline
Para rodar o pipeline de dados completo, execute o script main.py no terminal com o ambiente virtual ativado:

python main.py

O pipeline irá gerar dados sintéticos, processá-los e persistir os resultados nas tabelas bronze_transactions, silver_processed e gold_agg_sentiment no seu banco de dados PostgreSQL.

Execução dos Testes
Os testes unitários garantem a integridade das funções principais do pipeline. Para executá-los, certifique-se de que o ambiente virtual está ativado e rode o seguinte comando na raiz do projeto:

pytest

O pytest irá detectar e executar automaticamente todos os arquivos e funções de teste.