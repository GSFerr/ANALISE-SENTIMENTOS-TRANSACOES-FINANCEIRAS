import pytest
from src.processing.data_processor import simple_sentiment_analyzer

def test_sentiment_analyzer_positive():
    """
    Verifica se a função retorna 'positive' para textos com palavras-chave positivas.
    """
    assert simple_sentiment_analyzer("O serviço foi excelente, adorei!") == "positive"
    assert simple_sentiment_analyzer("Recomendo, foi um ótimo produto") == "positive"
    assert simple_sentiment_analyzer("parabéns pelo trabalho") == "positive"

def test_sentiment_analyzer_negative():
    """
    Verifica se a função retorna 'negative' para textos com palavras-chave negativas.
    """
    assert simple_sentiment_analyzer("A qualidade foi horrível e péssima.") == "negative"
    assert simple_sentiment_analyzer("O produto veio danificado, estou insatisfeito.") == "negative"
    assert simple_sentiment_analyzer("Deixou muito a desejar, um serviço ruim.") == "negative"

def test_sentiment_analyzer_neutral():
    """
    Verifica se a função retorna 'neutral' para textos sem palavras-chave.
    """
    assert simple_sentiment_analyzer("A entrega foi feita no horário correto.") == "neutral"
    assert simple_sentiment_analyzer("O produto chegou ontem.") == "neutral"
    assert simple_sentiment_analyzer("Gostei da embalagem.") == "neutral"

def test_sentiment_analyzer_mixed():
    """
    Verifica se a função retorna 'mixed' para textos com palavras-chave positivas e negativas.
    """
    assert simple_sentiment_analyzer("O atendimento foi ótimo, mas a entrega foi péssima.") == "mixed"
    assert simple_sentiment_analyzer("Produto excelente, mas a experiência foi horrível.") == "mixed"

@pytest.mark.parametrize("text, expected_sentiment", [
    (None, "neutral"),
    ("", "neutral"),
    (" ", "neutral")
])
def test_sentiment_analyzer_edge_cases(text, expected_sentiment):
    """
    Verifica cenários de casos de borda como Nulos e strings vazias.
    """
    assert simple_sentiment_analyzer(text) == expected_sentiment
