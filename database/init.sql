CREATE TABLE IF NOT EXISTS cambio_moedas (
    moeda_sigla VARCHAR(10),
    nome_moeda VARCHAR(100),
    cotacao DECIMAL(18, 4),
    variacao DECIMAL(18, 4),
    data_cotacao TIMESTAMP,
    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_moeda_sigla ON cambio_moedas(moeda_sigla);