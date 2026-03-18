import requests
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine

# 1. FUNÇÃO DE EXTRAÇÃO E TRANSFORMAÇÃO
def run_backfill():
    moedas = ['USD-BRL', 'EUR-BRL', 'BTC-BRL']

    dicionario_nomes = {
        'USD-BRL': 'Dólar Americano/Real Brasileiro',
        'EUR-BRL': 'Euro/Real Brasileiro',
        'BTC-BRL': 'Bitcoin/Real Brasileiro'
    }

    dias = 360
    dados_completos = []

    for moeda in moedas:
        print(f"Extraindo e transformando {dias} dias de {moeda}")
        url = f"https://economia.awesomeapi.com.br/json/daily/{moeda}/{dias}"
        response = requests.get(url)

        if response.status_code == 200:
            dados = response.json()

            #transformar os dados em um formato mais limpo e estruturado
            for item in dados:
                dados_completos.append({
                    'moeda_sigla': moeda.replace('-', ''),
                    'nome_moeda': dicionario_nomes[moeda],
                    'cotacao': float(item['bid']),
                    'variacao': float(item['pctChange']),
                    'data_cotacao': datetime.fromtimestamp(int(item['timestamp'])),
                    'data_processamento': datetime.now()
                })
        else:
            print(f"Erro ao buscar {moeda}. Status: {response.status_code}")

    # Transforma a lista limpa em um DataFrame do Pandas
    return pd.DataFrame(dados_completos)

# FUNÇÃO DE CONEXÃO COM O BANCO
def get_database_engine():
    user = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    db = os.environ.get('DB_NAME')
    host = 'finances-postgres'
    port = '5432'
    
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(connection_string)


if __name__ == "__main__":    
    # Executa a extração + transformação
    df_historico = run_backfill()

    if not df_historico.empty:
        print(f"Transformação concluída! Preparando para inserir {len(df_historico)} linhas.")
        
        try:
            # Conecta e Carrega
            engine = get_database_engine()
            df_historico.to_sql('cambio_moedas', con=engine, if_exists='append', index=False)
            print("Carga Histórica finalizada! 1 Ano de dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar no banco: {e}")
    else:
        print("Nenhum dado foi processado.")