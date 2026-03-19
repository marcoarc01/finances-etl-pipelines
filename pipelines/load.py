import pandas as pd
import glob
import os
from sqlalchemy import create_engine

# Localizar o arquivo processado mais recente
def get_latest_processed_file():
    files = glob.glob("data/processed/*.csv")
    
    if not files:
        print("Nenhum arquivo encontrado na pasta data/processed/")
        return None

    arquivo_mais_novo = max(files)
    
    print(f"Arquivo selecionado para carga: {arquivo_mais_novo}")
    return arquivo_mais_novo

# Criar a conexão com o PostgreSQL
def get_database_engine():
    user = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    host = 'finances-postgres'
    port = '5432'
    db = os.environ.get('DB_NAME')

    # Montamos a URI
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    
    engine = create_engine(connection_string)
    return engine


# Carregar o dado no Banco de Dados
def load_data_to_postgres(df, engine):
    if df is None:
        print("DataFrame vazio")
        return

    try:
        # histórico de moedas
        df.to_sql('cambio_moedas', con=engine, if_exists='append', index=False)
        print(f"{len(df)} linhas inseridas na tabela 'cambio_moedas'!")

    except Exception as e:
        print(f" Erro ao carregar no banco: {e}")



if __name__ == "__main__":
    print("Iniciando a carga dos dados no PostgreSQL.")
    
    arquivo_csv = get_latest_processed_file()
    if arquivo_csv:
        df = pd.read_csv(arquivo_csv)
        df['data_cotacao'] = pd.to_datetime(df['data_cotacao'])
        df['data_processamento'] = pd.to_datetime(df['data_processamento'])
    
    
    engine = get_database_engine()
    
    load_data_to_postgres(df, engine)

    print("Carga finalizada! Dados disponíveis no banco.")
