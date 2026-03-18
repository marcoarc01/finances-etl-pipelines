import pandas as pd
import json
import glob
import os
from datetime import datetime

# localizar o arquivo mais recente
def get_latest_raw_file():
    files = glob.glob("data/raw/*.json")
    
    if not files:
        print("Nenhum arquivo encontrado na pasta data/raw/")
        return None

    return max(files, key=os.path.getctime)

    
# Carregar o JSON e transformar em DataFrame
def load_data(file_path):
    # ler o JSON e virar um DataFrame
    if file_path is None:
        print("Nenhum arquivo para carregar.")
        return None
    
    with open(file_path, 'r') as f:
        json_data = json.load(f)
    
    # transformamos o dicionário em DataFrame
    df = pd.DataFrame.from_dict(json_data, orient='index')
    
    return df

# Transformação dos dados
def transform_data(df):
    if df is None:
        print("Nenhum DataFrame para transformar.")
        return None
    
    print("iniciando a transformação dos dados")
    
    df = df.reset_index()

    df = df[['index', 'name', 'bid', 'pctChange', 'create_date']]
    
    # renomear as colunas
    df.columns = ['moeda_sigla', 'nome_moeda', 'cotacao', 'variacao', 'data_cotacao']
    
    # converter os tipos de dados
    df['cotacao'] = pd.to_numeric(df['cotacao'])
    df['variacao'] = pd.to_numeric(df['variacao'])
    df['data_cotacao'] = pd.to_datetime(df['data_cotacao'])
    df['data_processamento'] = datetime.now()

    return df


# Salvar na camada processed
def save_processed_data(df):
    if df is None:
        print("Nenhum DataFrame para salvar.")
        return
    
    # verifica se existe a pasta
    os.makedirs("data/processed", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/processed/dados_transformados_{timestamp}.csv"
    
    # salvar o DataFrame como CSV
    df.to_csv(filename, index=False)
    print(f"Dados transformados salvos com sucesso em: {filename}")



if __name__ == "__main__":
    print("Iniciando a transformação dos dados")
    
    arquivo = get_latest_raw_file()
    
    df_raw = load_data(arquivo)

    df_load = transform_data(df_raw)

    save_processed_data(df_load)
    
    print("Transformação concluída com sucesso!")