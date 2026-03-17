import requests
import os
import json
from datetime import datetime

#retirar variavel do ambiente
currencies = os.environ.get('CURRENCIES')

#verificar se a variavel existe
if not currencies:
    print("ERRO, nao foi encontrada a variavel currencies")
    
#variavel existe, extrair os dados
else:
    print(f"a variavel currencies foi encontrada. moedas a serem extraidas: {currencies}")
    url = f"https://economia.awesomeapi.com.br/last/{currencies}"
    
    #extrair os dados da url e salvar em um arquivo json
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/raw/currencies{timestamp}.json"
        
        os.makedirs("data/raw", exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"dados extraidos com sucesso. salvo como: {filename}")
    
    #erro na requisicao
    except Exception as e:
        print("falha na requisicao")
        
    
