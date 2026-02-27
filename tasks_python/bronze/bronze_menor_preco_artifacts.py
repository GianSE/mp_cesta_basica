import pandas as pd
import requests
import time
import os
from datetime import datetime

# Configuração de caminhos
DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIRETORIO_SCRIPT, '..', '..'))

ARQUIVO_EANS = os.path.join(RAIZ_PROJETO, "dados", "EANs.csv")
PASTA_SAIDA = os.path.join(RAIZ_PROJETO, "menor_preco")
GEOHASH_LISTA = ["6gge7u6cc", "6ggy66666"]
API_URL = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"

def main():
    print("🚀 Iniciando Extração Bronze (API -> GitHub Artifacts)")
    
    try:
        df_eans = pd.read_csv(ARQUIVO_EANS, sep=';', dtype=str)
        lista_eans = df_eans[df_eans.columns[0]].dropna().unique().tolist()
        print(f"📦 Carregados {len(lista_eans)} EANs do arquivo CSV.")
    except Exception as e:
        print(f"❌ Erro ao ler EANs: {e}")
        return

    todas_as_notas = []

    for hash_local in GEOHASH_LISTA:
        print(f"\n🌍 Região: {hash_local}")
        for i, gtin in enumerate(lista_eans, 1):
            offset = 0
            count_gtin = 0
            while True:
                params = {"gtin": gtin, "local": hash_local, "raio": "20", "offset": offset}
                try:
                    r = requests.get(API_URL, params=params, timeout=20)
                    if r.status_code == 200:
                        prod = r.json().get("produtos", [])
                        if prod:
                            for p in prod:
                                p['gtin_pesquisado'] = gtin
                                p['geohash_pesquisado'] = hash_local
                            todas_as_notas.extend(prod)
                            count_gtin += len(prod)
                            
                            if len(prod) == 50:
                                offset += 50
                                time.sleep(0.5)
                                continue
                    break
                except:
                    break
            if count_gtin > 0:
                print(f"  [{i}/{len(lista_eans)}] GTIN {gtin}: {count_gtin} notas encontradas.")
            time.sleep(0.5)

    if todas_as_notas:
        print("\n🛠️ Consolidando dados...")
        df_bronze = pd.json_normalize(todas_as_notas).astype(str)
        os.makedirs(PASTA_SAIDA, exist_ok=True)
        
        nome_arquivo = f"notas_bronze_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        # CORREÇÃO AQUI: PASTA_SAIDA em vez de PASTA_TEMP_LOCAL
        caminho_final = os.path.join(PASTA_SAIDA, nome_arquivo)
        
        df_bronze.to_parquet(caminho_final, index=False, compression='zstd')
        print(f"✅ Arquivo Parquet gerado com sucesso: {caminho_final}")
    else:
        print("⚠️ Nenhuma nota encontrada em nenhuma das regiões.")

if __name__ == "__main__":
    main()