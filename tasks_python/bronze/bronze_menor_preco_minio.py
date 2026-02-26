import polars as pl
import requests
import time
import os
import io
import boto3
from datetime import datetime

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "bronze"

DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIRETORIO_SCRIPT, '..', '..'))
ARQUIVO_TERMOS = os.path.join(RAIZ_PROJETO, "dados", "produtos_cesta_basica.csv")
ARQUIVO_GEOHASHES = os.path.join(RAIZ_PROJETO, "dados", "municipios_pr_geohash.csv")
CAMINHO_GTINS_DESCOBERTOS = os.path.join(RAIZ_PROJETO, "dados", "gtins_descobertos.csv")
API_URL = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"

def gerar_variacoes(categoria, termo):
    """Gera variações inteligentes de pesos e volumes."""
    variacoes = [] 
    if categoria == "Grãos E Básicos":
        variacoes.extend([f"{termo} 1KG", f"{termo} 5KG", f"{termo} 500G"])
        
    elif categoria == "Óleos E Gorduras":
        if "MARGARINA" in termo or "MANTEIGA" in termo or "BANHA" in termo:
            variacoes.extend([f"{termo} 500G", f"{termo} 250G", termo])
        elif "AZEITE" in termo:
            variacoes.extend([f"{termo} 500ML", f"{termo} 250ML", termo])
        else: # Para Óleo de Soja, Milho, Girassol
            variacoes.extend([f"{termo} 900ML", termo])
            
    elif categoria in ["Farinhas E Milho", "Padaria E Biscoitos", "Massas"]:
        variacoes.extend([f"{termo} 500G", f"{termo} 1KG", f"{termo} 400G", f"{termo} 200G"])
        
    elif categoria == "Café E Leite":
        if "CAFE" in termo:
            variacoes.extend([f"{termo} 500G", f"{termo} 250G"])
        elif "LEITE" in termo and "PO" not in termo:
            variacoes.extend([f"{termo} 1L"])
        else:
            variacoes.append(termo)
            
    elif categoria == "Limpeza":
        variacoes.extend([f"{termo} 1KG", f"{termo} 500ML", f"{termo} 1L", f"{termo} 2L", f"{termo} 5L"])
        
    elif categoria == "Bebidas":
        variacoes.extend([f"{termo} 2L", f"{termo} 1.5L", f"{termo} 1L", f"{termo} 350ML", f"{termo} 500ML"])
        
    else:
        variacoes.append(termo)
        
    return variacoes

def main():
    print("🚀 Iniciando Extração Massiva (Com Parâmetro Corrigido)")
    
    # 1. Preparação
    df_referencia = pl.read_csv(ARQUIVO_TERMOS)
    linhas_referencia = df_referencia.to_dicts()
    total_termos = len(linhas_referencia) 
    
    df_geos = pl.read_csv(ARQUIVO_GEOHASHES)
    cidades_principais = ["Curitiba", "Londrina", "Maringá", "Cascavel", "Ponta Grossa", "Foz do Iguaçu", "São José dos Pinhais"]
    df_polos = df_geos.filter(pl.col("nome").is_in(cidades_principais))
    
    if df_polos.height == 0:
        print("❌ Erro nas cidades principais.")
        return

    lista_polos = df_polos.select(["nome", "geohash"]).to_dicts()
    todas_as_notas = []

    # 2. Coleta
    for polo in lista_polos: 
        cidade_nome = polo["nome"]
        geohash = polo["geohash"]
        
        print(f"\n🏙️ Região: {cidade_nome} (Geohash: {geohash})")
        print("=" * 40)
        
        for i, linha in enumerate(linhas_referencia, 1):
            categoria = linha.get("categoria", "Geral")
            termo_base = linha["descricao_busca"]
            variacoes = gerar_variacoes(categoria, termo_base)
            total_notas_base = 0
            
            print(f"🔍 [{i}/{total_termos}] {termo_base}...", end=" ")
            
            for busca in variacoes:
                offset = 0
                while offset < 5000: # Limite de segurança por variação
                    # ⚠️ A MÁGICA ACONTECE AQUI: Mudamos 'descricao' para 'termo'
                    params = {"termo": busca, "local": geohash, "raio": "20", "offset": offset}
                    
                    try:
                        r = requests.get(API_URL, params=params, timeout=15)
                        if r.status_code == 200:
                            dados = r.json().get("produtos", [])
                            if not dados: break
                            
                            for d in dados:
                                d['termo_origem'] = termo_base 
                                d['termo_buscado'] = busca
                                d['geohash_origem'] = geohash
                                d['cidade_origem'] = cidade_nome 
                            
                            todas_as_notas.extend(dados)
                            total_notas_base += len(dados)
                            
                            if len(dados) < 50: break
                            offset += 50
                            time.sleep(0.1) 
                        else: break
                    except: break
            
            print(f"✅ {total_notas_base} notas")

    if not todas_as_notas:
        print("\n⚠️ Nada coletado.")
        return

    # 3. Processamento
    print("\n🛠️ Processando e deduplicando...")
    df = pl.from_dicts(todas_as_notas)
    if "estabelecimento" in df.columns:
        df = df.unnest("estabelecimento")
    
    total_bruto = df.height
    df = df.unique(subset=["id"])
    total_unico = df.height

    print(f"📊 RESUMO: Bruto {total_bruto} | Único {total_unico}")

    # 4. GTINs
    if "gtin" in df.columns:
        novos_gtins = df.select("gtin").filter(pl.col("gtin") != "0").unique()
        if os.path.exists(CAMINHO_GTINS_DESCOBERTOS):
            try:
                antigos = pl.read_csv(CAMINHO_GTINS_DESCOBERTOS, infer_schema_length=10000)
                novos_gtins = novos_gtins.with_columns(pl.col("gtin").cast(pl.Utf8))
                antigos = antigos.with_columns(pl.col("gtin").cast(pl.Utf8))
                novos_gtins = pl.concat([antigos, novos_gtins]).unique()
            except Exception as e:
                pass
        novos_gtins.write_csv(CAMINHO_GTINS_DESCOBERTOS)

    # 5. MinIO
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    buffer.seek(0)
    
    agora = datetime.now()
    caminho = f"menor_preco/ano_hive={agora.year}/mes_hive={agora.month:02d}/dados_{agora.strftime('%H%M%S')}.parquet"
    
    try:
        s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
        s3.upload_fileobj(buffer, MINIO_BUCKET, caminho)
        print(f"📦 Parquet salvo no MinIO: {caminho}")
    except Exception as e:
        print(f"❌ Erro S3: {e}")

if __name__ == "__main__":
    main()