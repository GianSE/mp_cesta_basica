import polars as pl
import requests
import time
import os
import io
from datetime import datetime
from azure.storage.blob import BlobServiceClient 
from dotenv import load_dotenv  # Adicione isso

# Carrega as variáveis do arquivo .env (se ele existir)
load_dotenv() 

# Agora o os.getenv vai funcionar tanto no seu PC quanto no GitHub
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = "bronze" 

DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIRETORIO_SCRIPT, '..', '..'))
ARQUIVO_TERMOS = os.path.join(RAIZ_PROJETO, "dados", "produtos_cesta_basica.csv")
ARQUIVO_GEOHASHES = os.path.join(RAIZ_PROJETO, "dados", "municipios_pr_geohash.csv")
API_URL = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"

def testar_conexao_azure():
    """Testa se a credencial é válida e se o container existe antes de rodar a extração."""
    print("☁️  Testando conexão com a Azure Blob Storage...")
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER)
        
        if not container_client.exists():
            print(f"❌ Erro Crítico: O container '{AZURE_CONTAINER}' NÃO FOI ENCONTRADO na sua Storage Account.")
            print("Crie o container na Azure antes de rodar o script.")
            return False
            
        print("✅ Conexão com a Azure estabelecida com sucesso! Container validado.\n")
        return True
    except Exception as e:
        print(f"❌ Erro de Autenticação na Azure: {e}")
        return False

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
        else: 
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
    print("🚀 Iniciando Pipeline Bronze (Destino: AZURE CLOUD)")
    print("=" * 50)
    
    # 0. Teste de Conexão Fail-Fast
    if not testar_conexao_azure():
        return # Aborta o script se a Azure não responder
    
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
                while offset < 5000:
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

    # 4. UPLOAD PARA A AZURE
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    
    agora = datetime.now()
    caminho_blob = f"menor_preco/ano_hive={agora.year}/mes_hive={agora.month:02d}/dados_{agora.strftime('%H%M%S')}.parquet"
    
    print("\n📤 Iniciando upload para a Azure Blob Storage...")
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER, blob=caminho_blob)
        
        blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        
        print(f"📦 Sucesso absoluto! Parquet salvo na nuvem:")
        print(f"   URL Lógica: azure://{AZURE_CONTAINER}/{caminho_blob}")
    except Exception as e:
        print(f"❌ Erro fatal no momento do upload: {e}")

if __name__ == "__main__":
    main()