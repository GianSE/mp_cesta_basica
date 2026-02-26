from datetime import datetime
import polars as pl
import requests
import time
import os
import io
from azure.storage.blob import BlobServiceClient 
from dotenv import load_dotenv

load_dotenv() 

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = "bronze" 

DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIRETORIO_SCRIPT, '..', '..'))
ARQUIVO_TERMOS = os.path.join(RAIZ_PROJETO, "dados", "produtos_cesta_basica.csv")
ARQUIVO_GEOHASHES = os.path.join(RAIZ_PROJETO, "dados", "municipios_pr_geohash.csv")
API_URL = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"

def testar_conexao_azure():
    print("☁️  Testando conexão com a Azure Blob Storage...", flush=True)
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER)
        if not container_client.exists(): return False
        print("✅ Conexão Azure OK!\n", flush=True)
        return True
    except: return False

def gerar_variacoes(categoria, termo):
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
    agora = datetime.now()
    dia_da_semana = agora.weekday() 
    
    print(f"🚀 Iniciando Pipeline Bronze - Fatiamento Dia {dia_da_semana + 1}/7", flush=True)
    
    if not testar_conexao_azure(): return 

    df_referencia = pl.read_csv(ARQUIVO_TERMOS)
    linhas_referencia = df_referencia.to_dicts()
    total_produtos_lista = len(linhas_referencia)
    
    df_geos = pl.read_csv(ARQUIVO_GEOHASHES)
    tamanho_fatia = 57
    inicio = dia_da_semana * tamanho_fatia
    
    df_lote = df_geos.slice(inicio, tamanho_fatia) if dia_da_semana < 6 else df_geos.slice(inicio)
    lista_cidades = df_lote.select(["nome", "geohash"]).to_dicts()
    
    print(f"📅 Hoje é {agora.strftime('%A')}. Processando {len(lista_cidades)} cidades.", flush=True)

    todas_as_notas = []


    # 3. Coleta Massiva
    try:
        for idx_cid, polo in enumerate(lista_cidades, 1): 
            cidade_nome = polo["nome"]
            geohash = polo["geohash"]
            print(f"\n🏙️  [{idx_cid}/{len(lista_cidades)}] Região: {cidade_nome}", flush=True)
            
            for i, linha in enumerate(linhas_referencia, 1):
                termo_base = linha["descricao_busca"]
                variacoes = gerar_variacoes(linha.get("categoria", "Geral"), termo_base)
                notas_termo = 0
                
                print(f"  🔍 [{i}/{total_produtos_lista}] {termo_base}...", end=" ", flush=True)
                
                for busca in variacoes:
                    offset = 0
                    continua_variacao = True # Controle para saber se desistimos da variação atual
                    
                    while offset < 500 and continua_variacao:
                        params = {"termo": busca, "local": geohash, "raio": "20", "offset": offset}
                        
                        # --- LÓGICA DE 5 RETRIES SEGUIDOS ---
                        sucesso_chamada = False
                        for tentativa in range(1, 6): # Tenta até 5 vezes seguidas
                            try:
                                r = requests.get(API_URL, params=params, timeout=20) # Timeout de 20s
                                
                                if r.status_code == 200:
                                    dados = r.json().get("produtos", [])
                                    if not dados:
                                        sucesso_chamada = True
                                        continua_variacao = False # Não tem mais páginas
                                        break
                                    
                                    for d in dados:
                                        d['termo_origem'] = termo_base 
                                        d['cidade_origem'] = cidade_nome 
                                        d['geohash_origem'] = geohash
                                    
                                    todas_as_notas.extend(dados)
                                    notas_termo += len(dados)
                                    
                                    if len(dados) < 50: 
                                        continua_variacao = False
                                    else:
                                        offset += 50
                                    
                                    sucesso_chamada = True
                                    time.sleep(0.05)
                                    break # Sucesso! Sai do loop de retry e continua o while
                                
                                else:
                                    # Erro de status (ex: 500, 502, 503)
                                    print(f"(Erro {r.status_code} na tent. {tentativa})", end=" ", flush=True)
                                    time.sleep(2 * tentativa) # Espera progressiva
                                    
                            except requests.exceptions.RequestException as e:
                                # Erro de conexão ou timeout
                                if tentativa == 5:
                                    print(f"⚠️ Falha definitiva após 5 erros seguidos no termo '{busca}'.", end=" ", flush=True)
                                    continua_variacao = False # Desiste dessa variação
                                    break
                                time.sleep(3 * tentativa) 
                        
                        if not sucesso_chamada: 
                            break # Se as 5 tentativas falharam, sai do while desse produto
                
                print(f"✅ {notas_termo} notas", flush=True)

    except KeyboardInterrupt:
        print("\n\n🛑 Interrupção manual (Ctrl+C). Salvando progresso...")
        
        print(f"Salvando o que foi coletado até agora ({len(todas_as_notas)} notas)...")

    if not todas_as_notas:
        print("\n⚠️ Nada coletado hoje.", flush=True)
        return

    # 4. Processamento e Upload
    print("\n🛠️ Deduplicando e preparando upload...", flush=True)
    df = pl.from_dicts(todas_as_notas)
    if "estabelecimento" in df.columns: df = df.unnest("estabelecimento")
    df = df.unique(subset=["id"])

    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    
    timestamp_arquivo = agora.strftime('%H%M')
    caminho_blob = (
        f"menor_preco/ano_hive={agora.year}/"
        f"mes_hive={agora.month:02d}/"
        f"dia_hive={agora.day:02d}/"
        f"fatia_{dia_da_semana}_{timestamp_arquivo}.parquet"
    )
    
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER, blob=caminho_blob)
        blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        print(f"\n📦 Sucesso! Arquivo salvo em: {caminho_blob}", flush=True)
    except Exception as e:
        print(f"❌ Erro upload: {e}", flush=True)

if __name__ == "__main__":
    main()