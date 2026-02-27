from datetime import datetime
import polars as pl
import requests
import time
import os
import io
import threading
from azure.storage.blob import BlobServiceClient 
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv() 

# --- CONFIGURAÇÕES ---
STORAGE_PROVIDER = os.getenv("STORAGE_PROVIDER") # azure ou minio

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = "bronze"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = "bronze"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIRETORIO_SCRIPT, '..', '..'))
ARQUIVO_TERMOS = os.path.join(RAIZ_PROJETO, "dados", "produtos_cesta_basica.csv")
ARQUIVO_GEOHASHES = os.path.join(RAIZ_PROJETO, "dados", "municipios_pr_geohash.csv")
API_URL = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"

# Criando o "botão de pânico" para as threads
evento_parada = threading.Event()

# --- FUNÇÕES DE INFRAESTRUTURA E REGRA DE NEGÓCIO ---

def obter_cliente_minio():
    return boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)


def testar_conexao_storage():
    if STORAGE_PROVIDER == "minio":
        print("🪣  Testando conexão com o MinIO...", flush=True)
        try:
            s3_client = obter_cliente_minio()
            s3_client.head_bucket(Bucket=MINIO_BUCKET)
            print("✅ Conexão MinIO OK!\n", flush=True)
            return True
        except Exception as e:
            print(f"❌ Erro MinIO: {e}", flush=True)
            return False
    else:
        print("☁️  Testando conexão com a Azure Blob Storage...", flush=True)
        try:
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
            container_client = blob_service_client.get_container_client(AZURE_CONTAINER)
            if not container_client.exists(): 
                print(f"❌ Erro Azure: O container não existe.", flush=True)
                return False
            print("✅ Conexão Azure OK!\n", flush=True)
            return True
        except Exception as e: 
            print(f"❌ Erro ao conectar na Azure: {e}", flush=True)
            return False

def enviar_alerta_telegram(mensagem):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Credenciais do Telegram não encontradas no .env. Pulando envio.", flush=True)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": mensagem,
        "parse_mode": "Markdown" # Permite usar negrito, itálico, etc.
    }
    try:
        requests.post(url, json=payload, timeout=10)
        print("📱 Notificação enviada para o Telegram!", flush=True)
    except Exception as e:
        print(f"⚠️ Erro ao enviar mensagem para o Telegram: {e}", flush=True)

def gerar_variacoes(categoria, termo):
    variacoes = [] 
    if categoria == "Grãos E Básicos": variacoes.extend([f"{termo} 1KG", f"{termo} 5KG", f"{termo} 500G"])
    elif categoria == "Óleos E Gorduras":
        if "MARGARINA" in termo or "MANTEIGA" in termo or "BANHA" in termo: variacoes.extend([f"{termo} 500G", f"{termo} 250G", termo])
        elif "AZEITE" in termo: variacoes.extend([f"{termo} 500ML", f"{termo} 250ML", termo])
        else: variacoes.extend([f"{termo} 900ML", termo])
    elif categoria in ["Farinhas E Milho", "Padaria E Biscoitos", "Massas"]: variacoes.extend([f"{termo} 500G", f"{termo} 1KG", f"{termo} 400G", f"{termo} 200G"])
    elif categoria == "Café E Leite":
        if "CAFE" in termo: variacoes.extend([f"{termo} 500G", f"{termo} 250G"])
        elif "LEITE" in termo and "PO" not in termo: variacoes.extend([f"{termo} 1L"])
        else: variacoes.append(termo)
    elif categoria == "Limpeza": variacoes.extend([f"{termo} 1KG", f"{termo} 500ML", f"{termo} 1L", f"{termo} 2L", f"{termo} 5L"])
    elif categoria == "Bebidas": variacoes.extend([f"{termo} 2L", f"{termo} 1.5L", f"{termo} 1L", f"{termo} 350ML", f"{termo} 500ML"])
    else: variacoes.append(termo)
    return variacoes

def processar_e_salvar_lote(dados_lote, dia_da_semana, numero_lote):
    if not dados_lote:
        return True # Retorna True para não travar se estiver vazio
        
    print(f"\n🛠️ Preparando upload do Lote {numero_lote} ({len(dados_lote)} notas)...", flush=True)
    
    # Deduplicação
    df = pl.from_dicts(dados_lote)
    if "estabelecimento" in df.columns: 
        df = df.unnest("estabelecimento")
    df = df.unique(subset=["id"])

    # Compressão
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    
    agora = datetime.now()
    timestamp_arquivo = agora.strftime('%H%M')
    
    caminho_blob = (
        f"menor_preco/ano_hive={agora.year}/"
        f"mes_hive={agora.month:02d}/"
        f"dia_hive={agora.day:02d}/"
        f"fatia_{dia_da_semana + 1}_{timestamp_arquivo}_lote_{numero_lote}.parquet"
    )
    
    # Adicionando sistema de retries para a nuvem
    for tentativa in range(1, 4): # Tenta até 3 vezes
        try:
            if STORAGE_PROVIDER == "minio":
                s3_client = obter_cliente_minio()
                s3_client.put_object(Bucket=MINIO_BUCKET, Key=caminho_blob, Body=buffer.getvalue())
                print(f"📦 Lote {numero_lote} salvo no MINIO: {caminho_blob}", flush=True)
            else:
                blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
                blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER, blob=caminho_blob)
                blob_client.upload_blob(buffer.getvalue(), overwrite=True)
                print(f"📦 Lote {numero_lote} salvo na AZURE: {caminho_blob}", flush=True)
            
            return True # Sucesso! Sai da função e retorna True
            
        except Exception as e:
            print(f"⚠️ Erro no upload (Tentativa {tentativa}/3): {e}", flush=True)
            time.sleep(20 * tentativa) # Espera 5s, depois 10s...
            
    print(f"❌ FALHA CRÍTICA: Não foi possível salvar o Lote {numero_lote} na nuvem.", flush=True)
    return False # Falhou todas as vezes

# --- FUNÇÃO ISOLADA PARA A THREAD (WORKER) ---

def extrair_dados_variacao(sessao, busca, geohash, termo_base, cidade_nome):
    notas_coletadas = []
    offset = 0
    continua_variacao = True

    while offset < 500 and continua_variacao:
        # Se o botão de pânico foi apertado, a thread desiste de continuar
        if evento_parada.is_set():
            break

        params = {"termo": busca, "local": geohash, "raio": "20", "offset": offset}
        sucesso_chamada = False
        
        for tentativa in range(1, 6): 
            # Checa novamente antes de fazer a requisição
            if evento_parada.is_set():
                break

            try:
                r = sessao.get(API_URL, params=params, timeout=20) 
                
                if r.status_code == 200:
                    dados = r.json().get("produtos", [])
                    if not dados:
                        sucesso_chamada = True
                        continua_variacao = False 
                        break
                    
                    for d in dados:
                        d['termo_origem'] = termo_base 
                        d['cidade_origem'] = cidade_nome 
                        d['geohash_origem'] = geohash
                    
                    notas_coletadas.extend(dados)
                    
                    if len(dados) < 50: 
                        continua_variacao = False
                    else:
                        offset += 50
                    
                    sucesso_chamada = True
                    # Usa um sleep pequeno que pode ser interrompido
                    evento_parada.wait(0.05) 
                    break 
                
                elif r.status_code == 429: 
                    evento_parada.wait(5 * tentativa) # Pausa amigável que obedece o Ctrl+C
                
                else:
                    evento_parada.wait(2 * tentativa) 
                    
            except requests.exceptions.RequestException:
                if tentativa == 5:
                    continua_variacao = False 
                    break
                evento_parada.wait(20 * tentativa) 
        
        if not sucesso_chamada: 
            break 
            
    return notas_coletadas


# --- FLUXO PRINCIPAL ---

def main():
    tempo_inicio = time.time()
    total_notas_dia = 0
    agora = datetime.now()
    dia_da_semana = agora.weekday() 
    
    print(f"🚀 Iniciando Pipeline Bronze (Paralelizado) - Fatiamento Dia {dia_da_semana + 1}/7", flush=True)
    print(f"🔧 Provedor: {STORAGE_PROVIDER.upper()}", flush=True)
    if not testar_conexao_storage(): return 

    df_referencia = pl.read_csv(ARQUIVO_TERMOS)
    linhas_referencia = df_referencia.to_dicts()
    
    df_geos = pl.read_csv(ARQUIVO_GEOHASHES)
    tamanho_fatia = 57
    inicio = dia_da_semana * tamanho_fatia
    df_lote = df_geos.slice(inicio, tamanho_fatia) if dia_da_semana < 6 else df_geos.slice(inicio)
    lista_cidades = df_lote.select(["nome", "geohash"]).to_dicts()
    
    print(f"📅 Processando {len(lista_cidades)} cidades.", flush=True)

    tarefas = []
    for polo in lista_cidades:
        for linha in linhas_referencia:
            termo_base = linha["descricao_busca"]
            variacoes = gerar_variacoes(linha.get("categoria", "Geral"), termo_base)
            for busca in variacoes:
                tarefas.append((busca, polo["geohash"], termo_base, polo["nome"]))

    print(f"📋 Total de requisições base mapeadas: {len(tarefas)}", flush=True)
    print("⚡ Iniciando extração massiva. Por favor, aguarde...", flush=True)

    todas_as_notas = []
    
    # Variáveis de controle de lote
    TAMANHO_DO_LOTE = 1000
    numero_lote = 1
    
    # 2. Execução Paralela
    with requests.Session() as sessao:
        # Passando o max_workers direto
        executor = ThreadPoolExecutor(max_workers=5)
        futuros = {
            executor.submit(extrair_dados_variacao, sessao, t[0], t[1], t[2], t[3]): t 
            for t in tarefas
        }
        
        try:
            tarefas_concluidas = 0
            for futuro in as_completed(futuros):
                resultado = futuro.result()
                if resultado:
                    todas_as_notas.extend(resultado)
                    total_notas_dia += len(resultado) # <--- SOMA AQUI!
                
                tarefas_concluidas += 1
                
                # Feedback visual menor a cada 50
                if tarefas_concluidas % 50 == 0 or tarefas_concluidas == len(tarefas):
                    print(f"🔄 Progresso: {tarefas_concluidas}/{len(tarefas)} buscas... Notas no buffer: {len(todas_as_notas)}", flush=True)

                # --- LÓGICA DE CHECKPOINT ---
                if tarefas_concluidas % TAMANHO_DO_LOTE == 0:
                    print(f"\n⚠️ Atingiu {tarefas_concluidas} buscas. Salvando checkpoint do Lote {numero_lote}...")
                    sucesso_upload = processar_e_salvar_lote(todas_as_notas, dia_da_semana, numero_lote)
                    
                    if sucesso_upload:
                        # Limpa a memória apenas se o upload deu certo
                        todas_as_notas.clear()
                        numero_lote += 1
                    else:
                        print("⚠️ Retendo dados na memória para tentar enviar junto com o próximo lote...", flush=True)

        except KeyboardInterrupt:
            print("\n\n🛑 Interrupção manual (Ctrl+C) detectada! Cancelando threads pendentes...")
            evento_parada.set() # Aperta o botão de pânico
            # Cancela tudo o que ainda não começou a rodar na fila
            executor.shutdown(wait=False, cancel_futures=True)

    # 3. Processamento Final (Resíduo)
    if todas_as_notas:
        if evento_parada.is_set():
            print("\n⚠️ Salvando os dados residuais coletados antes do cancelamento...")
        else:
            print("\n✅ Extração massiva concluída. Salvando último lote residual...")
            
        processar_e_salvar_lote(todas_as_notas, dia_da_semana, numero_lote)
    else:
        # Só avisa que não tem nada se realmente não salvou nenhum lote antes
        if numero_lote == 1 and not evento_parada.is_set():
            print("\n⚠️ Nada coletado hoje.", flush=True)

    # Calcula o tempo total em minutos
    tempo_fim = time.time()
    minutos_processamento = round((tempo_fim - tempo_inicio) / 60, 2)
    
    # Mapeia o nome do dia da semana
    nomes_dias = ["Segunda-Feira", "Terça-Feira", "Quarta-Feira", "Quinta-Feira", "Sexta-Feira", "Sábado", "Domingo"]
    nome_dia_atual = nomes_dias[dia_da_semana]
    
    # Lógica simples para saber quantos lotes foram salvos de verdade
    qtd_lotes_salvos = numero_lote if todas_as_notas else numero_lote - 1
    if qtd_lotes_salvos == 0 and not todas_as_notas:
        qtd_lotes_salvos = 0 # Prevenção se nada rodar

    # Monta a mensagem formatada
    mensagem_telegram = f"""✅ *Extração Menor Preço concluída.*
⏱️ tempo de processamento: {minutos_processamento} min
🧾 notas: {total_notas_dia}
📍 geohashs: {len(lista_cidades)}
🍰 fatia: {dia_da_semana + 1} ({nome_dia_atual})
📦 lotes enviados: {qtd_lotes_salvos}
☁️ provedor: {STORAGE_PROVIDER.lower()}
📁 repositório: `mp_cesta_basica`"""

    print(f"\n🏁 Fim do dia! Foram avaliadas {len(lista_cidades)} cidades e coletadas {total_notas_dia} notas no total.")
    
    # Dispara a mensagem!
    enviar_alerta_telegram(mensagem_telegram)

if __name__ == "__main__":
    main()