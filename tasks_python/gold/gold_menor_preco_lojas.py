import pandas as pd
import requests
import time
import os
import glob
from datetime import datetime

# --- CONFIGURAÇÕES ---
PASTA_BRONZE_NOTAS = "dados_lake/bronze/notas"
PASTA_BRONZE_LOJAS = "dados_lake/bronze/lojas"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "ComparaTudoApp/1.0 (seu_email@exemplo.com)" # Substitua pelo seu email se quiser

def formatar_endereco(row):
    """
    Monta a string de endereço baseada nas colunas que vieram achatadas do JSON.
    As colunas geradas pelo pd.json_normalize têm o prefixo 'estabelecimento.'
    """
    tp_logr = str(row.get('estabelecimento.tp_logr', '')).strip()
    nm_logr = str(row.get('estabelecimento.nm_logr', '')).strip()
    nr_logr = str(row.get('estabelecimento.nr_logr', '')).strip()
    bairro = str(row.get('estabelecimento.bairro', '')).strip()
    uf = str(row.get('estabelecimento.uf', '')).strip()
    
    # Trata valores nulos ou strings 'nan'
    tp_logr = "" if tp_logr.lower() == 'nan' else tp_logr
    nm_logr = "" if nm_logr.lower() == 'nan' else nm_logr
    nr_logr = "" if nr_logr.lower() == 'nan' else nr_logr
    bairro = "" if bairro.lower() == 'nan' else bairro
    uf = "" if uf.lower() == 'nan' else uf

    # Monta padrão: "RUA NOME, NUMERO, BAIRRO, UF, BRASIL"
    partes = []
    rua = f"{tp_logr} {nm_logr}".strip()
    
    if rua: partes.append(rua)
    if nr_logr: partes.append(nr_logr)
    if bairro: partes.append(bairro)
    if uf: partes.append(uf)
    partes.append("BRASIL") # Ajuda o Nominatim a não se perder
    
    return ", ".join(partes).replace(" ,", ",")

def buscar_coordenadas(endereco):
    """Faz a requisição na API do Nominatim."""
    if not endereco or len(endereco) < 10:
        return None, None
        
    params = {
        "q": endereco,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": 1,
    }
    headers = {"User-Agent": USER_AGENT}
    
    try:
        resposta = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=15)
        if resposta.status_code == 200:
            dados = resposta.json()
            if isinstance(dados, list) and len(dados) > 0:
                return str(dados[0].get("lat")), str(dados[0].get("lon"))
    except Exception as e:
        print(f"Erro na API ({endereco}): {e}")
        
    return None, None

def main():
    print("🚀 Iniciando Enriquecimento de Lojas (Nominatim)")
    
    # 1. Encontra o arquivo parquet mais recente na pasta Bronze
    arquivos = glob.glob(os.path.join(PASTA_BRONZE_NOTAS, "*.parquet"))
    if not arquivos:
        print("❌ Nenhum arquivo de notas encontrado na Bronze para processar.")
        return
        
    arquivo_recente = max(arquivos, key=os.path.getmtime)
    print(f"📦 Lendo arquivo: {arquivo_recente}")
    
    df_notas = pd.read_parquet(arquivo_recente)
    
    # Verifica se a coluna CNPJ existe (chave primária da loja)
    coluna_cnpj = 'estabelecimento.cnpj'
    if coluna_cnpj not in df_notas.columns:
        print(f"❌ Coluna {coluna_cnpj} não encontrada. Impossível extrair lojas.")
        return

    # 2. Extrai apenas as lojas únicas do arquivo de notas
    # Remove as duplicatas baseadas no CNPJ e pega a primeira ocorrência
    df_lojas = df_notas.drop_duplicates(subset=[coluna_cnpj]).copy()
    print(f"🏪 Encontradas {len(df_lojas)} lojas únicas. Formatando endereços...")
    
    # 3. Cria a coluna de endereço formatado
    df_lojas['endereco_busca'] = df_lojas.apply(formatar_endereco, axis=1)
    
    # Adicionando colunas vazias para preencher depois
    df_lojas['latitude'] = None
    df_lojas['longitude'] = None
    
    # 4. Loop batendo na API (com controle de tempo)
    sucessos = 0
    for index, row in df_lojas.iterrows():
        endereco = row['endereco_busca']
        cnpj = row[coluna_cnpj]
        
        print(f"Buscando [{cnpj}] -> {endereco}...", end=" ")
        
        lat, lon = buscar_coordenadas(endereco)
        
        if lat and lon:
            df_lojas.at[index, 'latitude'] = lat
            df_lojas.at[index, 'longitude'] = lon
            sucessos += 1
            print(f"✅ {lat}, {lon}")
        else:
            print("❌ Não encontrado")
            
        # OBRIGATÓRIO: A API do Nominatim bane IPs que fazem mais de 1 req por segundo
        time.sleep(1.5)

    # 5. Salvando o resultado na nova pasta
    os.makedirs(PASTA_BRONZE_LOJAS, exist_ok=True)
    hoje = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_salvar = os.path.join(PASTA_BRONZE_LOJAS, f"lojas_raw_{hoje}.parquet")
    
    # Salva garantindo que é tudo string (VARCHAR)
    df_lojas = df_lojas.astype(str)
    df_lojas.to_parquet(caminho_salvar, index=False)
    
    print("\n" + "="*50)
    print(f"🏁 Processamento finalizado!")
    print(f"📊 Lojas com coordenadas encontradas: {sucessos}/{len(df_lojas)}")
    print(f"💾 Arquivo salvo em: {caminho_salvar}")

if __name__ == "__main__":
    main()