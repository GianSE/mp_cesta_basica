import polars as pl
import requests
import time
import os

# --- CONFIGURAÇÕES DE CAMINHO ---
DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIRETORIO_SCRIPT, '..'))

ARQUIVO_TERMOS = os.path.join(RAIZ_PROJETO, "dados", "produtos_cesta_basica.csv")
ARQUIVO_SAIDA = os.path.join(RAIZ_PROJETO, "dados", "dicionario_gtins_cesta.csv")

OPENFOOD_API_URL = "https://world.openfoodfacts.org/cgi/search.pl"

def gerar_variacoes(categoria, termo):
    """
    Gera variações de busca baseadas na categoria para capturar 
    diferentes GTINs de pesos e volumes específicos.
    """
    # Sempre inclui o termo original genérico
    variacoes = [termo] 
    
    if categoria == "Grãos E Básicos":
        variacoes.extend([f"{termo} 1KG", f"{termo} 5KG", f"{termo} 500G"])
        
    elif categoria == "Óleos E Gorduras":
        variacoes.extend([f"{termo} 900ML", f"{termo} 500ML"])
        
    elif categoria in ["Farinhas E Milho", "Padaria E Biscoitos", "Massas"]:
        variacoes.extend([f"{termo} 500G", f"{termo} 1KG", f"{termo} 400G", f"{termo} 200G"])
        
    elif categoria == "Café E Leite":
        if "CAFE" in termo:
            variacoes.extend([f"{termo} 500G", f"{termo} 250G"])
        elif "LEITE" in termo and "PO" not in termo:
            variacoes.extend([f"{termo} 1L"])
            
    elif categoria == "Limpeza":
        variacoes.extend([f"{termo} 1KG", f"{termo} 500ML", f"{termo} 1L", f"{termo} 2L"])
        
    elif categoria == "Bebidas":
        variacoes.extend([f"{termo} 2L", f"{termo} 1.5L", f"{termo} 1L", f"{termo} 350ML"])
        
    return variacoes

def main():
    print("🚀 Iniciando Coleta Massiva de GTINs (Com Expansão de Termos)...")
    
    # 1. Carrega sua lista de termos
    if not os.path.exists(ARQUIVO_TERMOS):
        print(f"❌ Erro: Arquivo {ARQUIVO_TERMOS} não encontrado.")
        return
        
    try:
        df_termos = pl.read_csv(ARQUIVO_TERMOS, separator=",")
        df_termos = df_termos.rename({col: col.strip() for col in df_termos.columns})
        
        if len(df_termos.columns) == 1:
            df_termos = pl.read_csv(ARQUIVO_TERMOS, separator=";")
            df_termos = df_termos.rename({col: col.strip() for col in df_termos.columns})
            
        if "descricao_busca" not in df_termos.columns:
            print(f"❌ Erro Crítico: Colunas encontradas: {df_termos.columns}")
            return
            
        linhas = df_termos.to_dicts()
        total_produtos_base = len(linhas)
        
    except Exception as e:
        print(f"❌ Erro ao ler o CSV: {e}")
        return
    
    print(f"✅ Arquivo lido. Total de produtos base: {total_produtos_base}")
    print("-" * 50)
    
    # 2. Prepara o arquivo de saída
    os.makedirs(os.path.dirname(ARQUIVO_SAIDA), exist_ok=True)
    arquivo_existe = os.path.exists(ARQUIVO_SAIDA) and os.path.getsize(ARQUIVO_SAIDA) > 0
    if not arquivo_existe:
        with open(ARQUIVO_SAIDA, "w", encoding="utf-8") as f:
            f.write("gtin,descricao_api,categoria,termo_busca\n")
            
    total_gtins_baixados = 0

    # 3. Loop de Consulta com Variações
    for i, linha in enumerate(linhas, 1):
        categoria = linha.get("categoria", "Geral")
        termo_base = linha["descricao_busca"]
        
        lista_buscas = gerar_variacoes(categoria, termo_base)
        print(f"\n📦 [{i}/{total_produtos_base}] Categoria: {categoria} | Base: {termo_base}")
        
        for busca in lista_buscas:
            print(f"  🔍 Buscando variação: '{busca}'...", end=" ")
            
            params = {
                "search_terms": busca,
                "search_simple": "1",
                "action": "process",
                "json": "1",
                "page_size": "250", 
                "countries_tags_en": "brazil"
            }
            
            headers = {"User-Agent": "ComparaTudo_App/1.0 - Projeto_Academico"}
            
            try:
                r = requests.get(OPENFOOD_API_URL, params=params, headers=headers, timeout=30)
                
                if r.status_code == 200:
                    produtos = r.json().get("products", [])
                    lote_parcial = []
                    
                    for p in produtos:
                        codigo_gtin = p.get("code", "")
                        nome_produto = p.get("product_name_pt") or p.get("product_name", "Nome Indisponível")
                        
                        if codigo_gtin and len(codigo_gtin) >= 8:
                            lote_parcial.append({
                                "gtin": codigo_gtin,
                                "descricao_api": nome_produto.strip(),
                                "categoria": categoria,
                                "termo_busca": termo_base # Salvamos o termo base para facilitar o agrupamento depois
                            })
                    
                    if lote_parcial:
                        df_lote = pl.DataFrame(lote_parcial, schema=["gtin", "descricao_api", "categoria", "termo_busca"])
                        with open(ARQUIVO_SAIDA, "a", encoding="utf-8") as f:
                            df_lote.write_csv(f, include_header=False)
                            
                        total_gtins_baixados += len(lote_parcial)
                        print(f"✅ {len(lote_parcial)} salvos.")
                    else:
                        print(f"⚪ 0 encontrados.")
                else:
                    print(f"⚠️ Erro HTTP {r.status_code}")
                    
            except Exception as e:
                print(f"❌ Timeout/Erro.")
                
            time.sleep(1.5)

    # 4. Limpeza Final (Deduplicação)
    print("\n🛠️ Coleta finalizada! Removendo duplicatas do arquivo...")
    if os.path.exists(ARQUIVO_SAIDA):
        df_final = pl.read_csv(ARQUIVO_SAIDA)
        total_bruto = df_final.height
        
        df_limpo = df_final.unique(subset=["gtin"], keep="first")
        total_limpo = df_limpo.height
        
        df_limpo.write_csv(ARQUIVO_SAIDA)
        
        print("="*50)
        print(f"🎉 DICIONÁRIO PRONTO COM EXPANSÃO DE TERMOS!")
        print(f"📊 Registros brutos baixados: {total_bruto}")
        print(f"💎 GTINs únicos finais: {total_limpo} (Removidos {total_bruto - total_limpo} duplicados)")
        print(f"📂 Salvo em: {ARQUIVO_SAIDA}")
        print("="*50)

if __name__ == "__main__":
    main()