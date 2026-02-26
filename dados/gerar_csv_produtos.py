import polars as pl
import os

# --- DADOS BRUTOS (Seu TXT + Sugestões Novas) ---
raw_data = """
# --- Grãos e básicos ---
ARROZ
ARROZ BRANCO
ARROZ PARBOILIZADO
ARROZ INTEGRAL
ARROZ TIPO 1
ARROZ TIPO 2
FEIJAO
FEIJAO CARIOCA
FEIJAO PRETO
FEIJAO VERMELHO
FEIJAO FRADINHO
FEIJAO 1KG
FEIJAO 500G
ACUCAR
ACUCAR CRISTAL
ACUCAR REFINADO
ACUCAR MASCAVO
SAL
SAL REFINADO
SAL GROSSO

# --- Farinhas e milho ---
FARINHA TRIGO
FARINHA DE TRIGO
FARINHA MANDIOCA
FARINHA DE MANDIOCA
FUBA
FUBA MIMOSO
FLOCAO
MILHO FLOCADO
AMIDO MILHO
POLVILHO
POLVILHO DOCE
POLVILHO AZEDO

# --- Óleos e gorduras ---
OLEO SOJA
OLEO
OLEO GIRASSOL
OLEO MILHO
AZEITE
AZEITE OLIVA
MARGARINA
MANTEIGA
BANHA

# --- Café e leite ---
CAFE
CAFE TORRADO
CAFE MOIDO
CAFE 500G
CAFE 250G
CAFE SOLUVEL
LEITE
LEITE INTEGRAL
LEITE DESNATADO
LEITE SEMIDESNATADO
LEITE UHT
LEITE EM PO
ACHOCOLATADO
CREME DE LEITE
LEITE CONDENSADO

# --- Massas ---
MACARRAO
MACARRAO ESPAGUETE
MACARRAO PARAFUSO
MACARRAO PENNE
MACARRAO INSTANTANEO
MIOJO

# --- Proteínas ---
CARNE BOVINA
CARNE MOIDA
ACEM
PATINHO
COXAO MOLE
FRANGO
PEITO FRANGO
COXA FRANGO
SOBRECOXA
OVO
OVOS
DUZIA OVO
LINGUICA
LINGUICA CALABRESA
LINGUICA TOSCANA
SALSICHA
MORTADELA
PRESUNTO
MUSSARELA
QUEIJO
SARDINHA
ATUM

# --- Enlatados e molhos ---
MOLHO TOMATE
EXTRATO TOMATE
MILHO VERDE
ERVILHA
SELETA LEGUMES

# --- Hortifruti ---
BATATA
CEBOLA
ALHO
TOMATE
BANANA
MACA
LARANJA

# --- Limpeza ---
SABAO PO
SABAO EM PO
SABAO BARRA
DETERGENTE
DETERGENTE LIQUIDO
AMACIANTE
AGUA SANITARIA
DESINFETANTE
MULTIUSO
ESPONJA
ESPONJA ACO
SACO LIXO

# --- Higiene ---
CREME DENTAL
ESCOVA DENTAL
SABONETE
SHAMPOO
CONDICIONADOR
DESODORANTE
PAPEL HIGIENICO
ABSORVENTE
FRALDA
APARELHO BARBEAR
HASTES FLEXIVEIS
"""

def parse_txt_to_csv():
    data = []
    current_category = "Geral"
    
    # Processando linha por linha
    for line in raw_data.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        
        # Identifica a categoria
        if line.startswith("#"):
            current_category = line.replace("#", "").replace("-", "").strip().title()
            continue
            
        # Adiciona o produto à lista
        data.append({
            "categoria": current_category,
            "termo_busca": line
        })

    # Criando o DataFrame Polars
    df = pl.DataFrame(data)
    
    # Garantindo que a pasta 'dados' existe
    os.makedirs("../dados", exist_ok=True)
    output_path = "../dados/produtos_cesta_basica.csv"
    
    # Salvando em CSV
    df.write_csv(output_path)
    print(f"✅ CSV gerado com sucesso em: {output_path}")
    print(f"📊 Total de produtos: {df.height}")

if __name__ == "__main__":
    parse_txt_to_csv()