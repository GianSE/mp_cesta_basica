import pandas as pd
import pygeohash as pgh

# Arquivos
input_file = "municipios_pr.csv"
output_file = "municipios_pr_geohash.csv"

# Precisão do geohash (5 a 7 é bom para nível de cidade)
PRECISAO = 6

# Lê o CSV
df = pd.read_csv(input_file)

# Garante que latitude e longitude são números
df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

# Remove linhas inválidas (se houver)
df = df.dropna(subset=["latitude", "longitude"])

# Função para gerar geohash
def gerar_geohash(row):
    return pgh.encode(row["latitude"], row["longitude"], precision=PRECISAO)

# Cria a coluna geohash
df["geohash"] = df.apply(gerar_geohash, axis=1)

# Salva o resultado
df.to_csv(output_file, index=False)

print(f"Arquivo gerado: {output_file}")
print(df[["nome", "latitude", "longitude", "geohash"]].head())