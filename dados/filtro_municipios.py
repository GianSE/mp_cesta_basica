import pandas as pd

# Arquivo de entrada
input_file = "municipios.csv"

# Arquivo de saída
output_file = "municipios_pr.csv"

# Código da UF do Paraná
CODIGO_UF_PR = 41

# Lê o CSV
df = pd.read_csv(input_file)

# Filtra apenas Paraná
df_pr = df[df["codigo_uf"] == CODIGO_UF_PR]

# Salva o novo CSV
df_pr.to_csv(output_file, index=False)

print(f"Arquivo gerado com sucesso: {output_file}")
print(f"Total de municípios do PR: {len(df_pr)}")