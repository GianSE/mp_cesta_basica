# � MP Cesta Básica

Pipeline de dados em Python para coleta e análise de preços de produtos da **cesta básica** no estado do Paraná, utilizando a API pública do **Menor Preço (Nota Paraná)**.

O projeto segue a **Arquitetura Medallion** (Bronze → Silver → Gold) e suporta múltiplos backends de armazenamento: **Azure Blob Storage**, **MinIO (S3)** e **arquivos locais (Parquet)**.

---

## 🧭 Sumário

- [Resumo](#-resumo)
- [Arquitetura](#-arquitetura)
- [APIs Utilizadas](#-apis-utilizadas)
- [Como Usar](#-como-usar)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Scripts de Dados](#-scripts-de-dados-dados)
- [DevOps e Tooling](#%EF%B8%8F-devops-e-tooling-_ops)

---

## 🎯 Resumo

O objetivo é monitorar e comparar preços de ~120 produtos essenciais (arroz, feijão, café, óleo, etc.) em **7 grandes cidades do Paraná**: Curitiba, Londrina, Maringá, Cascavel, Ponta Grossa, Foz do Iguaçu e São José dos Pinhais.

O pipeline:

1. **Extrai** dados da API do Menor Preço buscando por termos de produtos com variações de peso/volume (ex: "ARROZ 1KG", "ARROZ 5KG").
2. **Pagina** até 5.000 resultados por variação de produto, cobrindo múltiplos municípios via geohash.
3. **Desaninha** o JSON de estabelecimentos, **deduplica** por ID e armazena como **Parquet comprimido (zstd)** particionado no formato Hive (`ano_hive=YYYY/mes_hive=MM/`).
4. **Enriquece** dados de lojas com coordenadas geográficas via API do Nominatim (OpenStreetMap).

---

## 🏗️ Arquitetura

```
 dados/produtos_cesta_basica.csv ──┐
 dados/municipios_pr_geohash.csv ──┤
                                   ▼
              ┌──────────────────────────────────┐
              │  API Menor Preço (Nota Paraná)   │
              └──────────┬───────────────────────┘
                         │  dados de preços (JSON)
             ┌───────────┼───────────────┐
             ▼           ▼               ▼
      bronze_azure   bronze_minio   bronze_local
      (Azure Blob)   (MinIO/S3)    (Parquet local)
             │
             ▼
      gold_menor_preco_lojas ──► Nominatim API (geocodificação)
             │
             ▼
      Dados de lojas enriquecidos (Parquet)
```

| Camada | Status | Descrição |
|--------|--------|-----------|
| **Bronze** | ✅ Implementada | Extração bruta da API → Parquet particionado |
| **Silver** | 🚧 Pendente | Limpeza e padronização dos dados |
| **Gold** | ✅ Implementada | Enriquecimento de lojas com geocodificação |

---

## 🌐 APIs Utilizadas

| API | Objetivo |
|-----|----------|
| [Menor Preço (Nota Paraná)](https://menorpreco.notaparana.pr.gov.br) | Coleta de preços de produtos em notas fiscais |
| [Open Food Facts](https://world.openfoodfacts.org) | Descoberta de GTINs (códigos de barras) a partir de nomes de produtos |
| [Nominatim (OpenStreetMap)](https://nominatim.openstreetmap.org) | Geocodificação de endereços de lojas (lat/lon) |

---

## 🚀 Como Usar

### Pré-requisitos

- **Docker** e **Docker Compose** instalados
- (Opcional) Conta no **Azure Blob Storage** ou instância **MinIO** local

### 1. Clone o repositório

```bash
git clone <url-do-repo>
cd mp_cesta_basica
```

### 2. Configure variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com as credenciais necessárias:

```env
AZURE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
```

### 3. Suba o worker Docker

```bash
cd worker
docker compose up -d --build
```

O container monta o projeto em `/app` e mantém o ambiente Python pronto para executar os scripts.

### 4. Execute os scripts dentro do container

```bash
# Extração Bronze → Azure
docker exec -it worker-worker-1 python tasks_python/bronze/bronze_menor_preco_azure.py

# Extração Bronze → MinIO
docker exec -it worker-worker-1 python tasks_python/bronze/bronze_menor_preco_minio.py

# Extração Bronze → Local
docker exec -it worker-worker-1 python tasks_python/bronze/bronze_menor_preco.py

# Gold: Enriquecimento de lojas
docker exec -it worker-worker-1 python tasks_python/gold/gold_menor_preco_lojas.py
```

### 5. (Opcional) Setup de desenvolvimento local

```bash
python _ops/setup_dev.py
```

Configura hooks do Git, ajustes de `core.autocrlf` e cria um alias `worker` no PowerShell para acessar o container facilmente.

---

## 📂 Estrutura do Projeto

```
mp_cesta_basica/
│
├── tasks_python/               # Pipeline ETL (Medallion Architecture)
│   ├── bronze/                 # Camada Bronze — extração bruta
│   │   ├── bronze_menor_preco.py         # Extração local (Pandas + Parquet)
│   │   ├── bronze_menor_preco_azure.py   # Extração → Azure Blob Storage (Polars)
│   │   ├── bronze_menor_preco_minio.py   # Extração → MinIO/S3 (Polars + boto3)
│   │   └── check_azure_blob.py           # Utilitário para listar blobs no Azure
│   ├── silver/                 # Camada Silver — (em desenvolvimento)
│   └── gold/                   # Camada Gold — dados enriquecidos
│       └── gold_menor_preco_lojas.py     # Geocodificação de lojas via Nominatim
│
├── dados/                      # Dados de referência e scripts auxiliares
│   ├── produtos_cesta_basica.csv         # ~120 produtos da cesta básica por categoria
│   ├── municipios_pr_geohash.csv         # 399 municípios do PR com geohash
│   ├── municipios_pr.csv                 # Municípios do PR (filtrado do IBGE)
│   ├── municipios.csv                    # Todos os municípios do Brasil
│   ├── geohashes_pr.csv                  # Geohashes do PR
│   ├── gerar_csv_produtos.py             # Gera o CSV de produtos da cesta básica
│   ├── geohashs.py                       # Gera geohashes a partir de lat/lon dos municípios
│   ├── filtro_municipios.py              # Filtra municípios do Paraná (UF 41)
│   └── api_openfood.py                   # Busca GTINs na API Open Food Facts
│
├── _ops/                       # DevOps e ferramentas de desenvolvimento
│   ├── setup_dev.py                      # Configura ambiente de dev (hooks, aliases)
│   ├── rebuild_worker.py                 # Deploy Blue-Green do container Docker
│   ├── check_imports.py                  # Verificação de sintaxe (pre-commit hook)
│   └── hooks/
│       └── pre-commit                    # Hook Git de pré-commit
│
├── worker/                     # Infraestrutura Docker
│   ├── Dockerfile                        # Python 3.12-slim + deps MariaDB
│   ├── docker-compose.yml                # Serviço worker com volume montado
│   └── requirements.txt                  # polars, requests, azure-storage-blob, etc.
│
└── README.md
```

---

## 📦 Scripts de Dados (`dados/`)

| Script | O que faz |
|--------|-----------|
| `gerar_csv_produtos.py` | Contém a lista hardcoded de ~120 produtos organizados em 10 categorias (Grãos, Óleos, Farinhas, Café, Massas, Proteínas, Enlatados, Hortifruti, Limpeza, Higiene) e gera o `produtos_cesta_basica.csv` |
| `filtro_municipios.py` | Filtra `municipios.csv` (todos os municípios do Brasil) pelo código UF 41 (Paraná) |
| `geohashs.py` | Codifica lat/lon de cada município em geohash (precisão 6) usando `pygeohash` |
| `api_openfood.py` | Para cada produto, gera variações de busca e consulta a API Open Food Facts para descobrir GTINs (códigos de barras) |

### Categorias de Produtos

Os ~120 produtos monitorados estão organizados em:

> Grãos e Básicos · Óleos e Gorduras · Farinhas e Amidos · Café e Chá · Massas · Proteínas · Enlatados e Conservas · Hortifruti · Limpeza · Higiene Pessoal

---

## 🛠️ DevOps e Tooling (`_ops/`)

### Deploy Blue-Green (`rebuild_worker.py`)

Implementa uma estratégia de deploy **blue-green** para o container Docker:
- Monitora alterações em `requirements.txt` e `Dockerfile` via hash MD5
- Só reconstrói a imagem se houver mudanças
- Sobe o novo container, aguarda estabilização (15s), drena graciosamente o antigo (SIGTERM + timeout 300s)

### Verificação de Sintaxe (`check_imports.py`)

- Compila todos os arquivos `.py` do projeto para detectar erros de sintaxe
- Integrado como **hook de pré-commit** do Git

### Setup de Dev (`setup_dev.py`)

1. Instala o hook de pré-commit
2. Configura `core.safecrlf=false` e `core.autocrlf=input` no Git
3. Adiciona função `worker` ao `$PROFILE` do PowerShell para acessar o container rapidamente

---

## 🔧 Tecnologias

| Tecnologia | Uso |
|------------|-----|
| **Python 3.12** | Linguagem principal |
| **Polars** | Manipulação de DataFrames (produção) |
| **Pandas** | Manipulação de DataFrames (script local) |
| **Parquet (zstd)** | Formato de armazenamento |
| **Azure Blob Storage** | Backend de armazenamento cloud |
| **MinIO (S3)** | Backend de armazenamento local S3-compatível |
| **Docker** | Containerização do worker |
| **pygeohash** | Codificação geográfica dos municípios |
