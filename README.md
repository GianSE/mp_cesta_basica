# 🛍️ MP Feeder (v1.30) 🛒

Algoritmo em Python para a captação de notas fiscais da plataforma Menor Preço (Nota Paraná) e inserção em um banco de dados MariaDB.

O script foi desenvolvido para coletar dados de preços de concorrentes com base em uma lista de produtos (GTINs) e geolocalizações (Geohashs) pré-definidas.

## 🧭 Sumário

* [Principais Funcionalidades](#-principais-funcionalidades)
* [Como Usar](#-como-usar)
* [Scripts Utilitários](#%EF%B8%8F-scripts-utilitarios)
* [Fluxo de Execução](#-fluxo-de-execução)
* [Estrutura do Projeto](#-estrutura-do-projeto)

## 🎯 Resumo do Projeto

Este é um pipeline de ETL robusto e tolerante a falhas projetado para:

*   Coletar dados de preços da API do Menor Preço (Nota Paraná).

*   Enriquecer os dados com geocodificação de lojas (Nominatim API) e notificações (Telegram).

*   Carregar os dados em um banco MariaDB, com lógica de recuperação automática em caso de falha.

---

## ✨ Principais Funcionalidades

Este projeto é um pipeline de ETL (Extração, Transformação e Carga) completo e resiliente.

<details> 
    <summary>🧠 <strong>Atualização Inteligente de Produtos</strong></summary> Periodicamente (a cada 30+ dias), o script reconstrói a lista de 1000 produtos-alvo (<code>bronze_menorPreco_produtos</code>). Ele cruza os 2000 produtos mais vendidos por <i>valor</i> e <i>quantidade</i> da <code>bronze_plugpharma_vendas</code> e, em seguida, busca o <strong>GTIN principal</strong> (<code>codigo_principal = 1</code>) para cada um na <code>bronze_plugpharma_produtos</code>. 
</details>

<details> 
    <summary>🔄 <strong>Coleta Rotativa (Batch)</strong></summary> 
    O script não consulta os 1000 produtos de uma vez. Ele divide a lista em lotes de 100 GTINs e processa um lote por execução, continuando de onde parou na execução anterior (lógica gerenciada pelo <code>ultimo_indice.txt</code> e <code>pegar_ultimo_gtin</code>). 
</details>

<details> 
    <summary>🎣 <strong>Coleta Ampla de Dados</strong></summary> 
    Utiliza os GTINs do lote como "isca" na API do Menor Preço. No entanto, ele salva <i>todos</i> os produtos que a API retorna na nota fiscal, não apenas o produto-isca. Isso enriquece a tabela <code>bronze_menorPreco_notas</code> com uma vasta gama de produtos concorrentes. 
</details>

<details> 
    <summary>🗺️ <strong>Geocodificação de Novas Lojas</strong></summary> 
    Ao encontrar uma loja (<code>id_loja</code>) não cadastrada na <code>bronze_menorPreco_lojas</code>, o script utiliza a API do Nominatim Geocoding(OpenStreetMap) para buscar suas coordenadas de latitude e longitude antes de salvá-la. 
</details>

<details> 
    <summary>🛡️ <strong>Tolerância a Falhas (Banco de Dados)</strong></summary> 
    Se a inserção final no banco de dados falhar (ex: perda de conexão), o <code>handle_execution_error</code> é acionado. Ele salva <i>todos</i> os dados coletados (notas e lojas) em arquivos <code>.csv</code> locais (<code>notas_parciais.csv</code>, <code>lojas_parciais.csv</code>). 
</details>

<details> 
    <summary>🔁 <strong>Recuperação Automática</strong></summary> 
    Na próxima execução, o <code>main.py</code> detecta esses arquivos <code>.csv</code>. Ele primeiro executa o <code>run_recovery_flow</code>, que carrega os dados desses CSVs no banco de dados e depois os apaga, garantindo que nenhum dado seja perdido antes de iniciar uma nova coleta. 
</details>

<details> 
    <summary>🔔 <strong>Monitoramento e Notificações</strong></summary> 
    Envia mensagens de sucesso ou erro para um chat do Telegram, permitindo o monitoramento remoto da execução. 
</details>

---

## 🚀 Como Usar

<details> 
    <summary><strong>1. 📋 Pré-requisitos</strong></summary>

Garanta que você tenha um banco de dados MariaDB acessível. O script espera se conectar a um banco chamado <code>dbDrogamais</code>.

Você precisará das seguintes tabelas (fontes e destino):

<ul> 
    <li><code>bronze_plugpharma_vendas</code> (para análise de vendas)</li> 
    <li><code>bronze_plugpharma_produtos</code> (para buscar GTINs principais)</li> 
    <li><code>bronze_cidades</code> (para buscar geohashs)</li> 
    <li><code>dbSults.tb_report_auditoria_embedded</code> (para filtrar geohashs)</li> 
    <li><code>bronze_menorPreco_produtos</code> (destino da lista de 1000 produtos)</li> 
    <li><code>bronze_menorPreco_notas</code> (destino dos dados brutos da API)</li> 
    <li><code>bronze_menorPreco_lojas</code> (destino das lojas concorrentes)</li> 
</ul> 

</details>

<details>
    <summary><strong>2. 💻 Instalação</strong></summary>

Clone o repositório e instale as dependências do Python:

```bash
pip install -r requirements.txt
```

</details>

<details> 
    <summary><strong>3. 🔑 Configuração</strong></summary>

O script usa um arquivo <code>config.py</code> para armazenar suas chaves e senhas. Este arquivo é ignorado pelo Git.

Copie o arquivo de exemplo (use <code>copy</code> no Windows ou <code>cp</code> no Linux/Mac):
```bash
copy config.py.example config.py
```
Abra o <code>config.py</code> e preencha as variáveis com suas credenciais:

<ul>
    <li><strong><code>DB_CONFIG</code></strong>: Dicionário com <code>user</code>, <code>password</code>, <code>host</code> e <code>port</code> do seu MariaDB.</li> 
    <li><strong><code>TELEGRAM_TOKEN</code></strong>: O token do seu Bot do Telegram.</li> 
    <li><strong><code>TELEGRAM_CHAT_ID</code></strong>: O ID do chat para onde as notificações serão enviadas.</li> 
</ul>

</details>

<details> 
    <summary><strong>4. 🏗️ Inicialização do Banco (Primeira Execução)</strong></summary>
Antes de executar o pipeline principal pela primeira vez, você precisa garantir que as tabelas de destino existam. O script init_db.py faz isso para você.
    
```bash
python utils\init_db.py
```

Este script irá criar as tabelas bronze_menorPreco_produtos, bronze_menorPreco_lojas e bronze_menorPreco_notas com o esquema e collate corretos, caso elas ainda não existam.

</details>

<details> 
    <summary><strong>4. ▶️ Execução</strong></summary>

Uma vez configurado, basta executar o <code>main.py</code>:

```bash
python main.py
```

O script cuidará do resto, seja iniciando uma nova coleta ou recuperando dados de uma execução anterior com falha.

</details>

---

## 🛠️ Scripts Utilitarios

A pasta utils/ contém scripts para administrar, fazer backup e etc no banco de dados.

<details> <summary><code>utils/init_db.py</code></summary> <strong>O que faz:</strong> Script para (re)criar todo o banco de dados. Ele lê e executa automaticamente todos os arquivos <code>.sql</code> da pasta <code>utils/migrations/</code> em ordem alfabética. </details>

<details> <summary><code>utils/export_schema.py</code></summary> <strong>O que faz:</strong> Script para versionamento de banco. Ele se conecta ao banco, lê a estrutura "ao vivo" de todas as tabelas e procedures listadas nele, e sobrescreve os arquivos <code>.sql</code> na pasta <code>utils/migrations/</code>. <strong>Fluxo de trabalho:</strong> 1. Altere a tabela no banco (ex: DBeaver) -> 2. Rode <code>python utils\export_schema.py</code> -> 3. Faça o commit da mudança no arquivo <code>.sql</code>. </details>

<details> <summary><code>utils/executor_silver.py</code></summary> <strong>O que faz:</strong> Executa manualmente a procedure <code>proc_atualiza_silver_menorPreco_notas</code>. Útil para forçar a atualização dos dados da camada Silver (transformação Bronze -> Silver) sem ter que rodar o pipeline de coleta (<code>main.py</code>) inteiro. </details>

---

## 📊 Fluxo de Execução

<details>
    <summary><strong>1. main.py</strong></summary>
    <ul>
        <li>Inicio da orquestração</li>
    </ul>
</details>

<details>
    <summary><strong>2. Verifica Falha Anterior</strong></summary>
    <ul>
        <li>O script procura pelo arquivo notas_parciais.csv.</li>
    </ul>
</details>

<details> 
    <summary><strong>3. Fluxo de Recuperação (Se .csv existe)</strong></summary> 
    <ul> 
        <li><code>flow.run_recovery_flow</code> é chamado.</li> 
        <li>Os dados dos arquivos .csv são lidos e inseridos no banco de dados.</li> 
        <li>Os arquivos .csv são removidos após o sucesso da carga.</li> 
    </ul> 
</details>

<details> 
    <summary><strong>4. Fluxo Normal (Se .csv não existe)</strong></summary> 
    <ul> 
        <li><code>flow.run_normal_flow</code> é chamado.</li> 
        <li><strong>[E] Extração:</strong> 
            <ul> 
                <li>(Opcional) Atualiza a lista de 1000 produtos-alvo se tiver > 30 dias.</li> 
                <li>Seleciona o lote de 100 GTINs do dia.</li> 
                <li>Gera a lista de consultas (Geohash x GTIN).</li> 
            </ul> 
        </li> 
        <li><strong>[T] Transformação (Coleta):</strong> 
            <ul> 
                <li><code>api_services.buscar_notas</code> coleta os dados da API do Menor Preço.</li> 
                <li>Retorna os DataFrames <code>Notas_geral</code> e <code>Lojas_SC_geral</code> para o <code>main.py</code>.</li> 
            </ul> 
        </li> 
        <li><strong>[L] Carga:</strong> 
            <ul> 
                <li><code>main.py</code> recebe os DataFrames.</li> 
                <li>(Opcional) <code>api_services.buscar_lat_lon_lojas_sc</code> enriquece <code>Lojas_SC_geral</code> com Lat/Lon do Nominatim</li> 
                <li><code>db_manager.inserir_lojas_sc</code> e <code>db_manager.inserir_notas</code> carregam os dados no MariaDB.</li> 
            </ul> 
        </li> 
    </ul> 
</details>

<details> 
    <summary><strong>5. Finalização</strong></summary> 
    <ul> 
        <li><strong>Sucesso:</strong> <code>handle_success</code> limpa o <code>ultimo_indice.txt</code> e envia notificação de sucesso via Telegram.</li> 
        <li><strong>Falha (Ex: DB Offline):</strong> <code>handle_execution_error</code> é chamado, <code>save_partial_data</code> cria os arquivos .csv para a próxima execução e envia notificação de erro.</li> 
    </ul> 
</details>

---

## 📂 Estrutura do Projeto

<details> <summary>🚦 <strong>main.py</strong></summary> Ponto de entrada. Orquestra os fluxos (normal vs. recuperação) e a etapa de Carga (Load). </details>

<details> <summary>▶️ <strong>mp_feeder.bat</strong></summary> Atalho para executar o <code>main.py</code> no Windows, ativando o <code>venv</code> automaticamente. </details>

<details> <summary>🗃️ <strong>MP_Feeder/</strong></summary> Pasta com toda a lógica de negócio principal do ETL (Extração, Transformação e Carga).  
    <ul> 
        <li><code>flow.py</code>: Contém a lógica principal (<code>run_normal_flow</code>, <code>run_recovery_flow</code>).</li> 
        <li><code>db_manager.py</code>: Abstrai toda a comunicação com o MariaDB (SELECTs, INSERTs).</li> 
        <li><code>api_services.py</code>: Gerencia chamadas para APIs externas (Nota Paraná, Nominatim, Telegram).
        </li> <li><code>etl_utils.py</code>: Funções auxiliares (Pandas, gerenciamento de índice).</li> 
        <li><code>error_handler.py</code>: Funções centralizadas para lidar com exceções e salvar CSVs.</li> 
    </ul>
</details>

<details> <summary>🏗️ <strong>utils/</strong></summary> Pasta com scripts de utilidade e manutenção do banco.  
    <ul> 
        <li><code>init_db.py</code>: Script para (re)criar o banco a partir dos arquivos de migração.</li> 
        <li><code>export_schema.py</code>: Script para salvar o schema atual do banco nos arquivos de migração.</li> 
        <li><code>executor_silver.py</code>: Script para rodar manualmente a procedure da camada Silver.</li> 
        <li><code>migrations/</code>: Pasta contendo todos os arquivos <code>.sql</code> que definem a estrutura (schema) do banco.</li> 
    </ul>
</details>

<details> <summary>🔒 <strong>config.py (e .example)</strong></summary> Armazena as credenciais e chaves de API. </details>

<details> <summary>📦 <strong>requirements.txt</strong></summary> Lista de pacotes Python necessários. </details>

<details> <summary>🙈 <strong>.gitignore</strong></summary> Define os arquivos que não devem ser versionados (logs, config.py, arquivos .csv, etc.). </details>
