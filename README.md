# Bootcamp Data Analytics - Digital College

Este repositório consolida projetos práticos e códigos desenvolvidos com o objetivo de construir uma arquitetura de dados completa. O foco do projeto varia desde a modelagem e extração de dados em ambiente Cloud até a automação de pipelines de ETL e geração de inteligência de cliente (Customer Intelligence) através de algoritmos de segmentação.

# Stacks

<p align="left">
  <strong>Orquestração & Engenharia:</strong><br>
  <a href="https://airflow.apache.org/" target="_blank">
    <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white" alt="Apache Airflow">
  </a>
</p>

<p align="left">
  <strong>Análise & Manipulação de Dados:</strong><br>
  <a href="https://pandas.pydata.org/" target="_blank">
    <img src="https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas">
  </a>
  <a href="https://numpy.org/" target="_blank">
    <img src="https://img.shields.io/badge/numpy-013243?style=for-the-badge&logo=numpy&logoColor=white" alt="NumPy">
  </a>
</p>

<p align="left">
  <strong>Visualização de Dados:</strong><br>
  <a href="https://matplotlib.org/" target="_blank">
    <img src="https://img.shields.io/badge/Matplotlib-%23ffffff.svg?style=for-the-badge&logo=Matplotlib&logoColor=black" alt="Matplotlib">
  </a>
  <a href="https://seaborn.pydata.org/" target="_blank">
    <img src="https://img.shields.io/badge/Seaborn-9cf?style=for-the-badge&logo=python&logoColor=white" alt="Seaborn">
  </a>
</p>

<p align="left">
  <strong>Banco de Dados & Ferramentas:</strong><br>
  <a href="https://www.postgresql.org/" target="_blank">
    <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
  </a>
  <a href="https://jupyter.org/" target="_blank">
    <img src="https://img.shields.io/badge/Jupyter-F37626.svg?style=for-the-badge&logo=Jupyter&logoColor=white" alt="Jupyter">
  </a>
  <a href="https://git-scm.com/" target="_blank">
    <img src="https://img.shields.io/badge/GIT-E44C30?style=for-the-badge&logo=git&logoColor=white" alt="Git">
  </a>
</p>


## Visão Geral do Projeto

O projeto simula um cenário real de **Business Analytics**, onde dados transacionais brutos são transformados em estratégias de retenção de clientes. A solução implementada orquestra o fluxo de dados para calcular automaticamente o score **RFM (Recência, Frequência e Monetário)**, classificando clientes em clusters como *"Champions"*, *"Loyal Customers"* ou *"At Risk"*.

### Pilares Técnicos

1. **Engenharia de Dados:** Conexão com banco de dados em nuvem, extração via drivers SQL e orquestração de tarefas.
2. **Data Science & Analytics:** Manipulação vetorial de dados com Pandas e discretização estatística (qcut/cut) para scoring.
3. **Automação:** Implementação de DAGs no Apache Airflow para execução recorrente e envio de relatórios.

---

## Construção

As seguintes ferramentas e bibliotecas foram utilizadas para construir a solução:

| Área | Tecnologia | Utilização no Projeto |
| --- | --- | --- |
| **Linguagem** | `Python` | Scripting, automação e análise exploratória. |
| **Orquestração** |  | Gerenciamento do pipeline de dados (Extração -> RFM -> Email). |
| **Banco de Dados** | `Postgres` | Armazenamento de dados transacionais (Vendas, Clientes, NF). |
| **Processamento** |  | Manipulação de DataFrames, limpeza e cálculo de quintis. |
| **Visualização** |  | Geração de gráficos para análise de distribuição de segmentos. |
| **Conectividade** | `Psycopg2` & `SQLAlchemy` | Drivers de alta performance para comunicação Python-SQL. |

---

## Arquitetura da Solução

### 1. Modelagem e Extração (SQL & Python)

O projeto consome dados de um Data Warehouse hospedado em nuvem (`postgresql-datadt.alwaysdata.net`).

* **Querying:** Consultas SQL complexas com `JOINs` para unificar tabelas de Notas Fiscais, Pessoas Físicas e Jurídicas.
* **Conexão:** Uso da biblioteca `psycopg2` para conexões seguras e eficientes.

### 2. Algoritmo de Segmentação RFM

Implementado tanto em **Jupyter Notebooks** (para prototipagem e visualização) quanto em scripts de produção. A lógica segue:

* **Recência (R):** Dias desde a última compra (Cálculo: `Data Referência - Data Última Compra`).
* **Frequência (F):** Contagem total de pedidos únicos (`count`).
* **Monetário (M):** Somatório do valor gasto (`sum`).

Os clientes são pontuados de 1 a 5 em cada dimensão utilizando **Discretização por Quantis (`pd.qcut`)**, garantindo uma distribuição estatística equilibrada dos scores.

### 3. Pipeline Automatizado (Apache Airflow)

Uma **DAG (Directed Acyclic Graph)** foi desenvolvida para rodar diariamente (`schedule='0 21 * * *'`) com as seguintes Tasks:

1. `extrair_dados`: Conecta no DB, executa a query e serializa os dados.
2. `calcular_rfm`: Aplica a lógica matemática e gera os scores R, F e M.
3. `segmentar_clientes`: Classifica os clientes com base em regras de negócio (ex: *Se R>=4 e F>=4 -> "Champions"*).
4. `enviar_relatorio`: Dispara um e-mail automático com os KPIs da segmentação processada.

---

## Análises

Nos notebooks de análise (`analise.ipynb` e `analise_rfm.ipynb`), foram realizadas explorações profundas:

* **Clusterização:** Identificação de grupos de alto valor vs. grupos em churn.
* **Ações Estratégicas:** Mapeamento automático de ações sugeridas para cada cluster (ex: *Clientes VIP* -> "Acesso antecipado"; *Novos Clientes* -> "Onboarding").
* **Visualização:** Gráficos de barras para monitorar o volume de clientes por segmento.


---

*Projeto desenvolvido com foco em aplicação prática de conceitos de Engenharia e Análise de Dados.*
