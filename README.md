# ⚡ Orquestração de Pipeline de Vendas E-Commerce com Apache Airflow

[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=Apache-Airflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![ETL](https://img.shields.io/badge/Pipeline-ETL-green?style=for-the-badge)](#)

Este repositório contém a implementação prática de uma **DAG (Directed Acyclic Graph)** no **Apache Airflow**, projetada para automatizar e orquestrar um pipeline ETL de **Vendas e E-Commerce**.

---

## 📐 Fluxo da DAG (`dag_orquestracao_vendas`)

```mermaid
flowchart LR
    A[inicio_pipeline] --> B[extracao_pedidos_ecommerce]
    B --> C[validacao_e_limpeza_clientes]
    C --> D[calculo_faturamento_diario]
    D --> E[envio_relatorio_diretoria]
```

---

## 🛠️ Detalhamento das Tarefas (Tasks)

1. **`extracao_pedidos_ecommerce`**:
   - Ingestão simulada dos pedidos diários brutos de vendas do e-commerce.
2. **`validacao_e_limpeza_clientes`**:
   - Validação de e-mails, padronização de nomes de clientes (Title Case) e sanitização de valores monetários (`"R$ 1.200,00"` ➔ `1200.00`).
3. **`calculo_faturamento_diario`**:
   - Consolidação do faturamento total do dia, cálculo do ticket médio por pedido e identificação de produtos de maior destaque.
4. **`envio_relatorio_diretoria`**:
   - Geração de resumo executivo e disparo de notificação/alerta de sucesso (`SUCCESS`).

---

## 📁 Estrutura do Repositório

```text
Pipeline_Orquestrado_Airflow/
├── README.md                           # Documentação do projeto
└── dags/
    └── dag_orquestracao_vendas.py     # Código da DAG em Python
```

---

## 🚀 Como Executar

1. Clone o repositório.
2. Ative o ambiente virtual e execute o Airflow em modo standalone:
   ```bash
   airflow standalone
   ```
3. Acesse a interface web do Airflow em `http://localhost:8080`.
4. Ative a DAG **`dag_orquestracao_vendas`** e clique em **Trigger DAG** (Play) para ver a execução!
