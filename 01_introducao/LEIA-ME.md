# 🎓 Introdução ao Pipeline de Dados

## O que é um Pipeline de Dados?

Um **pipeline de dados** é um conjunto de processos automatizados que:

1. **Coleta** dados de diferentes fontes
2. **Transforma** os dados em informações úteis
3. **Carrega** os dados em um destino final

Este processo é chamado de **ETL** (Extract, Transform, Load).

---

## 🏗️ Arquitetura do Nosso Pipeline

```
┌─────────────────┐
│  ARQUIVOS CSV   │
│  (Entrada)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   AMAZON S3     │
│  (Armazenamento)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PYTHON/SPARK   │
│ (Processamento) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PARQUET       │
│  (Resultado)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   GLUE CATALOG  │
│   (Catalogação) │
└─────────────────┘
```

---

## 🔑 Conceitos Fundamentais

### 1. Amazon S3 (Simple Storage Service)
- Armazenamento em nuvem **ilimitado**
- Estrutura: **Buckets** (pastas raiz) e **Objetos** (arquivos)
- Altamente **disponível** e **durável**
- Ideal para data lakes

**Exemplo de caminho S3:**
```
s3://meu-bucket/dados/vendas/2024/01/vendas.csv
```

### 2. Formato CSV (Comma-Separated Values)
- Formato **simples** e **portável**
- Fácil de ler e escrever
- Amplamente suportado

**Exemplo:**
```csv
id,nome,valor,data
1,Produto A,100.00,2024-01-01
2,Produto B,250.50,2024-01-02
3,Produto C,75.25,2024-01-03
```

### 3. Formato Parquet
- Formato **colunar** otimizado
- **Compressão** automática
- **Melhor performance** para análises
- Reduz **custo de armazenamento**

**Vantagens:**
- 80% menor tamanho que CSV
- Leitura 10x mais rápida
- Suporta tipos de dados complexos

### 4. Apache Spark
- Engine de processamento **distribuído**
- Processa **grandes volumes** de dados
- Funciona em **Python** (PySpark)
- Ideal para transformações complexas

### 5. AWS Glue Data Catalog
- **Catalogação** centralizada de dados
- Cria um **índice** de todos seus dados
- Facilita **descoberta** de dados
- Integra com Athena, Redshift, etc.

---

## 📊 Fluxo Prático - Passo a Passo

### Cenário: Análise de Vendas

**Objetivo:** Processar 3 meses de vendas e calcular totais por produto

```
Step 1: Criar bucket S3
├─ Bucket: vendas-empresa-2024
└─ Estrutura:
   ├─ raw/          (dados brutos)
   ├─ processed/    (dados processados)
   └─ catalog/      (metadados)

Step 2: Upload de dados brutos
├─ s3://vendas-empresa-2024/raw/vendas_jan.csv
├─ s3://vendas-empresa-2024/raw/vendas_fev.csv
└─ s3://vendas-empresa-2024/raw/vendas_mar.csv

Step 3: Processar com Spark
├─ Ler todos os CSVs
├─ Combinar em um DataFrame
├─ Calcular totalizações
└─ Limpar dados inválidos

Step 4: Salvar como Parquet
└─ s3://vendas-empresa-2024/processed/vendas_total.parquet

Step 5: Catalogar no Glue
├─ Tabela: vendas_total
├─ Schema: (id, produto, quantidade, valor_total, data)
└─ Localização: s3://vendas-empresa-2024/processed/
```

---

## 💡 Por que usar esse stack?

| Tecnologia | Por que? |
|-----------|---------|
| **AWS S3** | Escalável, barato, durável, integrado com AWS |
| **Spark** | Processa TeraBytes em minutos |
| **Parquet** | Formato otimizado, comprimido, rápido |
| **Glue** | Integração nativa com AWS, sem servidores |
| **Python** | Linguagem mais acessível para ciência de dados |

---

## 📋 Checklist Antes de Começar

- [ ] Conta AWS criada
- [ ] AWS CLI instalado
- [ ] Python 3.8+ instalado
- [ ] boto3 instalado (`pip install boto3`)
- [ ] PySpark instalado (`pip install pyspark`)
- [ ] Pandas instalado (`pip install pandas`)
- [ ] Acesso a S3 testado

---

## 🎯 Resultado Final

Ao final desta aula, você terá:

✅ Um bucket S3 funcional com dados organizados
✅ Um script Python que processa dados automaticamente
✅ Dados salvos em formato Parquet otimizado
✅ Um catálogo de dados no Glue
✅ Conhecimento para criar pipelines profissionais

---

## 🚀 Próximo Passo

Vá para `02_configuracao_aws/guia_s3.md` para revisar a estrutura do bucket e a organização dos dados do projeto.

Se ainda não houver credenciais configuradas, consulte também `02_configuracao_aws/credenciais_aws.md`.

---

**Dica:** Não se preocupe se não entender tudo agora. Cada conceito ficará mais claro na prática! 🎓
