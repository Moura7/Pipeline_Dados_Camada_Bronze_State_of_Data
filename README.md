# Pipeline de Dados: Camada Bronze na AWS

[![Status](https://img.shields.io/badge/status-validado%20em%20AWS%20Glue-success)](https://aws.amazon.com/glue/)
[![Versão](https://img.shields.io/badge/vers%C3%A3o-2.0-blue)](https://github.com/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-yellow)](https://www.python.org/)

Pipeline didático de ingestão de arquivos CSV para a camada Bronze de um data lake. Os dados são armazenados no Amazon S3, processados com Apache Spark no AWS Glue, convertidos para Parquet e registrados no Glue Data Catalog.

> O projeto foi validado no ambiente AWS Glue. A execução dos scripts Glue depende do runtime gerenciado da AWS e não é equivalente a executar um script Python comum localmente.

## Índice

- [Características principais](#características-principais)
- [Tecnologias usadas](#tecnologias-usadas)
- [Requisitos](#requisitos)
- [Instalação e configuração](#instalação-e-configuração)
- [Como usar](#como-usar)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Segurança e dados](#segurança-e-dados)
- [Contribuindo](#contribuindo)
- [Licença](#licença)

## Características principais

- Ingestão de dados CSV no Amazon S3.
- Processamento distribuído com PySpark no AWS Glue.
- Conversão para Parquet com compressão Snappy.
- Particionamento por `anomesdia` no formato `YYYYMMDD`.
- Registro e atualização de tabelas no AWS Glue Data Catalog.
- Dois fluxos de dados independentes:
  - clientes e vendas;
  - pesquisa State of Data Brazil 2024.

### Tabelas geradas

| Fluxo | Tabela | Entrada | Saída |
| --- | --- | --- | --- |
| Clientes e vendas | `workspace.tb_clientes_bronze` | `data-input/bronze/base-clientes/clientes.csv` | `data-output/bronze/base-clientes/` |
| Clientes e vendas | `workspace.tb_vendas_bronze` | `data-input/bronze/base-vendas/vendas.csv` | `data-output/bronze/base-vendas/` |
| State of Data 2024 | `workspace.tb_state_of_data_bronze` | `data-input/bronze/state-of-data-2024/` | `data-output/bronze/state-of-data-2024/` |

## Tecnologias usadas

- Python e boto3
- Amazon S3
- AWS Glue e Glue Data Catalog
- Apache Spark / PySpark
- Apache Parquet
- Jupyter Notebook
- AWS IAM e CloudWatch

## Requisitos

- Conta AWS Academy Lab ou conta AWS com permissões para S3, Glue, IAM e STS.
- AWS CLI instalado e configurado, ou AWS Toolkit para VS Code.
- Python 3.10 ou superior.
- Região AWS definida para o ambiente, normalmente `us-east-1`.
- Uma role IAM que possa ser assumida pelo AWS Glue. No AWS Academy, normalmente é utilizada a `LabRole`.

As dependências Python estão em [requirements.txt](requirements.txt). O pacote `awsglue` não é instalado por esse arquivo porque é fornecido pelo ambiente gerenciado do AWS Glue.

## Instalação e configuração

### 1. Obter o projeto

```powershell
git clone https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git
Set-Location "Pipeline de Dados - Camada Bronze"
```

### 2. Criar um ambiente virtual

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Se o PowerShell bloquear a ativação do ambiente, ajuste a política apenas para o seu usuário:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### 3. Configurar a AWS

Configure um perfil local usando o AWS CLI:

```powershell
aws configure
aws sts get-caller-identity
```

Consulte [02_configuracao_aws/credenciais_aws.md](02_configuracao_aws/credenciais_aws.md) para o fluxo do AWS Academy. Nunca versionar chaves, tokens, arquivos `.env` ou credenciais no GitHub.

### 4. Preparar o S3

Crie um bucket com nome globalmente único e replique os prefixos esperados pelos scripts:

```text
s3://SEU_BUCKET/
├── data-input/bronze/base-clientes/clientes.csv
├── data-input/bronze/base-vendas/vendas.csv
├── data-input/bronze/state-of-data-2024/<arquivo>.csv
└── data-output/bronze/
```

O guia conceitual de organização está em [01_introducao/LEIA-ME.md](01_introducao/LEIA-ME.md). Os notebooks também demonstram a criação do bucket, o upload dos arquivos e a configuração do Glue Job.

## Como usar

### Opção recomendada: notebooks

Abra um dos notebooks no VS Code ou Jupyter:

- [05_notebooks/Aula_Camada_Bronze.ipynb](05_notebooks/Aula_Camada_Bronze.ipynb): fluxo com clientes e vendas.
- [05_notebooks/Aula_Camada_Bronze_Kaggle.ipynb](05_notebooks/Aula_Camada_Bronze_Kaggle.ipynb): fluxo com a base State of Data 2024.

Execute as células na ordem para:

1. validar as credenciais AWS;
2. criar ou selecionar o bucket S3;
3. enviar os arquivos de entrada;
4. enviar o script ao bucket de assets;
5. criar ou atualizar o Glue Job;
6. executar e acompanhar o job;
7. consultar os arquivos Parquet e as tabelas do catálogo.

### Scripts do AWS Glue

Os scripts recebem os argumentos obrigatórios `JOB_NAME` e `BUCKET_NAME` no Glue:

- [04_scripts_python/glue-base-clientes-bronze.py](04_scripts_python/glue-base-clientes-bronze.py): processa clientes e vendas.
- [04_scripts_python/glue-base-state-of-data-bronze.py](04_scripts_python/glue-base-state-of-data-bronze.py): processa a pesquisa State of Data 2024 e normaliza os nomes das colunas.

No console do AWS Glue, selecione um runtime compatível com Spark, informe o caminho do script no S3 e configure, por exemplo:

```text
--JOB_NAME = nome-do-job
--BUCKET_NAME = SEU_BUCKET
```

Após a execução, valide a partição `anomesdia=YYYYMMDD` no S3 e as tabelas no database `workspace` do Glue Data Catalog.

## Estrutura do projeto

```text
.
├── 01_introducao/
│   └── LEIA-ME.md
├── 02_configuracao_aws/
│   └── credenciais_aws.md
├── 03_arquivos_exemplo/
│   ├── clientes.csv
│   └── vendas.csv
├── 04_scripts_python/
│   ├── glue-base-clientes-bronze.py
│   └── glue-base-state-of-data-bronze.py
├── 05_notebooks/
│   ├── Aula_Camada_Bronze.ipynb
│   └── Aula_Camada_Bronze_Kaggle.ipynb
├── 06_dados_externos_mcp/
│   └── Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv
├── .gitignore
├── README.md
└── requirements.txt
```

## Segurança e dados

- Os arquivos de exemplo de clientes contêm dados pessoais fictícios ou de demonstração; confirme a origem antes de publicar o repositório.
- A base State of Data 2024 pode conter informações potencialmente sensíveis. Verifique os termos de uso e a licença original do dataset antes de redistribuí-la.
- Não publique credenciais AWS, tokens, chaves privadas ou arquivos de configuração com segredos.
- Em um ambiente real, aplique IAM com menor privilégio, criptografia, controle de acesso e política de retenção no S3.
- O bucket padrão usado em alguns exemplos pertence a um ambiente de laboratório. Substitua-o pelo seu próprio `BUCKET_NAME`.

## Contribuindo

1. Faça um fork do projeto.
2. Crie uma branch para sua alteração: `git checkout -b minha-alteracao`.
3. Atualize código e documentação mantendo os caminhos e nomes de tabelas coerentes.
4. Valide os notebooks ou o Glue Job no ambiente AWS apropriado.
5. Abra um Pull Request descrevendo a alteração e os testes realizados.

Não inclua credenciais, dados pessoais ou arquivos gerados pelo ambiente no commit.

## Licença

Este projeto ainda não possui um arquivo `LICENSE` nem uma licença formal declarada. Até que uma licença seja escolhida, todos os direitos permanecem reservados ao autor. A licença e os termos de uso do dataset externo devem ser tratados separadamente.

**Versão:** 2.1  
**Última atualização:** 2026-10-13