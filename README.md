# Comprehensive Vehicle Specification

Este projeto é um pipeline de dados para processar especificações de veículos (carros e motos), utilizando PySpark e Iceberg.

## Estrutura do Projeto

- `datalib/`: Biblioteca principal contendo a lógica de ETL e sistema de dados.
- `raw/`: Scripts para a camada Raw, responsáveis pela ingestão e validação inicial dos dados.
- `data/`: Dados brutos (landing) e processados. (Não versionado)

## Pré-requisitos

Para gerenciar o ambiente e as dependências, este projeto utiliza Rye.

1.  Instale o Rye seguindo as instruções oficiais.
2.  Instale o `pre-commit` para garantir a qualidade do código:
    ```bash
    pip install pre-commit
    ```

## Configuração do Ambiente

1.  Clone o repositório.
2.  Dentro do diretório do projeto, instale as dependências com o Rye:
    ```bash
    rye sync
    ```
3.  Configure os hooks de pre-commit:
    ```bash
    pre-commit install
    ```

## Como Executar

Para executar um dos scripts de processamento, você pode usar o Rye:

```bash
rye run python raw/bikes.py
```
