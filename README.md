# ShopBrasil — Pipeline ETL com Apache Airflow

Pipeline de ingestão e análise que substitui um script cron: coleta produtos da [FakeStore API](https://fakestoreapi.com/docs), valida os dados, grava no PostgreSQL e calcula métricas por categoria.

Stack: **Apache Airflow 2.9.3**, **PostgreSQL 16** (metadados + banco analítico) e **Adminer**.

---

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/) (v2+)

---

## Como rodar

### 1. Subir os serviços

Na raiz do projeto:

```bash
docker compose up -d
```

Na primeira execução, o container `airflow-init` migra o banco de metadados, cria o usuário admin e registra a connection `postgres_shop`. Aguarde alguns minutos até scheduler e webserver ficarem saudáveis.

### 2. Verificar status

```bash
docker compose ps
```

Todos os serviços devem estar `running` (ou `healthy`).

### 3. Criar o pool de concorrência

A DAG usa o pool `ecommerce_pool` (limite de 2 slots) nas tasks mapeadas de métricas. Crie-o uma vez:

**Pela UI:** Admin → Pools → Add → nome `ecommerce_pool`, slots `2`.

**Pelo CLI:**

```bash
docker compose exec airflow-scheduler airflow pools set ecommerce_pool 2 "Pool do lab ShopBrasil"
```

### 4. Ativar e executar a DAG

1. Acesse a UI do Airflow (credenciais abaixo).
2. Localize a DAG **`shop_etl`**.
3. Ative o toggle (DAGs sobem pausadas por padrão).
4. Dispare manualmente com **Trigger DAG** ou aguarde o agendamento diário às **06:00 (Brasília)**.

### 5. Testar via CLI (opcional)

```bash
docker compose exec airflow-scheduler airflow dags test shop_etl
```

---

## URLs e credenciais

### Airflow (UI)

| Campo    | Valor                          |
|----------|--------------------------------|
| URL      | http://localhost:8080          |
| Usuário  | `admin`                        |
| Senha    | `admin`                        |

### Adminer (banco analítico `shopdb`)

| Campo    | Valor                          |
|----------|--------------------------------|
| URL      | http://localhost:8081          |
| Sistema  | PostgreSQL                     |
| Servidor | `postgres-shop`                |
| Usuário  | `shop`                         |
| Senha    | `shop123`                      |
| Base     | `shopdb`                       |

> Use o servidor `postgres-shop` (nome do container na rede Docker). Não use `localhost` dentro do Adminer.

### PostgreSQL externo (opcional)

Para conectar com cliente SQL na máquina host:

| Campo    | Valor                          |
|----------|--------------------------------|
| Host     | `localhost`                    |
| Porta    | `5433`                         |
| Usuário  | `shop`                         |
| Senha    | `shop123`                      |
| Base     | `shopdb`                       |

---

## Comandos úteis

```bash
# Subir em segundo plano
docker compose up -d

# Ver logs em tempo real
docker compose logs -f

# Logs só do scheduler
docker compose logs -f airflow-scheduler

# Parar os containers (mantém volumes)
docker compose down

# Parar e apagar volumes (reset completo dos bancos)
docker compose down -v

# Recriar scheduler/webserver após mudanças no compose
docker compose up -d airflow-scheduler airflow-webserver
```

---

## Estrutura do projeto

```
shop-brasil/
├── dags/
│   └── shop_etl.py          # DAG principal (TaskFlow API)
├── plugins/
│   └── operators/
│       └── validar_produtos_operator.py
├── sql/
│   └── init.sql             # Tabelas produtos, metricas, etl_log
├── data/
│   └── staging/             # JSON temporário entre tasks (evita XCom grande)
├── docker-compose.yml
└── instrucoes.md            # Enunciado completo da atividade
```

---

## Tabelas no banco

| Tabela     | Descrição                                      |
|------------|------------------------------------------------|
| `produtos` | Snapshot diário idempotente dos produtos da API |
| `metricas` | Métricas por categoria (média, min, max, qtd) |
| `etl_log`  | Log técnico de execuções                       |

A idempotência usa a coluna `data_referencia` (dia civil em `America/Sao_Paulo`): reprocessar o mesmo dia substitui os registros em vez de duplicar.

---

## Troubleshooting

- **DAG não aparece:** confira `docker compose logs airflow-scheduler` por erros de import.
- **Pool não encontrado:** crie `ecommerce_pool` (passo 3 acima).
- **Tabelas ausentes:** recrie o volume do banco com `docker compose down -v` e suba de novo (apaga dados existentes).
- **Mudanças no `docker-compose.yml`:** recrie os containers afetados com `docker compose up -d`.
