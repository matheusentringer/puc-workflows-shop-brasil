import logging
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# =============================================================================
# Configurações do DAG
# =============================================================================

POSTGRES_CONN_ID = "postgres_shop"      # Connection criada pelo airflow-init

DEFAULT_ARGS = {
    "owner": "shop",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,   # 2min → 4min → 8min
    "email_on_failure": False,
    "email_on_retry": False,
}

@dag(
    dag_id="shop_etl",
    description="Pipeline de ETL para o projeto ShopBrasil",
    schedule="0 6 * * *",   # Executa todos os dias às 06:00
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["shop", "etl", "airflow"],
    max_active_runs=1,
)
def shop_etl():
    
    @task(task_id="buscar_produtos")
    def buscar_produtos() -> list[dict]:

        import requests  # importar dentro da task = isolamento correto
        
        url = "https://fakestoreapi.com/products"

        log.info("Buscando dados da API")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        resultados = response.json()

        log.info("Total de produtos coletados: %s", len(resultados))

        return resultados



    @task(task_id="salvar_no_banco")
    def salvar_no_banco(registros: list[dict], **context) -> int:

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        run_id = context["run_id"]
        data_execucao = context["ds"]           # YYYY-MM-DD da execução

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:

            insert_sql = """
                INSERT INTO produtos (product_id, title, price, category, dag_run_id)
                VALUES (%s, %s, %s, %s, %s)
            """
            valores = [
                (
                    r["id"],
                    r["title"],
                    r["price"],
                    r["category"],
                    run_id,
                )
                for r in registros
            ]

            cur.executemany(insert_sql, valores)
            conn.commit()

            total = len(valores)
            log.info("✓ %d registros inseridos na tabela 'produtos'", total)
            log.info("  run_id: %s | data: %s", run_id, data_execucao)

            return total
            
        except Exception as e:
            conn.rollback()
            log.error("Erro ao inserir no banco: %s", e)
            raise
        finally:
            cur.close()
            conn.close()
            cur = conn.cursor()

    with TaskGroup('ingestao') as ingestao_group:
        dados_brutos = buscar_produtos()
        total = salvar_no_banco(dados_brutos)

dag_instance = shop_etl()