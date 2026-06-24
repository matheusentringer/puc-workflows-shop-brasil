import logging
from datetime import timedelta
import pendulum  # pyright: ignore[reportMissingImports]
from airflow.decorators import dag, task  # pyright: ignore[reportMissingImports]
from airflow.utils.task_group import TaskGroup  # pyright: ignore[reportMissingImports]

log = logging.getLogger(__name__)

# =============================================================================
# Configurações do DAG
# =============================================================================

POSTGRES_CONN_ID = "postgres_shop"      # Connection criada pelo airflow-init
TZ_BRASILIA = "America/Sao_Paulo"

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
    schedule="0 6 * * *",   # Executa todos os dias às 06:00 (horário de Brasília)
    start_date=pendulum.datetime(2024, 1, 1, tz=TZ_BRASILIA),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["shop", "etl", "airflow"],
    max_active_runs=1,
)
def shop_etl():

    @task(task_id="obter_data_referencia")
    def obter_data_referencia() -> str:
        data_ref = pendulum.now(TZ_BRASILIA).to_date_string()
        log.info("Data de referência (Brasília): %s", data_ref)
        return data_ref
    
    @task(task_id="buscar_produtos")
    def buscar_produtos() -> list[dict]:

        import requests # pyright: ignore[reportMissingModuleSource]
        
        url = "https://fakestoreapi.com/products"

        log.info("Buscando dados da API")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        resultados = response.json()

        log.info("Total de produtos coletados: %s", len(resultados))

        return resultados


    @task(task_id="salvar_no_banco")
    def salvar_no_banco(registros: list[dict], data_referencia: str, **context) -> int:

        from airflow.providers.postgres.hooks.postgres import PostgresHook # pyright: ignore[reportMissingImports]

        run_id = context["run_id"]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            cur.execute(
                "DELETE FROM produtos WHERE data_referencia = %s",
                (data_referencia,),
            )

            insert_sql = """
                INSERT INTO produtos (product_id, title, price, category, data_referencia, dag_run_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            valores = [
                (
                    r["id"],
                    r["title"],
                    r["price"],
                    r["category"],
                    data_referencia,
                    run_id
                )
                for r in registros
            ]

            cur.executemany(insert_sql, valores)
            conn.commit()

            total = len(valores)
            log.info("✓ %d registros inseridos na tabela 'produtos'", total)
            log.info("  run_id: %s | data: %s", run_id, data_referencia)

            return total
            
        except Exception as e:
            conn.rollback()
            log.error("Erro ao inserir no banco: %s", e)
            raise
        finally:
            cur.close()
            conn.close()


    @task(task_id="extrair_categorias")
    def extrair_categorias(data_referencia: str) -> list[str]:

        from airflow.providers.postgres.hooks.postgres import PostgresHook # pyright: ignore[reportMissingImports]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        try:
            select_sql = """
                SELECT DISTINCT category FROM produtos WHERE data_referencia = %s
            """

            rows = hook.get_records(select_sql, parameters=(data_referencia,),)
            categorias = [row[0] for row in rows]

            log.info("Categorias extraídas: %s", categorias)
            return categorias

        except Exception as e:
            log.error("Erro ao extrair categorias do banco")
            raise


    @task(task_id="calcular_metricas")
    def calcular_metricas(categoria: str, data_referencia: str, **context):

        from airflow.providers.postgres.hooks.postgres import PostgresHook # pyright: ignore[reportMissingImports]
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        log.info("Calculando métricas da categoria: %s", categoria)

        try:
            select_sql = """
                SELECT price FROM produtos WHERE category = %s AND data_referencia = %s
            """

            log.info("Buscando registros no banco")
            rows = hook.get_records(select_sql, parameters=(categoria, data_referencia))

            prices = [float(row[0]) for row in rows]
            log.info("%i registros encontrados", len(prices))

            quantity = len(prices)
            average = round((sum(prices) / quantity), 2)
            maximum = round(max(prices), 2)
            minimum = round(min(prices), 2)

            log.info("Métricas calculadas: %s", {
                "quantidade": quantity,
                "média": average,
                "máximo": maximum,
                "mínimo": minimum
            })

            return {
                "categoria": categoria,
                "quantidade": quantity,
                "media": average,
                "maximo": maximum,
                "minimo": minimum
            }

        except Exception as e:
            log.error("Erro ao calcular métricas: %s", e)
            raise


    @task(task_id="salvar_metricas")
    def salvar_metricas(resultados: list[dict], data_referencia: str, **context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook # pyright: ignore[reportMissingImports]
        run_id = context["run_id"]
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            cur.execute(
                "DELETE FROM metricas WHERE data_referencia = %s",
                (data_referencia,),
            )

            insert_sql = """
                INSERT INTO metricas
                    (categoria, data_referencia, dag_run_id, quantidade, media, maximo, minimo)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            valores = [
                (
                    m["categoria"],
                    data_referencia,
                    run_id,
                    m["quantidade"],
                    m["media"],
                    m["maximo"],
                    m["minimo"]
                )
                for m in resultados
            ]

            cur.executemany(insert_sql, valores)
            conn.commit()

            total = len(valores)
            log.info("✓ %d registros inseridos na tabela 'metricas'", total)
            log.info("  run_id: %s | data: %s", run_id, data_referencia)


        except Exception as e:
            conn.rollback()
            log.error("Erro ao salvar métricas no banco de dados: %s", e)
            raise




    with TaskGroup('ingestao') as ingestao_group:
        data_ref = obter_data_referencia()
        dados_brutos = buscar_produtos()
        total_de_valores = salvar_no_banco(dados_brutos, data_ref)

    with TaskGroup('analise') as analise_group:
        categorias = extrair_categorias(data_ref)
        metricas = calcular_metricas.partial(data_referencia=data_ref).expand(categoria=categorias)
        salvar_metricas(metricas, data_ref)

    ingestao_group >> analise_group     #analise só executa depois da ingestao

dag_instance = shop_etl()