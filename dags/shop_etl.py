import logging
from datetime import timedelta
import pendulum  # pyright: ignore[reportMissingImports]
from airflow.decorators import dag, task  # pyright: ignore[reportMissingImports]
from airflow.utils.task_group import TaskGroup
from plugins.operators.validar_produtos_operator import ValidarProdutosOperator  # pyright: ignore[reportMissingImports]

log = logging.getLogger(__name__)

# =============================================================================
# Configurações do DAG
# =============================================================================

POSTGRES_CONN_ID = "postgres_shop"      # Connection criada pelo airflow-init
TZ_BRASILIA = "America/Sao_Paulo"
SLA_PIPELINE = timedelta(hours=1)       # painel deve estar pronto até 1h após as 06:00

# =============================================================================
# Callbacks de alerta e SLA (requisitos 3.4 e 4.3)
# =============================================================================

def alerta_falha(context):
    ti = context["task_instance"]
    log.error(
        "[ALERTA SIMULADO] Pipeline ShopBrasil FALHOU\n"
        "  DAG: %s\n"
        "  Task: %s\n"
        "  Run: %s\n"
        "  Tentativa: %s\n"
        "  → E-mail simulado: pricing@shopbrasil.com\n"
        "  → Slack simulado: #alertas-dados",
        ti.dag_id,
        ti.task_id,
        context["run_id"],
        ti.try_number,
    )


def alerta_retry(context):
    ti = context["task_instance"]
    log.warning(
        "[RETRY] Task %s — tentativa %s (run: %s)",
        ti.task_id,
        ti.try_number,
        context["run_id"],
    )


def alerta_sucesso(context):
    ti = context["task_instance"]
    log.info(
        "[SUCESSO] Task %s concluída (run: %s)",
        ti.task_id,
        context["run_id"],
    )


def alerta_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    tasks_atrasadas = [t.task_id for t in task_list]
    log.error(
        "[SLA MISS SIMULADO] Pipeline ShopBrasil estourou o prazo de %s\n"
        "  DAG: %s\n"
        "  Tasks atrasadas: %s\n"
        "  → E-mail simulado: admin@shopbrasil.com\n"
        "  → Slack simulado: #alertas-dados",
        SLA_PIPELINE,
        dag.dag_id,
        tasks_atrasadas,
    )


DEFAULT_ARGS = {
    "owner": "shop",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,   # 2min → 4min → 8min
    "email_on_failure": False,
    "email_on_retry": False,
    "sla": SLA_PIPELINE,
    "sla_miss_callback": alerta_sla_miss,
    "on_failure_callback": alerta_falha,
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

    @task(
        task_id="buscar_produtos",
        on_retry_callback=alerta_retry,
        on_success_callback=alerta_sucesso,
    )
    def buscar_produtos() -> list[dict]:

        import requests # pyright: ignore[reportMissingModuleSource]
        
        url = "https://fakestoreapi.com/products"

        log.info("Buscando dados da API")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            resultados = response.json()

            log.info("Total de produtos coletados: %s", len(resultados))

            return resultados

        except Exception as e:
            log.error("Erro ao buscar produtos: %s", e)
            raise


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


    @task(task_id="calcular_metricas", pool="ecommerce_pool")
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

        validar = ValidarProdutosOperator(
            task_id="validar_produtos",
            upstream_task_id="ingestao.buscar_produtos",  # id completo com TaskGroup
        )

        total_de_valores = salvar_no_banco(dados_brutos, data_ref)

        dados_brutos >> validar >> total_de_valores

    with TaskGroup('analise') as analise_group:
        categorias = extrair_categorias(data_ref)
        metricas = calcular_metricas.partial(data_referencia=data_ref).expand(categoria=categorias)
        salvar_metricas(metricas, data_ref)

    ingestao_group >> analise_group     #analise só executa depois da ingestao

dag_instance = shop_etl()