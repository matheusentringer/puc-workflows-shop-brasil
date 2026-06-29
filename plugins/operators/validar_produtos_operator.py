# plugins/operators/validar_produtos_operator.py

from airflow.models import BaseOperator
from airflow.utils.context import Context


class ValidarProdutosOperator(BaseOperator):
    """
    Valida o schema mínimo dos produtos retornados pela API
    antes de persistir ou calcular métricas.
    """

    def __init__(
        self,
        upstream_task_id: str,
        campos_obrigatorios: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.upstream_task_id = upstream_task_id
        self.campos_obrigatorios = campos_obrigatorios or [
            "id", "title", "price", "category"
        ]

    def execute(self, context: Context):
        ti = context["ti"]
        produtos = ti.xcom_pull(task_ids=self.upstream_task_id)

        if not produtos:
            raise ValueError("Nenhum produto recebido para validação")

        if not isinstance(produtos, list):
            raise ValueError(f"Esperava list, recebeu {type(produtos)}")

        for i, produto in enumerate(produtos):
            if not isinstance(produto, dict):
                raise ValueError(f"Produto {i}: esperava dict, recebeu {type(produto)}")

            for campo in self.campos_obrigatorios:
                if campo not in produto:
                    raise ValueError(f"Produto {i}: campo '{campo}' ausente")

            if not isinstance(produto["id"], int):
                raise ValueError(f"Produto {i}: 'id' deve ser int")

            if not isinstance(produto["title"], str) or not produto["title"].strip():
                raise ValueError(f"Produto {i}: 'title' inválido")

            if not isinstance(produto["price"], (int, float)) or produto["price"] < 0:
                raise ValueError(f"Produto {i}: 'price' inválido")

            if not isinstance(produto["category"], str) or not produto["category"].strip():
                raise ValueError(f"Produto {i}: 'category' inválido")

        self.log.info("✓ %d produtos validados com sucesso", len(produtos))
        return len(produtos)  # opcional: push XCom com total validado