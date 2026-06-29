"""
Operador customizado (requisito opcional 4.1).

Valida o schema mínimo dos produtos da FakeStore antes de persistir no banco.
Reutilizável em outras DAGs via plugins/operators/.
"""

from airflow.models import BaseOperator
from airflow.utils.context import Context


class ValidarProdutosOperator(BaseOperator):
    """Valida campos obrigatórios e tipos de cada produto retornado pela API."""

    def __init__(
        self,
        upstream_task_id: str,
        campos_obrigatorios: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # task_id completo da task upstream (inclui prefixo do TaskGroup, se houver)
        self.upstream_task_id = upstream_task_id
        self.campos_obrigatorios = campos_obrigatorios or [
            "id", "title", "price", "category",
        ]

    def execute(self, context: Context):
        import json
        from pathlib import Path

        ti = context["ti"]
        # XCom traz o caminho do JSON gerado por buscar_produtos
        arquivo = ti.xcom_pull(task_ids=self.upstream_task_id)

        if not arquivo:
            raise ValueError("Nenhum arquivo de produtos recebido para validação")

        if not isinstance(arquivo, str):
            raise ValueError(f"Esperava caminho str, recebeu {type(arquivo)}")

        path = Path(arquivo)
        if not path.exists():
            raise ValueError(f"Arquivo não encontrado: {path}")

        with path.open(encoding="utf-8") as f:
            produtos = json.load(f)

        if not isinstance(produtos, list):
            raise ValueError(f"Esperava list no JSON, recebeu {type(produtos)}")

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
        return len(produtos)
