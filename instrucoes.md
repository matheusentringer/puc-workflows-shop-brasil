# ATIVIDADE 01 - AIRFLOW

## 1. Contexto

Você acaba de assumir como líder técnico (tech lead) do time de dados da **ShopBrasil**, um marketplace de e-commerce em rápido crescimento.

Todas as manhãs, antes que a equipe de pricing e os gerentes de categoria comecem o expediente, eles **abrem um painel com o panorama de preços por categoria, preço médio, mínimo, máximo e quantidade de produtos** para decidir as promoções e reposições do dia.

Atualmente, esse painel é alimentado por um **script Python**, agendado via **cron**, que busca os dados de uma API de catálogo.

### 1.1 Situação-Problema

A arquitetura atual é frágil, pois, quando a API oscila de madrugada, o script falha silenciosamente e o time fica sem dados.

Quando alguém o executa "na mão" novamente, os números aparecem duplicados na base de dados. Além disso, a cada nova categoria que surge, é preciso editar o código linha a linha.

Com isso, temos uma arquitetura que:

- **não escala**;
- **não é confiável**;
- **ninguém dorme tranquilo**.

---

## 2. Objetivo

Sua missão é projetar e conduzir seu time na construção de um pipeline de verdade no **Apache Airflow**, substituindo o script executado via cron.

O pipeline deve:

1. Coletar os produtos da API de catálogo (representada pela FakeStore API);
2. Calcular métricas por categoria;
3. Gravar os resultados na base analítica PostgreSQL.

Como tech lead, você não apenas implementa a referência, mas também define os padrões que o time seguirá.

### Na prática, o pipeline precisa:

- Rodar automaticamente todos os dias às **06:00 (horário de Brasília)**, antes do início do expediente;
- Resistir a instabilidades da API, realizando novas tentativas sem comprometer a execução completa;
- Escalar automaticamente conforme novas categorias forem adicionadas;
- Nunca duplicar dados durante reprocessamentos;
- Emitir alertas quando ocorrerem falhas;
- Ser modular, organizado e de fácil manutenção.

---

## 3. Requisitos Obrigatórios

### 3.1 Fonte de Dados

- API: https://fakestoreapi.com/docs
  - Este é o Swagger da API da qual os dados deverão ser capturados.

### 3.2 Modelagem e Estrutura

- Utilizar **TaskFlow API** (`@dag` / `@task`);
- As dependências devem surgir da chamada das funções;
- Utilizar **XComs automáticos via retorno (`return`)**, passando apenas dados pequenos entre tasks;
- O pipeline deve conter, de forma identificável:
  - Topologia linear;
  - Fan-out (mapeamento por categoria);
  - Fan-in (consolidação).

### 3.3 Agendamento

- Timezone ancorado em `America/Sao_Paulo` (utilizando `pendulum`);
- Configurar `start_date`;
- Configurar `catchup=False`.

### 3.4 Ingestão Resiliente

- Criar a task **Buscar Produtos** com:
  - Retry;
  - Exponential backoff;
- Implementar tratamento de erros com `try/except + raise` para acionar o retry;
- Configurar os seguintes callbacks na task crítica:
  - `on_failure_callback`
  - `on_retry_callback`
  - `on_success_callback`

### 3.5 Processamento Paralelo

- Calcular métricas por categoria utilizando **Dynamic Task Mapping** (`.expand(...)`);
- Utilizar um pool para limitar a concorrência das tasks mapeadas:
  - Pool: `ecommerce_pool`
  - Limite: 2 slots

### 3.6 Organização

- Agrupar as tasks em pelo menos **2 TaskGroups**.
- Exemplo:
  - Ingestão;
  - Análise.

### 3.7 Persistência no PostgreSQL

Criar uma task responsável por:

- Salvar os registros no banco de dados com consistência suficiente para evitar duplicidades;
- Utilizar `PostgresHook` e uma Connection do Airflow.

#### Requisito de Idempotência

A gravação deve ser **idempotente**, ou seja:

> Reexecutar a mesma DAG Run não deve gerar linhas duplicadas.

---

## 4. Requisitos Opcionais

### 4.1 Operador Customizado

Criar um operador:

- `ValidarProdutosOperator` (`BaseOperator`)

Objetivo:

- Validar o schema dos produtos antes do processamento.

### 4.2 Tabela de Histórico

Além do snapshot idempotente:

- Gravar uma segunda tabela em modo **append**;
- Registrar a data da execução.

Objetivo:

- Acompanhar a evolução dos preços ao longo do tempo.

### 4.3 SLA / Alerta

Configurar:

- Um **SLA**, ou
- Um `on_failure_callback` que simule o envio de alertas.

---

## 5. Modo de Entrega

- Disponibilizar o projeto em um repositório (GitHub, GitLab etc.);
- Garantir acesso de visualização ao repositório;
- Entregar todo o pipeline em um projeto dockerizado;
- Publicar o link do projeto.
