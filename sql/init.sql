
-- Snapshot idempotente de produtos brutos da API (DELETE + INSERT por data_referencia)
CREATE TABLE IF NOT EXISTS produtos (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    category VARCHAR(255) NOT NULL,
    data_referencia DATE NOT NULL,        -- dia de negócio (calendário Brasília)
    dag_run_id  VARCHAR(250),            -- rastreabilidade: qual run gerou o registro
    inserido_em TIMESTAMP DEFAULT NOW()  -- auditoria: quando gravou fisicamente
);

-- Snapshot idempotente de métricas por categoria (painel diário)
CREATE TABLE IF NOT EXISTS metricas (
    id SERIAL PRIMARY KEY,
    categoria VARCHAR(255) NOT NULL,
    data_referencia DATE NOT NULL,
    dag_run_id VARCHAR(250),
    quantidade INTEGER NOT NULL,
    media NUMERIC(10, 2) NOT NULL,
    maximo NUMERIC(10, 2) NOT NULL,
    minimo NUMERIC(10, 2) NOT NULL,
    inserido_em TIMESTAMP DEFAULT NOW(),
    UNIQUE (categoria, data_referencia)  -- garante 1 linha por categoria/dia no banco
);

-- Log técnico de execuções (extensão opcional do lab)
CREATE TABLE IF NOT EXISTS etl_log (
    id          SERIAL PRIMARY KEY,
    dag_id      VARCHAR(250),
    run_id      VARCHAR(250),
    task_id     VARCHAR(250),
    status      VARCHAR(50),
    registros   INTEGER DEFAULT 0,
    mensagem    TEXT,
    criado_em   TIMESTAMP DEFAULT NOW()
);


-- Permissões para o usuário da Connection postgres_shop
GRANT ALL ON ALL TABLES IN SCHEMA public TO shop;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO shop;

DO $$ BEGIN
    RAISE NOTICE 'Banco inicializado com sucesso!';
    RAISE NOTICE 'Tabelas: produtos, metricas, etl_log';
END $$;
