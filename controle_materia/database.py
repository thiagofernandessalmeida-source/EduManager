import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

LOGGER = logging.getLogger("database")

# ======================================================
# Conexão PostgreSQL (Neon / Cloud / Local)
# ======================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não definida")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # acorda o Neon automaticamente
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

def get_session():
    return SessionLocal()

# ======================================================
# DDL
# ======================================================

def create_table():
    sql = """
    CREATE SCHEMA IF NOT EXISTS edumanager;

    CREATE TABLE IF NOT EXISTS edumanager.controle_materia (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        turma VARCHAR,
        materia VARCHAR,
        professor_titular VARCHAR,
        trimestre VARCHAR,
        capitulo VARCHAR,
        bloco VARCHAR,
        status VARCHAR,
        data_limite_da_entrega DATE,
        data_da_entrega DATE,
        validacao_operacional VARCHAR,
        revisao_pedagogica VARCHAR,
        diagramacao VARCHAR,
        data_de_aprovacao_final DATE,
        obs VARCHAR
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

    LOGGER.info("Tabela controle_materia verificada/criada.")

def create_professores_table():
    sql = """
    CREATE TABLE IF NOT EXISTS edumanager.professores (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        nome VARCHAR UNIQUE
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

    LOGGER.info("Tabela professores verificada/criada.")

# ======================================================
# CRUD
# ======================================================

def fetch_all(filters: dict | None = None) -> pd.DataFrame:
    base_sql = "SELECT * FROM edumanager.controle_materia"
    params = {}

    if filters:
        clauses = []
        for key, value in filters.items():
            clauses.append(f"{key} = :{key}")
            params[key] = value
        base_sql += " WHERE " + " AND ".join(clauses)

    with engine.connect() as conn:
        result = conn.execute(text(base_sql), params)
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def insert_record(data: dict):
    keys = ", ".join(data.keys())
    values = ", ".join([f":{k}" for k in data.keys()])

    sql = text(f"""
        INSERT INTO edumanager.controle_materia ({keys})
        VALUES ({values})
    """)

    session = get_session()
    try:
        session.execute(sql, data)
        session.commit()
        LOGGER.info("Registro inserido com sucesso.")
    except Exception:
        session.rollback()
        LOGGER.exception("Erro ao inserir registro.")
        raise
    finally:
        session.close()

def update_status(record_id: int, status: str):
    sql = text("""
        UPDATE edumanager.controle_materia
        SET status = :status
        WHERE id = :id
    """)
    session = get_session()
    try:
        session.execute(sql, {"status": status, "id": record_id})
        session.commit()
        LOGGER.info(f"Status atualizado para ID {record_id}.")
    except Exception:
        session.rollback()
        LOGGER.exception("Erro ao atualizar status.")
        raise
    finally:
        session.close()

def delete_record(record_id: int):
    sql = text("""
        DELETE FROM edumanager.controle_materia
        WHERE id = :id
    """)
    session = get_session()
    try:
        session.execute(sql, {"id": record_id})
        session.commit()
        LOGGER.info(f"Registro removido: ID {record_id}.")
    except Exception:
        session.rollback()
        LOGGER.exception("Erro ao deletar registro.")
        raise
    finally:
        session.close()

def inserir_professor(nome: str):
    sql = text("""
        INSERT INTO edumanager.professores (nome)
        VALUES (:nome)
        ON CONFLICT (nome) DO NOTHING
    """)
    session = get_session()
    try:
        session.execute(sql, {"nome": nome})
        session.commit()
    finally:
        session.close()

def listar_professores() -> pd.DataFrame:
    sql = "select distinct professor_titular as nome from 	edumanager.controle_materia"
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return pd.DataFrame(result.fetchall(), columns=result.keys())
