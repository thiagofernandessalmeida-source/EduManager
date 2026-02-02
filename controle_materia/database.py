import os
import logging
import duckdb as db
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
    base_sql = """
        SELECT 
            a.id
            ,a.turma                   
            ,a.materia                 
            ,a.professor_titular       
            ,a.trimestre               
            ,a.capitulo                
            ,a.bloco, bgr.grupo                   
            ,case 
                when coalesce (b.status, a.status) is not null 
                    and coalesce (b.status, a.status) = 'Concluido' 
                    and a.data_de_aprovacao_final is null then 'Aguardando revisão pedagógica'
                    else coalesce (b.status, a.status)
                    end as status                   
            ,case when b.bloco is not null then b.data_limite_da_entrega else a.data_limite_da_entrega end as data_limite_da_entrega
            ,a.data_da_entrega         
            ,a.validacao_operacional   
            ,a.revisao_pedagogica      
            ,a.diagramacao             
            ,a.data_de_aprovacao_final 
            ,a.obs 
            FROM edumanager.controle_materia a
            left join edumanager.bloco_grupo_relation bgr on a.id = bgr.id
            left join (select a.bloco, a.data_limite_da_entrega, a.grupo
            ,case 
                when current_date < a.data_limite_da_entrega 
                    and a.bloco::int - 1 = 0 
                        then 'Em andamento' 
                when current_date >= a.data_limite_da_entrega 
                    and a.bloco::int - 1 = 0 
                        then 'Concluido'
                when current_date < a.data_limite_da_entrega 
                    and current_date > (select b.data_limite_da_entrega from edumanager.bloco b where b.bloco::int = a.bloco::int - 1 and a.grupo = b.grupo)
                        then 'Em andamento'
                when current_date < a.data_limite_da_entrega 
                    and current_date < (select b.data_limite_da_entrega from edumanager.bloco b where b.bloco::int = a.bloco::int - 1 and a.grupo = b.grupo)
                        then 'Não iniciado'
                else 'Não iniciado'
            end as status
            from edumanager.bloco a ) b on a.bloco = b.bloco
            and b.grupo = bgr.grupo
            ORDER BY a.id
    """
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


def insert_bloco(data: dict):
    session = get_session()

    try:
        bloco = data["bloco"]
        data_limite = data["data_limite_da_entrega"]

        # 1️⃣ Verifica se bloco + data já existem
        check_sql = text("""
            SELECT 1
            FROM edumanager.bloco
            WHERE bloco = :bloco
              AND data_limite_da_entrega = :data_limite
        """)

        exists = session.execute(
            check_sql,
            {"bloco": bloco, "data_limite": data_limite}
        ).first()

        if exists:
            return {
                "success": False,
                "message": "Bloco e data já existem no cadastro."
            }

        # 2️⃣ Busca a maior sequência do grupo para o bloco
        seq_sql = text("""
            SELECT
                MAX(
                    CAST(
                        SPLIT_PART(grupo, '.', 2) AS INTEGER
                    )
                ) AS max_seq
            FROM edumanager.bloco
            WHERE bloco = :bloco
        """)

        result = session.execute(seq_sql, {"bloco": bloco}).first()
        max_seq = result.max_seq or 0
        next_seq = max_seq + 1

        grupo = f"Grupo {bloco}.{next_seq}"

        # 3️⃣ Insere novo registro
        insert_sql = text("""
            INSERT INTO edumanager.bloco (bloco, data_limite_da_entrega, grupo)
            VALUES (:bloco, :data_limite, :grupo)
        """)

        session.execute(insert_sql, {
            "bloco": bloco,
            "data_limite": data_limite,
            "grupo": grupo
        })

        session.commit()

        return {
            "success": True,
            "message": "Bloco cadastrado com sucesso.",
            "grupo": grupo
        }

    except Exception as e:
        session.rollback()
        raise e
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

def update_bloco_grupo_relation (record_id: int, bloco: str ,grupo: str):
    sql = text("""
        UPDATE edumanager.bloco_grupo_relation
        SET bloco = :bloco, grupo = :grupo
        WHERE id = :id
    """)
    session = get_session()
    try:
        session.execute(sql, {"bloco": bloco, "grupo":grupo, "id": record_id})
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

def login_user(email: str, password: str) -> str | None:
    sql = text("""
        SELECT status
        FROM edumanager.users
        WHERE email = :email
          AND password = :password
        LIMIT 1
    """)

    with engine.connect() as conn:
        result = conn.execute(
            sql,
            {"email": email, "password": password}
        ).fetchone()

    if result:
        return result.status   # ou result[0]
    return None


def cadastrar_novo_usuario(email: str, password: str, status: str):

    sql = text("""
        INSERT INTO edumanager.users (email, password, status)
        VALUES (:email, :password, :status)
    """)

    with engine.begin() as conn:
        conn.execute(
            sql,
            {"email": email, "password": password, "status": status}
        )

    return f"user {email} cadastrado com sucesso com status {status}"




