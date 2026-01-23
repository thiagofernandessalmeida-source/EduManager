import duckdb
import logging

DB_FILE = "controle_materia.db"
LOGGER = logging.getLogger("database")


def get_connection():
    return duckdb.connect(DB_FILE)


def create_table():
    sql_sequence = """
    CREATE SEQUENCE IF NOT EXISTS seq_controle_materia_id;
    """

    sql_table = """
    CREATE TABLE IF NOT EXISTS controle_materia (
        id BIGINT DEFAULT nextval('seq_controle_materia_id'),
        turma TEXT,
        materia TEXT,
        professor_titular TEXT,
        trimestre TEXT,
        capitulo TEXT,
        bloco TEXT,
        status TEXT,
        data_limite_da_entrega DATE,
        data_da_entrega DATE,
        validacao_operacional TEXT,
        revisao_pedagogica TEXT,
        diagramacao TEXT,
        data_de_aprovacao_final DATE,
        obs TEXT
    );
    """

    try:
        with get_connection() as con:
            con.execute(sql_sequence)
            con.execute(sql_table)

        LOGGER.info("Tabela e sequence criadas/verificadas com sucesso.")
    except Exception:
        LOGGER.exception("Erro ao criar tabela ou sequence.")
        raise


def fetch_all(filters=None):
    base_query = "SELECT * FROM controle_materia"
    params = []

    if filters:
        clauses = []
        for key, value in filters.items():
            clauses.append(f"{key} = ?")
            params.append(value)
        base_query += " WHERE " + " AND ".join(clauses)

    try:
        with get_connection() as con:
            return con.execute(base_query, params).fetchdf()
    except Exception:
        LOGGER.exception("Erro ao buscar dados.")
        raise


def insert_record(data: dict):
    keys = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    sql = f"INSERT INTO controle_materia ({keys}) VALUES ({placeholders})"

    try:
        with get_connection() as con:
            con.execute(sql, list(data.values()))
        LOGGER.info("Registro inserido com sucesso.")
        LOGGER.info("Iseridos {} registros.".format(len(data)))
    except Exception:
        LOGGER.exception("Erro ao inserir registro.")
        raise


def update_status(record_id: int, status: str):
    try:
        with get_connection() as con:
            con.execute(
                "UPDATE controle_materia SET status = ? WHERE id = ?",
                [status, record_id]
            )
        LOGGER.info(f"Status atualizado para ID {record_id}.")
    except Exception:
        LOGGER.exception("Erro ao atualizar status.")
        raise


def delete_record(record_id: int):
    try:
        with get_connection() as con:
            con.execute(
                "DELETE FROM controle_materia WHERE id = ?",
                [record_id]
            )
        LOGGER.info(f"Registro removido: ID {record_id}.")
    except Exception:
        LOGGER.exception("Erro ao deletar registro.")
        raise

def create_professores_table():
    sql = """
    CREATE TABLE IF NOT EXISTS professores (
        id INTEGER,
        nome TEXT UNIQUE
    );
    """
    with get_connection() as con:
        con.execute(sql)

def inserir_professor(nome: str):
    sql = "INSERT OR IGNORE INTO professores VALUES (NULL, ?)"
    with get_connection() as con:
        con.execute(sql, [nome])

def listar_professores():
    sql = "SELECT nome FROM professores ORDER BY nome"
    with get_connection() as con:
        return con.execute(sql).fetchdf()
