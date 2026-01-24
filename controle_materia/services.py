import pandas as pd
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from database import get_session

LOGGER = logging.getLogger("services")


def validar_colunas_excel(df: pd.DataFrame):
    required = {
        "turma", "materia", "professor_titular", "trimestre",
        "capitulo", "bloco", "status", "data_limite_da_entrega",
        "data_da_entrega", "validacao_operacional", "revisao_pedagogica",
        "diagramacao", "data_de_aprovacao_final", "obs"
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes no Excel: {missing}")


def calcular_alertas(df: pd.DataFrame, dias_alerta: int):
    hoje = datetime.today().date()
    limite = hoje + timedelta(days=dias_alerta)

    df["data_limite_da_entrega"] = pd.to_datetime(
        df["data_limite_da_entrega"], errors="coerce"
    ).dt.date

    alertas = df[
        (df["data_limite_da_entrega"].notna()) &
        (df["data_limite_da_entrega"] <= limite) &
        (df["status"] != "Concluído")
    ]

    LOGGER.info(f"{len(alertas)} alertas encontrados.")
    return alertas


def atualizar_registro(registro_id: int, campo: str, valor):
    """
    Atualiza dinamicamente um campo do registro.
    ATENÇÃO: campo deve ser validado antes de chamar essa função.
    """

    sql = text(f"""
        UPDATE edumanager.controle_materia
        SET {campo} = :valor
        WHERE id = :id
    """)

    session = get_session()
    try:
        session.execute(sql, {"valor": valor, "id": registro_id})
        session.commit()
        LOGGER.info(
            f"Registro {registro_id} atualizado: {campo} = {valor}"
        )
    except Exception:
        session.rollback()
        LOGGER.exception("Erro ao atualizar registro.")
        raise
    finally:
        session.close()
