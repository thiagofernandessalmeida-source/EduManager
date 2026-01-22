import pandas as pd
import logging
from datetime import datetime, timedelta
from database import get_connection

LOGGER = logging.getLogger("services")


def validar_colunas_excel(df: pd.DataFrame):
    required = {
        "unidade_tematica", "trimestre", "capitulo",
        "subtemas_sugeridos", "origem_referencia",
        "materia", "turma", "status",
        "data_limite", "professor_titular", "obs"
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes no Excel: {missing}")


def calcular_alertas(df: pd.DataFrame, dias_alerta: int):
    hoje = datetime.today().date()
    limite = hoje + timedelta(days=dias_alerta)

    df["data_limite"] = pd.to_datetime(df["data_limite"]).dt.date

    alertas = df[
        (df["data_limite"] <= limite) &
        (df["status"] != "Concluído")
    ]

    LOGGER.info(f"{len(alertas)} alertas encontrados.")
    return alertas

def atualizar_registro(registro_id: int, campo: str, valor):
    sql = f"UPDATE controle_materia SET {campo} = ? WHERE id = ?"
    with get_connection() as con:
        con.execute(sql, [valor, registro_id])
