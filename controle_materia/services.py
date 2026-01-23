import pandas as pd
import logging
from datetime import datetime, timedelta
from database import get_connection

LOGGER = logging.getLogger("services")


def validar_colunas_excel(df: pd.DataFrame):
    required = {
        "turma","materia","professor_titular","trimestre",
        "capitulo","bloco","status","data_limite_da_entrega",
        "data_da_entrega","validacao_operacional","revisao_pedagogica",
        "diagramacao","data_de_aprovacao_final","obs"
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes no Excel: {missing}")


def calcular_alertas(df: pd.DataFrame, dias_alerta: int):
    hoje = datetime.today().date()
    limite = hoje + timedelta(days=dias_alerta)

    df["data_limite_da_entrega"] = pd.to_datetime(df["data_limite_da_entrega"]).dt.date

    alertas = df[
        (df["data_limite_da_entrega"] <= limite) &
        (df["status"] != "Concluído")
    ]

    LOGGER.info(f"{len(alertas)} alertas encontrados.")
    return alertas

def atualizar_registro(registro_id: int, campo: str, valor):
    sql = f"UPDATE controle_materia SET {campo} = ? WHERE id = ?"
    with get_connection() as con:
        con.execute(sql, [valor, registro_id])
