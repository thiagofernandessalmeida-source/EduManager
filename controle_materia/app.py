import streamlit as st
import pandas as pd
from logger_config import setup_logger
from database import (
    create_table, fetch_all,
    insert_record, update_status, delete_record
)
from services import validar_colunas_excel, calcular_alertas
import logging

setup_logger()
LOGGER = logging.getLogger("app")

st.set_page_config(page_title="Controle de Matéria", layout="wide")

create_table()

STATUS_CORES = {
    "Não iniciado": "🔴",
    "Em andamento": "🟡",
    "Concluído": "🟢"
}

st.title("📚 Controle e Gerenciamento de Matéria Escolar")

# ================= Sidebar =================
st.sidebar.header("🔔 Configurações")
dias_alerta = st.sidebar.slider(
    "Antecedência do alerta (dias)",
    1, 30, 7
)

# ================= Tabs =================
tabs = st.tabs(["📊 Visualização", "✍️ Cadastro", "⚙️ Configurações"])

# ================= Visualização =================
with tabs[0]:
    try:
        df = fetch_all()
    except Exception:
        st.error("Erro ao carregar dados.")
        st.stop()

    col1, col2 = st.columns(2)
    filtro_turma = col1.selectbox(
        "Filtrar por Turma",
        ["Todos"] + sorted(df["turma"].dropna().unique().tolist())
    )
    filtro_prof = col2.selectbox(
        "Filtrar por Professor",
        ["Todos"] + sorted(df["professor_titular"].dropna().unique().tolist())
    )

    filtros = {}
    if filtro_turma != "Todos":
        filtros["turma"] = filtro_turma
    if filtro_prof != "Todos":
        filtros["professor_titular"] = filtro_prof

    df_filtrado = fetch_all(filtros) if filtros else df

    # Alertas
    alertas = calcular_alertas(df_filtrado, dias_alerta)
    if not alertas.empty:
        st.warning(f"⚠️ {len(alertas)} matéria(s) com prazo próximo!")

    df_filtrado["status"] = df_filtrado["status"].apply(
        lambda x: f"{STATUS_CORES.get(x, '')} {x}"
    )

    st.dataframe(df_filtrado, use_container_width=True)

    st.subheader("🛠️ Editar Status / Excluir")
    record_id = st.number_input("ID do registro", min_value=1, step=1)
    novo_status = st.selectbox(
        "Novo Status",
        ["Não iniciado", "Em andamento", "Concluído"]
    )

    col_a, col_b = st.columns(2)
    if col_a.button("Atualizar Status"):
        try:
            update_status(record_id, novo_status)
            st.success("Status atualizado.")
        except Exception:
            st.error("Erro ao atualizar status.")

    if col_b.button("Excluir Registro"):
        try:
            delete_record(record_id)
            st.success("Registro excluído.")
        except Exception:
            st.error("Erro ao excluir registro.")

# ================= Cadastro =================
with tabs[1]:
    with st.form("form_cadastro"):
        data = {
            "unidade_tematica": st.text_input("Unidade Temática"),
            "trimestre": st.text_input("Trimestre"),
            "capitulo": st.text_input("Capítulo"),
            "subtemas_sugeridos": st.text_area("Subtemas Sugeridos"),
            "origem_referencia": st.text_input("Origem / Capítulo Referência"),
            "materia": st.text_input("Matéria"),
            "turma": st.text_input("Turma"),
            "status": st.selectbox(
                "Status",
                ["Não iniciado", "Em andamento", "Concluído"]
            ),
            "data_limite": st.date_input("Data Limite"),
            "professor_titular": st.text_input("Professor Titular"),
            "obs": st.text_area("Observações")
        }

        submitted = st.form_submit_button("Salvar")

        if submitted:
            try:
                insert_record(data)
                st.success("Registro cadastrado com sucesso.")
            except Exception:
                st.error("Erro ao salvar registro.")

    st.divider()
    st.subheader("📥 Importar Excel")

    uploaded = st.file_uploader(
        "Selecione um arquivo .xlsx",
        type=["xlsx"]
    )

    if uploaded:
        try:
            with st.spinner("Importando dados..."):
                df_excel = pd.read_excel(uploaded)
                validar_colunas_excel(df_excel)
                for _, row in df_excel.iterrows():
                    insert_record(row.to_dict())
            st.success("Importação concluída.")
        except Exception as e:
            LOGGER.exception("Erro na importação do Excel.")
            st.error(f"Erro ao importar Excel: {e}")

# ================= Configurações =================
with tabs[2]:
    st.info("Configurações futuras podem ser adicionadas aqui.")
