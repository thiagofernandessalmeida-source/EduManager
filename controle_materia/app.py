import streamlit as st
import pandas as pd
import logging
from logger_config import setup_logger
from database import (
    create_table, fetch_all,
    insert_record, update_status, delete_record,
    create_professores_table, inserir_professor, listar_professores
)
from services import validar_colunas_excel, calcular_alertas

# ================= Setup =================
setup_logger()
LOGGER = logging.getLogger("app")

st.set_page_config(page_title="Controle de Matéria", layout="wide")

if "db_initialized" not in st.session_state:
    create_table()
    create_professores_table()
    st.session_state.db_initialized = True


STATUS_CORES = {
    "Não iniciado": "🔴",
    "Em andamento": "🟡",
    "Concluído": "🟢"
}

st.title("📚 EduManager – Controle e Gerenciamento de Matéria Escolar")

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
    df = fetch_all()

    professores_df = listar_professores()
    professores = professores_df["nome"].tolist() if not professores_df.empty else []

    # ================= Filtros =================
    st.subheader("🔎 Filtros")

    col1, col2, col3, col4 = st.columns(4)

    filtro_turma = col1.selectbox(
        "Filtrar por Turma",
        ["Todos"] + sorted(df["turma"].dropna().unique().tolist())
    )

    filtro_prof = col2.selectbox(
        "Filtrar por Professor",
        ["Todos"] + sorted(df["professor_titular"].dropna().unique().tolist())
    )

    filtro_materia = col3.selectbox(
        "Filtrar por Matéria",
        ["Todos"] + sorted(df["materia"].dropna().unique().tolist())
    )

    filtro_capitulo = col4.selectbox(
        "Filtrar por Capítulo",
        ["Todos"] + sorted(df["capitulo"].dropna().unique().tolist())
    )

    col_dias, _ = st.columns([1, 3])
    filtro_dias = col_dias.number_input(
        "Mostrar matérias com prazo em até (dias)",
        min_value=0,
        value=0,
        help="0 = mostrar todas"
    )

    # ================= Aplicar filtros =================
    hoje = pd.Timestamp.today().normalize()
    df_filtrado = df.copy()

    if filtro_turma != "Todos":
        df_filtrado = df_filtrado[df_filtrado["turma"] == filtro_turma]

    if filtro_prof != "Todos":
        df_filtrado = df_filtrado[df_filtrado["professor_titular"] == filtro_prof]

    if filtro_materia != "Todos":
        df_filtrado = df_filtrado[df_filtrado["materia"] == filtro_materia]

    if filtro_capitulo != "Todos":
        df_filtrado = df_filtrado[df_filtrado["capitulo"] == filtro_capitulo]

    # 👉 0 = não filtra
    if filtro_dias > 0:
        df_filtrado = df_filtrado[
            (df_filtrado["data_limite_da_entrega"].notna()) &
            ((pd.to_datetime(df_filtrado["data_limite_da_entrega"]) - hoje).dt.days <= filtro_dias)
        ]

    # ================= Alertas =================
    df_filtrado["alerta"] = df_filtrado["data_limite_da_entrega"].apply(
        lambda d: "⚠️ Prazo próximo"
        if pd.notna(d) and (pd.to_datetime(d) - hoje).days <= dias_alerta
        else ""
    )

    st.subheader("✏️ Controle de Matérias")

    edited_df = st.data_editor(
        df_filtrado,
        use_container_width=True,
        num_rows="fixed",
        key="editor_materias",
        column_config={
            "turma": st.column_config.TextColumn(
                "Turma"
            ),
            "materia": st.column_config.TextColumn(
                "Materia"
            ),
            "trimestre": st.column_config.TextColumn(
                "Trimestre"
            ),
            "capitulo": st.column_config.TextColumn(
                "Capitulo"
            ),
            "bloco": st.column_config.TextColumn(
                "Bloco"
            ),
            "validacao_operacional": st.column_config.TextColumn(
                "Validação Operacional"
            ),
            "revisao_pedagogica": st.column_config.TextColumn(
                "Revisão Pedagógica"
            ),
            "diagramacao": st.column_config.TextColumn(
                "Diagramação"
            ),
            "obs": st.column_config.TextColumn(
                "Observação"
            ),
            "status": st.column_config.SelectboxColumn(
                "Status",
                options=["Não iniciado", "Em andamento", "Concluído"]
            ),
            "professor_titular": st.column_config.SelectboxColumn(
                "Professor Titular",
                options=professores
            ),
            "data_limite_da_entrega": st.column_config.DateColumn(
                "Data Limite da Entrega",
                format="DD/MM/YYYY"
            ),
            "data_da_entrega": st.column_config.DateColumn(
                "Data da Entrega",
                format="DD/MM/YYYY"
            ),
            "data_de_aprovacao_final": st.column_config.DateColumn(
                "Data de Aprovação Final",
                format="DD/MM/YYYY"
            ),
            "alerta": st.column_config.TextColumn(
                "⚠️ Alerta",
                disabled=True
            )
        }
    )

    col_save, _ = st.columns([1, 5])

    if col_save.button("💾 Salvar alterações"):
        try:
            for _, row in edited_df.iterrows():
                original = df[df["id"] == row["id"]].iloc[0]

                changes = {
                    col: row[col]
                    for col in df.columns
                    if row[col] != original[col]
                }

                if changes:
                    set_clause = ", ".join([f"{k} = ?" for k in changes])
                    values = list(changes.values()) + [row["id"]]

                    from database import get_connection
                    with get_connection() as con:
                        con.execute(
                            f"UPDATE controle_materia SET {set_clause} WHERE id = ?",
                            values
                        )

            st.success("Alterações salvas com sucesso.")
            st.rerun()

        except Exception:
            st.error("Erro ao salvar alterações.")

# ================= Cadastro =================
with tabs[1]:
    professores_df = listar_professores()
    professores = professores_df["nome"].tolist() if not professores_df.empty else []

    st.session_state.pop("form_enviado", None)

    with st.form("form_cadastro"):
        data = {
            "turma": st.text_input("Turma"),
            "materia": st.text_input("Matéria"),
            "professor_titular": st.selectbox(
                "Professor Titular",
                professores
            ),
            "trimestre": st.text_input("Trimestre"),
            "capitulo": st.text_input("Capítulo"),
            "bloco": st.text_input("Bloco"),
            "status": st.selectbox(
                "Status",
                ["Não iniciado", "Em andamento", "Concluído"]
            ),
            "data_limite_da_entrega": st.date_input("Data Limite da Entrega", format="DD/MM/YYYY"),
            "data_da_entrega": st.date_input("Data da Entrega", format="DD/MM/YYYY"),
            "validacao_operacional": st.text_input("Validacao Operacional"),
            "revisao_pedagogica": st.text_input("Revisao Pedagogica"),
            "diagramacao": st.text_input("Diagramacao"),
            "data_de_aprovacao_final": st.date_input("Data de Aprovacao Final", format="DD/MM/YYYY"),
            "obs": st.text_area("Observações")
        }

        submitted = st.form_submit_button("Salvar")

        if submitted:
            insert_record(data)
            st.success("Registro cadastrado com sucesso.")

            if "form_enviado" not in st.session_state:
                st.session_state.form_enviado = True
                st.rerun()

    st.divider()
    st.subheader("📥 Importar Excel")

    if "upload_processado" not in st.session_state:
        st.session_state.upload_processado = False

    uploaded = st.file_uploader(
        "Selecione um arquivo .xlsx",
        type=["xlsx"],
        on_change=lambda: st.session_state.update({"upload_processado": False})
    )

    if uploaded and not st.session_state.upload_processado:
        with st.spinner("Importando dados..."):
            df_excel = pd.read_excel(uploaded)
            validar_colunas_excel(df_excel)
            df_excel = df_excel.astype(str)
            df_excel = df_excel.replace(
                {"NaT": None, "nan": None, "None": None}
            )

            for _, row in df_excel.iterrows():
                insert_record(row.to_dict())

        st.session_state.upload_processado = True
        st.success("Importação concluída.")
        st.rerun()

# ================= Configurações =================
with tabs[2]:
    st.subheader("👨‍🏫 Cadastro de Professores")

    nome_prof = st.text_input("Nome do professor")

    if st.button("Adicionar professor"):
        if nome_prof.strip():
            inserir_professor(nome_prof.strip())
            st.success("Professor cadastrado.")
            st.rerun()
        else:
            st.warning("Informe um nome válido.")
