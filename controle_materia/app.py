from time import sleep
import streamlit as st
import pandas as pd
import logging
from sqlalchemy import text
from logger_config import setup_logger
from database import (
    create_table, fetch_all,
    insert_record, delete_record,
    create_professores_table, inserir_professor, listar_professores,
    get_session, cadastrar_novo_usuario,insert_bloco
)
from services import validar_colunas_excel
from loggin import render_login

# ================= Login gate =================
if "logged" not in st.session_state:
    st.session_state.logged = False

if not st.session_state.logged:
    render_login()
    st.stop()   # ⛔ BLOQUEIA o resto do app

# ================= Setup =================
setup_logger()
LOGGER = logging.getLogger("app")

st.set_page_config(page_title="Controle de Matéria", layout="wide")

if "db_initialized" not in st.session_state:
    create_table()
    create_professores_table()
    st.session_state.db_initialized = True

st.title("📚 EduManager – Controle e Gerenciamento de Matéria Escolar")

# ================= Sidebar =================
st.sidebar.header("🔔 Configurações")
dias_alerta = st.sidebar.slider(
    "Antecedência do alerta (dias)",
    1, 30, 7
)

st.sidebar.divider()
st.sidebar.subheader("📖 App Version")
st.sidebar.info(
    """
    **EduManager** v1.0
    """
)

# ================= Tabs =================

if st.session_state.status in ["super_admin", "admin"]:
    tabs = st.tabs(["📊 Visualização", "✍️ Cadastro", "👤 Cadastro de Usuario", "📖 Sobre"])


    # ================= Visualização =================
    with tabs[0]:
        df = fetch_all()

        professores_df = listar_professores()
        professores = professores_df["nome"].tolist() if not professores_df.empty else []

        st.subheader("🔎 Filtros")

        col1, col2, col3, col4 = st.columns(4)

        filtro_turma = col1.selectbox(
            "Turma",
            ["Todos"] + sorted(df["turma"].dropna().unique().tolist())
        )

        filtro_prof = col2.selectbox(
            "Professor",
            ["Todos"] + sorted(df["professor_titular"].dropna().unique().tolist())
        )

        filtro_materia = col3.selectbox(
            "Matéria",
            ["Todos"] + sorted(df["materia"].dropna().unique().tolist())
        )

        filtro_capitulo = col4.selectbox(
            "Capítulo",
            ["Todos"] + sorted(df["capitulo"].dropna().unique().tolist())
        )

        filtro_dias = st.number_input(
            "Mostrar matérias com prazo em até (dias)",
            min_value=0,
            value=0,
            help="0 = mostrar todas"
        )

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

        if filtro_dias > 0:
            df_filtrado = df_filtrado[
                (df_filtrado["data_limite_da_entrega"].notna()) &
                ((pd.to_datetime(df_filtrado["data_limite_da_entrega"]) - hoje).dt.days <= filtro_dias)
            ]

        df_filtrado["alerta"] = df_filtrado["data_limite_da_entrega"].apply(
            lambda d: "⚠️ Prazo próximo"
            if pd.notna(d) and (pd.to_datetime(d) - hoje).days <= dias_alerta
            else ""
        )

        df_filtrado["excluir"] = False

        st.subheader("✏️ Controle de Matérias")

        edited_df = st.data_editor(
            df_filtrado,
            use_container_width=True,
            num_rows="fixed",
            key="editor_materias",
            column_config={
                "excluir": st.column_config.CheckboxColumn("🗑️ Excluir"),
                "status": st.column_config.TextColumn(
                    "Status",
                    disabled = True
                ),
                "professor_titular": st.column_config.TextColumn(
                    "Professor Titular"
                ),
                "data_limite_da_entrega": st.column_config.DateColumn(
                    "Data Limite",
                    format="DD/MM/YYYY"
                ),
                "data_da_entrega": st.column_config.DateColumn(
                    "Data da Entrega",
                    format="DD/MM/YYYY"
                ),
                "data_de_aprovacao_final": st.column_config.DateColumn(
                    "Aprovação Final",
                    format="DD/MM/YYYY"
                ),
                "alerta": st.column_config.TextColumn(
                    "⚠️ Alerta",
                    disabled=True
                )
            }
        )

        col_save, col_delete = st.columns(2)

        # ===== SALVAR ALTERAÇÕES =====
        if col_save.button("💾 Salvar alterações"):
            session = get_session()
            try:
                for _, row in edited_df.iterrows():
                    original = df[df["id"] == row["id"]].iloc[0]

                    changes = {
                        col: row[col]
                        for col in df.columns
                        if col not in ["alerta"] and row[col] != original[col]
                    }

                    if changes:
                        set_clause = ", ".join([f"{k} = :{k}" for k in changes])
                        sql = text(f"""
                            UPDATE edumanager.controle_materia
                            SET {set_clause}
                            WHERE id = :id
                        """)
                        changes["id"] = row["id"]
                        session.execute(sql, changes)

                session.commit()
                st.success("Alterações salvas com sucesso.")
                st.rerun()

            except Exception:
                session.rollback()
                LOGGER.exception("Erro ao salvar.")
                st.error("Erro ao salvar alterações.")
            finally:
                session.close()

        # ===== EXCLUIR =====
        if col_delete.button("🗑️ Excluir selecionados"):
            ids = edited_df[edited_df["excluir"] == True]["id"].tolist()

            if not ids:
                st.warning("Nenhum registro selecionado.")
            else:
                session = get_session()
                try:
                    for rid in ids:
                        session.execute(
                            text("DELETE FROM edumanager.controle_materia WHERE id = :id"),
                            {"id": rid}
                        )
                    session.commit()
                    st.success(f"{len(ids)} registro(s) excluído(s).")
                    st.rerun()
                except Exception:
                    session.rollback()
                    LOGGER.exception("Erro ao excluir.")
                    st.error("Erro ao excluir registros.")
                finally:
                    session.close()

    # ================= Cadastro =================
    with tabs[1]:
        professores_df = listar_professores()
        professores = professores_df["nome"].tolist() if not professores_df.empty else []

        st.subheader("📥 Cadastrar Matéria")
        with st.form("form_cadastro"):
            data = {
                "turma": st.text_input("Turma"),
                "materia": st.text_input("Matéria"),
                "professor_titular": st.selectbox("Professor", professores),
                "trimestre": st.text_input("Trimestre"),
                "capitulo": st.text_input("Capítulo"),
                "bloco": st.text_input("Bloco"),
                "status": st.selectbox("Status", ["Não iniciado", "Em andamento", "Concluído"]),
                "data_limite_da_entrega": st.date_input("Data Limite"),
                "data_da_entrega": st.date_input("Data da Entrega"),
                "validacao_operacional": st.text_input("Validação Operacional"),
                "revisao_pedagogica": st.text_input("Revisão Pedagógica"),
                "diagramacao": st.text_input("Diagramação"),
                "data_de_aprovacao_final": st.date_input("Aprovação Final"),
                "obs": st.text_area("Observações")
            }

            if st.form_submit_button("Salvar"):
                insert_record(data)
                st.success("Registro cadastrado.")
                st.rerun()

        st.divider()
        st.subheader("📥 Importar Excel")

        uploaded = st.file_uploader("Arquivo .xlsx", type=["xlsx"])

        if uploaded:
            df_excel = pd.read_excel(uploaded)
            validar_colunas_excel(df_excel)
            df_excel = df_excel.astype(str).replace({"nan": None, "NaT": None})

            for _, row in df_excel.iterrows():
                insert_record(row.to_dict())

            st.success("Importação concluída.")
            st.rerun()

        st.divider()
        st.subheader("📥 Cadastrar Bloco")

        with st.form("form_bloco"):
            data = {
                "bloco": st.text_input("Bloco"),
                "data_limite_entrega": st.date_input("Data Limite")
            }

            if st.form_submit_button("Salvar"):
                insert_bloco(data)
                st.success("Bloco cadastrado.")
                sleep(10)
                st.rerun()

    # ================= Cadastro de Usuário =================

    with tabs[2]:
        st.subheader("👤 Cadastro de Usuario")

        new_email = st.text_input("user email")
        new_pwd = st.text_input("senha")
        new_status = st.selectbox("Status", ["super_admin", "admin", "reader"]) if st.session_state.status == "super_admin" else st.selectbox("Status", ["admin", "reader"])

        if st.button("Adicionar"):
            if new_email.strip():
                cadastrar_novo_usuario(new_email.strip(), new_pwd, new_status)
                st.success("Usuário cadastrado.")
                sleep(10)
                st.rerun()
            else:
                st.warning("Usuário não cadastro.")

    # ================= Sobre =================
    with tabs[3]:
        st.subheader("📖 Sobre")

        st.info(
            """
            **EduManager** v1.0
           
            Aplicação desenvolvida para otimizar o controle e gerenciamento 
            de fluxos de matérias escolares, prazos e aprovações pedagógicas.
        
            **Desenvolvido por:**
            Thiago Fernandes S. Almeida
        
            **Contato:**
            thiago.fernandes.s.almeida@gmail.com
            """
        )

else:
    tabs = st.tabs(["📊 Visualização", "📖 Sobre"])
    # ================= Visualização =================
    with tabs[0]:
        df = fetch_all()

        professores_df = listar_professores()
        professores = professores_df["nome"].tolist() if not professores_df.empty else []

        st.subheader("🔎 Filtros")

        col1, col2, col3, col4 = st.columns(4)

        filtro_turma = col1.selectbox(
            "Turma",
            ["Todos"] + sorted(df["turma"].dropna().unique().tolist())
        )

        filtro_prof = col2.selectbox(
            "Professor",
            ["Todos"] + sorted(df["professor_titular"].dropna().unique().tolist())
        )

        filtro_materia = col3.selectbox(
            "Matéria",
            ["Todos"] + sorted(df["materia"].dropna().unique().tolist())
        )

        filtro_capitulo = col4.selectbox(
            "Capítulo",
            ["Todos"] + sorted(df["capitulo"].dropna().unique().tolist())
        )

        filtro_dias = st.number_input(
            "Mostrar matérias com prazo em até (dias)",
            min_value=0,
            value=0,
            help="0 = mostrar todas"
        )

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

        if filtro_dias > 0:
            df_filtrado = df_filtrado[
                (df_filtrado["data_limite_da_entrega"].notna()) &
                ((pd.to_datetime(df_filtrado["data_limite_da_entrega"]) - hoje).dt.days <= filtro_dias)
                ]

        df_filtrado["alerta"] = df_filtrado["data_limite_da_entrega"].apply(
            lambda d: "⚠️ Prazo próximo"
            if pd.notna(d) and (pd.to_datetime(d) - hoje).days <= dias_alerta
            else ""
        )

        # df_filtrado["excluir"] = False

        st.subheader("✏️ Controle de Matérias")

        edited_df = st.data_editor(
            df_filtrado,
            use_container_width=True,
            num_rows="fixed",
            key="editor_materias",
            column_config={
                "excluir": st.column_config.CheckboxColumn("🗑️ Excluir"),
                "status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["Não iniciado", "Em andamento", "Concluído"]
                ),
                "professor_titular": st.column_config.TextColumn(
                    "Professor Titular"
                ),
                "data_limite_da_entrega": st.column_config.DateColumn(
                    "Data Limite",
                    format="DD/MM/YYYY"
                ),
                "data_da_entrega": st.column_config.DateColumn(
                    "Data da Entrega",
                    format="DD/MM/YYYY"
                ),
                "data_de_aprovacao_final": st.column_config.DateColumn(
                    "Aprovação Final",
                    format="DD/MM/YYYY"
                ),
                "alerta": st.column_config.TextColumn(
                    "⚠️ Alerta",
                    disabled=True
                )
            }, disabled=True
        )

        col_save, col_delete = st.columns(2)

        # ================= Sobre =================
    with tabs[1]:
        st.subheader("📖 Sobre")

        st.info(
            """
            **EduManager** v1.0

            Aplicação desenvolvida para otimizar o controle e gerenciamento 
            de fluxos de matérias escolares, prazos e aprovações pedagógicas.

            **Desenvolvido por:**
            Thiago Fernandes S. Almeida

            **Contato:**
            thiago.fernandes.s.almeida@gmail.com
            """
        )