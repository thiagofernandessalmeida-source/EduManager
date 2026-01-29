import streamlit as st
from database import login_user

def render_login():
    st.set_page_config(page_title="Login Edumanager", layout="centered")

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        with st.container(border=True):
            st.subheader("🔐 Login")

            user = st.text_input("Email")
            password = st.text_input("Senha", type="password")

            submit = st.button("Entrar", use_container_width=True)

        if submit:
            status = login_user(user, password)

            if status:
                st.session_state.logged = True
                st.session_state.user = user
                st.session_state.status = status
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos")
