import streamlit as st

pg = st.navigation([st.Page("chat.py"), st.Page("catalog.py")])
pg.run()