import json

import pandas as pd
import streamlit as st
import random
import time
import requests
import pandas as df
import io

from uuid import uuid4
import ast



def fallBack(df,urls):
    st.dataframe(df)

    for url in urls:
        response = requests.get(url)
        content = json.loads(response.text)
        df = pd.read_json(io.StringIO(content['data']))
        st.dataframe(df)

def parseData(urls,titles):

    url = urls.pop()
    title = titles.pop()
    response = requests.get(url)
    content = json.loads(response.text)

    df = pd.read_json(io.StringIO(content['data']))
    df = df.add_suffix(str(title))

    tmp = zip(urls,titles)
    for url,title in tmp:
        response = requests.get(url)
        content = json.loads(response.text)
        df_to_merge = pd.read_json(io.StringIO(content['data']))
        df = df.add_suffix(str(title))
        try:
            df = pd.concat([df, df_to_merge], axis=1, join="inner")
        except:
            fallBack(df,urls)
            return None

    if len(df.shape) < 2:
        st.table(df.values)
    else:
        st.dataframe(df)

def check_message(message):
    if "yes" in message or "no" in message:
        return True
    else:
        return False

st.title("Demo Interface")
bot = st.chat_message("assistant")

base_url = "http://127.0.0.1:5100/chat"
conversation_id = str(uuid4())

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

prompt = st.chat_input("Please describe what kind of data you are looking for")

bot.write("How can I help ?")

@st.fragment
def display_df(res_df):
    st.dataframe(res_df)


if prompt:
    if "df" in st.session_state:
        display_df(st.session_state["df"])

    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})


    if not check_message(prompt):
        url = f"{base_url}?message={prompt}"
        response = requests.get(url)
        content = json.loads(response.text)

        try:
            res_df = pd.read_json(io.StringIO(content['data']))
        except ValueError:
            res_df = pd.Series(ast.literal_eval(content['data']))

        if not "plan" in st.session_state:
            st.session_state["plan"] = content['plan']
            st.session_state["df"] = res_df

        if "df" in st.session_state:
            display_df(res_df)

        with st.chat_message("assistant"):
            st.session_state.messages.append(
                {"role": "assistant", "content": "Does this data product fit your requirments ?"})
            st.write("Does this data product fit your requirments ?")

    else:

        if "yes" in prompt:
            with st.chat_message("assistant"):
                st.write("Great! I triggerd the conversion to Airflow, you should see the DAG in your interface soon")
                st.session_state.messages.append(
                    {"role": "assistant", "content": "Great! I triggerd the conversion to Airflow, you should see the DAG in your interface soon"})
            response = requests.get("http://127.0.0.1:5100/trigger_to_airflow", json={"plan": st.session_state["plan"]})
            content = json.loads(response.text)

            with st.chat_message("assistant"):
                if content["dag_id"] is not None:
                    st.write(f"The DAG id is {content['dag_id']}")
                    st.session_state.messages.append(
                        {"role": "assistant", "content": f"The DAG id is {content['dag_id']}"})
                else:
                    st.write("Something seemed to have gone wrong we need to try again")
                    st.session_state.messages.append(
                        {"role": "assistant", "content": "Something seemed to have gone wrong we need to try again"})
        else:
            with st.chat_message("assistant"):
                st.write("Should we try again? What data product are you looking for?")
                st.session_state.messages.append(
                    {"role": "assistant",
                     "content": "Should we try again? What data product are you looking for?"})

