import json

import pandas as pd
import streamlit as st
import random
import time
import requests
import pandas as df
import io

import util
from uuid import uuid4
import ast



def parseData(urls,titles):
    content = []

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
            util.fallBack(df,urls)
            return None

    if len(df.shape) < 2:
        #TODO chcek if this could be nicer
        st.table(df.values)
    else:
        st.dataframe(df)

def formatHistory():
    pass

#TODO
    # get chat streaming ready
    # https://docs.streamlit.io/develop/tutorials/chat-and-llm-apps/build-conversational-apps

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

prompt = st.chat_input("Say something")

bot.write("Hello human")

if prompt:
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    url = f"{base_url}?message={prompt}"
    response = requests.get(url)
    content = json.loads(response.text)

    st.markdown(content)
    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    st.dataframe(df)
