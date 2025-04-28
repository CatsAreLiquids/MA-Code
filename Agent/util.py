from langchain_core.messages import HumanMessage
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import tool
import os
from dotenv import load_dotenv
import json
from langchain.agents import initialize_agent, Tool, AgentType

from langchain.chains import RetrievalQA

from langchain_postgres.vectorstores import PGVector
# import RAG
from langchain_core.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from langchain.tools.retriever import create_retriever_tool
from langgraph.prebuilt import create_react_agent

from datetime import date
import requests
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
import re
import yaml

# TODO need to keep the titles of the data
def parseResult(str):
    urls = re.findall(r"\]\s*\((.*?)\)", str)
    titles = re.findall(r"\[(.*?)\]", str)

    if urls:
        return {'success': True, 'urls': urls, 'data_names': titles, 'message': 'I found your data'}
    else:
        return {'success': False, 'urls': None, 'data_names': None, 'message': str}


# Note works since we currently only use the products string needs adjustemnt if I extend API
def validateURL(urls):
    valid = []

    for url in urls:
        if "http://127.0.0.1:5000/" in url:

            parsedProduct = url.split("products/")[1].split("/")[0]

            if parsedProduct in productList:
                valid.append(url)

    if not valid:
        return ("No valid Urls found please try rephrasing your question", [])
    elif len(valid) != len(urls):
        return ("Some generated Urls are invalid please review the data and try rephrasing", valid)
    else:
        return ("I found the data you were looking for please wait while I load it ", valid)
