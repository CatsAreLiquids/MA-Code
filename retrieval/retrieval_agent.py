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

@tool
def getDataProduct(base_api,filename, func, filter_dict):
    """
    Returns the full url including filters and base api for the user query
    base_api: a url for a specific data product
    filename: specific data to load
    func: any additional computations requested by the user set by the matchFunction tool
    filter_dict: specific filter based on the users input and created with the extractFilter tool

    :return: a url
    """

    filter_str = ""
    for key, value in filter_dict.items():
        filter_str += f"&{key}={value}"

    func_str = ""
    if func is not None:
        for key, value in filter_dict.items():
            func_str += f"&{key}={value}"
    else:
        func_str = "None&rolling=False&period=None"

    # TODO need o fix this somehow the dict doesnt want to process
    url = f"{base_api}?file={filename}&func={func}{filter_str}"

    return url