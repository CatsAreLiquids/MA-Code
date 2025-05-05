from Agent.agents import processing_agent, retrieval_agent
import util
import json
import ast
import os
from transformations import aggregation
from transformations import filter

from langchain_postgres.vectorstores import PGVector

from langchain_core.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.agents import tool
import yaml
from typing import List, Optional

from Agent.tools import init_tools

from langchain_core.tools import StructuredTool

from langchain.tools.retriever import create_retriever_tool

from langchain_core.callbacks import UsageMetadataCallbackHandler
import langchain_core

print(langchain_core.__version__)

load_dotenv()
callback = UsageMetadataCallbackHandler()

embeddings = AzureOpenAIEmbeddings(
    model="text-embedding-ada-002",
    azure_endpoint=os.getenv("TextEmb_EndPoint")
)
llm = AzureChatOpenAI(
    azure_endpoint=os.environ["GPT_EndPoint"],
    openai_api_version=os.environ["GPT_APIversion"],
    model=os.environ["GPT_model_name"],
    deployment_name=os.environ["GPT_deployment"],
    temperature=0,

)
connection = "postgresql+psycopg://langchain:langchain@localhost:6024/langchain"  # Uses psycopg3!
collection_name = "my_docs"

vector_store = PGVector(
    embeddings=embeddings,
    collection_name=collection_name,
    connection=connection,
    use_jsonb=True,
)


