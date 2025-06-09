import os

from dotenv import load_dotenv
from langchain_postgres.vectorstores import PGVector
from langchain.agents import AgentExecutor, create_tool_calling_agent, tool
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.tools.retriever import create_retriever_tool
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_core.callbacks import UsageMetadataCallbackHandler


def get_LLM():
    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
        model=os.environ["GPT_model_name"],
        deployment_name=os.environ["GPT_deployment"],
        temperature=0,

    )
    return llm

def get_structured_LLM():
    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
        model=os.environ["GPT_model_name"],
        deployment_name=os.environ["GPT_deployment"],
        temperature=0,

    ).with_structured_output(method="json_mode")
    return llm

def get_LLM_with_callbacks():
    callback = UsageMetadataCallbackHandler()
    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
        model=os.environ["GPT_model_name"],
        deployment_name=os.environ["GPT_deployment"],
        temperature=0,
        callbacks=[callback]
    )

    return llm, callback


def get_embeddings():
    return AzureOpenAIEmbeddings(
        model="text-embedding-ada-002",
        azure_endpoint=os.getenv("TextEmb_EndPoint")
    )


def getVectorStore(PGCollection=None):
    if PGCollection is None:
        PGCollection = os.environ["PGCollection"]

    return PGVector(
        embeddings=get_embeddings(),
        collection_name=PGCollection,
        connection=os.environ["PGConnection"],
        use_jsonb=True,
    )
