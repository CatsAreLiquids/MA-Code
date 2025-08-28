import os

from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_postgres.vectorstores import PGVector


def get_LLM():
    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
        model=os.environ["GPT-4o_model_name"],
        deployment_name=os.environ["GPT-4o_deployment"],
        temperature=0,
    )
    return llm

def get_structured_LLM():
    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
        model=os.environ["GPT-4o_model_name"],
        deployment_name=os.environ["GPT-4o_deployment"],
        temperature=0,

    ).with_structured_output(method="json_mode")
    return llm


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
