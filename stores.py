from langchain_openai import AzureOpenAIEmbeddings,AzureChatOpenAI
from langchain_postgres.vectorstores import PGVector
from langchain_neo4j import GraphCypherQAChain, Neo4jGraph

import os
from dotenv import load_dotenv

load_dotenv()

embeddings = AzureOpenAIEmbeddings(
    model="text-embedding-ada-002",
    azure_endpoint=os.getenv("TextEmb_EndPoint")
)

default_chat = AzureChatOpenAI(
    azure_endpoint=os.environ["GPT_EndPoint"],
    openai_api_version=os.environ["GPT_APIversion"],
)


def load_vector_store():
    vector_store = PGVector(
        embeddings=embeddings,
        collection_name=os.getenv("PGCollection"),
        connection=os.getenv("PGConnection"),
        use_jsonb=True,
    )

    return vector_store


def load_graph_db():
    graph = Neo4jGraph(url=os.getenv("GraphEndpoint"),
                       username=os.getenv("GraphUser"),
                       password=os.getenv("GraphPassword")
                       )

    return graph
