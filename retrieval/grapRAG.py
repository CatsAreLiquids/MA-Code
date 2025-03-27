from langchain_neo4j import GraphCypherQAChain, Neo4jGraph

from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
import json
from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_core.documents import Document
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from dotenv import load_dotenv
import os

graph = Neo4jGraph(url="neo4j://localhost:7687", username="neo4j", password="password")

#print(graph.add_graph_documents())
load_dotenv()
llm = AzureChatOpenAI(
    azure_endpoint=os.environ["GPT_EndPoint"],
    openai_api_version=os.environ["GPT_APIversion"],
)

llm_transformer = LLMGraphTransformer(llm=llm)

def productToGraph():
    pass


def add_doc( file, type):
    data = json.load(open(file))
    docs = []
    for key in data.keys():
        tags = data[key]["tags"]
        tags2 = ','.join(tags)
        meta_dict = {"tags": tags}
        meta_dict["tags2"] = tags2
        meta_dict["type"] = type
        meta_dict["file"] = key
        meta_dict["min_year"] = data[key]["min_year"]
        meta_dict["max_year"] = data[key]["max_year"]
        docs.append(Document(page_content=data[key]["description"], metadata=meta_dict))

    graph_documents = llm_transformer.convert_to_graph_documents(docs)
    print(graph_documents)
    graph.add_graph_documents(
            graph_documents,
            baseEntityLabel=True,
            include_source=True
        )
    #graph.add_graph_documents(docs)

#add_doc("./data/data_products/metadata_automatic.json","automatic")

print(graph.schema)
chain = GraphCypherQAChain.from_llm(
    llm, graph=graph, verbose=True, allow_dangerous_requests=True
)
print(chain.invoke({"query": "Sum of emissions in Austria last decadse"}))