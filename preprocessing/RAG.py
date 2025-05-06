import os

from langchain_core.documents import Document
from langchain_postgres import PGVector
from langchain_postgres.vectorstores import PGVector
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from dotenv import load_dotenv
import json
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

from langchain_core.prompts import PromptTemplate
import re
from langchain_community.query_constructors.pgvector import PGVectorTranslator
from pydantic import BaseModel

from langchain.chains.query_constructor.ir import (
    Comparator,
    Comparison,
    Operation,
    Operator,
    StructuredQuery,
)
from typing import Optional, List
import uuid



class Search(BaseModel):
    query: str
    id: Optional[List]
    location: Optional[str]


def add_docs(db, file, type):
    data = json.load(open(file))
    docs = []
    for key in data.keys():
        tags = data[key]["tags"]
        tags2 = ','.join(tags)
        meta_dict = {"tags": tags}
        meta_dict["type"] = type
        meta_dict["file"] = key
        meta_dict["min_year"] = data[key]["min_year"]
        meta_dict["max_year"] = data[key]["max_year"]
        docs.append(Document(page_content=data[key]["description"], metadata=meta_dict))
    db.add_documents(docs)

def add_doc(db, file, type):
    data = json.load(open("../data/EDGAR_2024_GHG/metadata_automatic.json"))[file]

    tags = data["tags"]
    meta_dict = {"tags": tags,
                 "type": type,
                 "file": file + ".csv",
                 "min_year": data["min_year"],
                 "max_year": data["max_year"],
                 "id":str(uuid.uuid4())}

    doc = Document(page_content=data["description"], metadata=meta_dict)
    db.add_documents([doc])

def retrieve(vector_store, llm):
    retriever = vector_store.as_retriever()
    prompt = PromptTemplate.from_template(json.load(open("../prompts.json"))['rag_prompt'])

    def format_docs(docs):
        return "\n\n".join(doc.page_content for doc in docs)

    rag_chain = (
            {"context": retriever | format_docs, "question": RunnablePassthrough()}
            | prompt
            | llm
            | StrOutputParser()
    )

    return rag_chain


def construct_comparisons(query: Search):
    comparisons = []
    if query.id is not None:
        comparisons.append(
            Comparison(
                comparator=Comparator.IN,
                attribute="id",
                value=query.id,
            )
        )
    if query.location is not None:
        comparisons.append(
            Comparison(
                comparator=Comparator.LIKE,
                attribute="location",
                value=query.location,
            )
        )
    return comparisons


if __name__ == "__main__":
    load_dotenv()

    connection = "postgresql+psycopg://langchain:langchain@localhost:6024/langchain"  # Uses psycopg3!
    collection_name = "my_docs"

    embeddings = AzureOpenAIEmbeddings(
        model="text-embedding-ada-002",
        azure_endpoint=os.getenv("TextEmb_EndPoint")
    )
    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
        model=os.environ["GPT_model_name"],
        deployment_name=os.environ["GPT_deployment"],
        temperature=0
    )

    vector_store = PGVector(
        embeddings=embeddings,
        collection_name=collection_name,
        connection=connection,
        use_jsonb=True,
    )

    # vector_store.add_documents(docs, ids=[doc.metadata["id"] for doc in docs])
    add_doc(vector_store,"GHG_by_sector_and_country","automatic")
    #add_docs(vector_store, "../data/Sales_Data/metadata_automatic.json","automatic")
    #vector_store.delete_collection()
    # filter = {"id": {"$in": [1, 5, 2, 9]}, "location": {"$in": ["pond", "market"]}}



    #print(vector_store.similarity_search("ducks", k=4, filter={'tags': {'$ilike': '%San Marino%'}, 'tags': {'$ilike': '%greenhouse gas emissions%'}, 'min_year': {'$lte': 1984}, 'max_year': {'$gte': 1984} }))
    # 'min_year': {'$gte': [1984]}, 'max_year': {'$lte': [1984]}
    #"tags": {"$ilike": '%San Marino%'}
    rag_chain = retrieve(vector_store, llm)
    res= vector_store.similarity_search("test", k = 30,)
    for i in res:
        print(i)
    #vector_store.delete(['d935e880-2bf6-45ff-b642-8e1521591331'])
