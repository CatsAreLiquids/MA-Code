from Backend.RAG import vector_db
from Backend import models


def productRetriever(query):
    prompt = """Your task is to help find the best fitting data product. You are provided with a user query .
                Provide the most likely fitting data products, always provide the data products name and why it would fit this query
        Question: {question} 
        Context: {context} 
        Answer:
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}'"
    config = {"filter": {"type": {"$eq": "product"}}}

    retrieval_chain = vector_db.getChain(prompt,config)
    return retrieval_chain.invoke(mod_query)