@tool(parse_docstring=True)
def productRetrieverBreakdown(query):
    """
    Returns the describtion of potetial dataproducts to answer the query. Only usefull when trying to retrieve data
    Args:
        query: a natural languge user query
    Returns:
        a text descrbing potential functions
    """
    prompt = """Your task is to help identifiy all necerssary data products for to answer a user query
                Provide up to three fitting data products and why the would fit this query. Always include the data products name and the reason why it would fit
        Question: {question} 
        Context: {context} 
        Answer:
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}'"
    config = {"filter": {"type": {"$eq": "product"}}}

    retrieval_chain = vector_db.getChain(prompt,config)
    return retrieval_chain.invoke(mod_query)