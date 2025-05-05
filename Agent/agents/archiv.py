@tool
def chooseProduct(query,retrievedProducts):
    """
    Decides the best possible data product based on a user query and the retrieved products
    :param query: text about the queries
    :param retrievedProducts: data product decription and data product name
    :return: {{"name":"name","url":"http://127.0.0.1:5000/exampleUrl"}}
    """
    sys_prompt = """ Your task is to help identify the correct url and data product for a user based on their query
                        Only provide one url at a time together withe the name of the data product.
                        The output should be a valid json formatted as follwos:
                        {{"name":"name","url":"http://127.0.0.1:5000/exampleUrl"}}
        """
    input_prompt = PromptTemplate.from_template("""
            User Query:{query}
            possible Products:{retrievedProducts}
            """)
    input_prompt = input_prompt.format(query=query, catalog_dict=retrievedProducts)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)
@tool
def formatOutput():
    """

    :return:
    """
    pass
