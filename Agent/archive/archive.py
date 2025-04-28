@tool
def createFilter(query,keywords):
    """
        creates a filter for a retriever based on the provided user query and keyweords
        :return: a list of filters [filter={'tags': {'$ilike': '%Germany%'},...]

    """
    sys_prompt = """You are an AI language model assistant. Your task is to generate a string filter  based on a user query and a list of given keywords.
                With the example : 
                user query : "I would like to have the generall CO2 emissions data of Germany for the years 2000 to 2010"
                keywords: ["CO2 emissions", "Germany", 2000, 2010]
                The result should look like:
                [filter={'tags': {'$ilike': '%Germany%'},{'tags': {'$ilike': '%CO2 emissions%'}], 'min_year': {'$lte': 2000},'max_year': {'$gte': 2010} }}
                Only use the following filters:
                Text (case-insensitive like):'$ilike'
                Logical (or):'$or',
                Logical (and):'$and',
                Greater than (>):'$gt',
                Greater than or equal (>=):'$gte',
                Less than (<):'$lt',
                Less than or equal (<=):'$lte'
                """
    input_prompt= PromptTemplate.from_template("""
    User Query:{query}
                keywords: {keywords}
    """)
    input_prompt = input_prompt.format(query=query, keywords=keywords)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)

@tool
def extendFilter(keywords):
    """
    provides additional keywords based on the input list,
    :return: a comma seperated list of keywords: [keyword1,keyword2,keyword3,...]
    """

    # remove all numerical keywords first
    keywords= [keyword for keyword in keywords if not isinstance(keyword, int)]
    print(keywords)
    # Note based on langchain multiquery prompt
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
                    You are an AI language model assistant. Your task is to generate alternative keywords for each keyword in the given list.
                    Generating these keywords will help with filtering data annotated by multiple sources.
                    The goal is it to provide extended filter keywords after we were unsuccsesfull with the first keywords.
                    Therefore we need to boarden the scope of the keywords, in the sense that "Europe" is an alternative keyword for "Ireland"
                    Provide the alternative keywords as a comma seperated list: ["keyword1","keyword2","keyword3"].
                """
            ),
            ("user", keywords),
        ]
    )

    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
    )

    response = llm.invoke(prompt.format(input=keywords))
    return response.content
