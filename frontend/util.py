
def fallBack(df,urls):
    st.dataframe(df)

    for url in urls:
        response = requests.get(url)
        content = json.loads(response.text)
        df = pd.read_json(io.StringIO(content['data']))
        st.dataframe(df)

#TODO title parsing
def parseData(urls,titles):
    content = []

    url = urls.pop()
    title = titles.pop()
    response = requests.get(url)
    content = json.loads(response.text)
    df = pd.read_json(io.StringIO(content['data']))
    df = df.add_suffix(str(title))

    tmp = zip(urls,titles)
    for url,title in tmp:
        response = requests.get(url)
        content = json.loads(response.text)
        df_to_merge = pd.read_json(io.StringIO(content['data']))
        df = df.add_suffix(str(title))
        try:
            df = pd.concat([df, df_to_merge], axis=1, join="inner")
        except:
            fallBack(df,urls)
            return None

    st.dataframe(df)

def confirmResult(query: str,result):
    """
    Breaks down a user query into multiple steps
    :param query user query asking for an data product
    :return: list of steps
    """
    if isinstance(result,pd.Series) or isinstance(result,pd.DataFrame):
        data = result[:5]
    else:
        data = result

    sys_prompt = """ Your task is to decide wether the provided data answers a query. 
                    For this you will receive at max the top 5 rows of the data, try to extrapolate if the data answers the query
                    Do only return True if the data seems to answer the query and return False, if the query is not answerd give a one sentence sumarry of what seems to be wrong
        """
    input_prompt = PromptTemplate.from_template("""
                User Query:{query}
                Data:{data}
                """)
    input_prompt = input_prompt.format(query=query,data= data)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages, config={"callbacks": [callback]})