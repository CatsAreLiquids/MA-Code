
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