from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from dotenv import load_dotenv
import os
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import json
from pydantic import BaseModel, Field
from langchain_postgres.vectorstores import PGVector
from langchain.schema.runnable import RunnablePassthrough
from langchain.schema.output_parser import StrOutputParser
load_dotenv()

llm = AzureChatOpenAI(
    azure_endpoint=os.environ["GPT_EndPoint"],
    openai_api_version=os.environ["GPT_APIversion"],
    model=os.environ["GPT_model_name"],
    deployment_name=os.environ["GPT_deployment"],
    temperature=0
)
embeddings = AzureOpenAIEmbeddings(
    model="text-embedding-ada-002",
    azure_endpoint=os.getenv("TextEmb_EndPoint")
)
class RAG(BaseModel):
    reason: str = Field(description="Reason for choosing the specific file")
    file: str = Field(description="The file name")

def retrieve(state):
    query = state["messages"][0].content

    connection = "postgresql+psycopg://langchain:langchain@localhost:6024/langchain"  # Uses psycopg3!
    collection_name = "my_docs"

    vector_store = PGVector(
        embeddings=embeddings,
        collection_name=collection_name,
        connection=connection,
        use_jsonb=True,
    )
    retriever = vector_store.as_retriever()
    structured_llm = llm.with_structured_output(RAG)

    # RAG
    template = """Answer the following question based on this context:

    {context}

    Question: {question}
    """

    prompt = ChatPromptTemplate.from_template(template)

    rag_chain = (
            {"context": retriever, "question": RunnablePassthrough()}
            | prompt
            | structured_llm
            | StrOutputParser()
    )

    response = rag_chain.invoke({"question": query})
    state['file'] = response.file

    return {"messages": [ response['reason'] +" " + response['file']]}





def extractKeywords(state):
    """
    Function to extract keywords out of a user message
    :return: a comma seperated list of keywords: [keyword1,keyword2,keyword3,...]
    """
    messages = state["messages"][0].content
    query = state["messages"][0].content

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                json.load(open("../prompts.json"))['tools']["extractKeywords"]
            ),
            ("user", query),
        ]
    )
    response = llm.invoke(prompt.format(input=query))

    state['keywords'] = response.content
    return {"messages": [state['keywords']]}


def getCatalogItem(state):
    """
        Retrieves the data catalog entry for specific data product
        file: the file name of a specific data product
        :return: dict containing all information about the data product
    """
    file = state['file']
    try:
        with open("../dataCatalog/configs/" + file + ".yml") as stream:
            val = yaml.safe_load(stream)
    except FileNotFoundError:
        val = "could not find catalog Item stop execution"

    {"messages": [val]}


def getDataProduct(state):
    """
    Returns the full url including filters and base api for the user query
    base_api: a url for a specific data product
    filename: specific data to load
    func: any additional computations requested by the user set by the matchFunction tool
    filter_dict: specific filter based on the users input and created with the extractFilter tool

    :return: a url
    """

    filter_str = ""
    for key, value in state['filter_dict'].items():
        filter_str += f"&{key}={value}"

    func_str = ""
    if func is not None:
        for key, value in filter_dict.items():
            func_str += f"&{key}={value}"
    else:
        func_str = "None&rolling=False&period=None"

    url = f"{state['base_api']}?file={state['file']}&func={stae['func']}{filter_str}"

    return url


def extractFilter(state):
    """
    Creates a dict including filter atrributes and values for the getDataProduct tool
    query: the original user query
    :return: a dict with the corresponding filters such as dict={'Country1':'Austria','Country2':'Germany','min_year':1990,'max_year':1999}
    """
    query = state["messages"][0].content

    sys_prompt = """You are a helpful assistent used to extract filters out of a user message.
                    Only return uniquly identifiable filters such as year spans or countries, aviod unclear filter like "co2 emissions"
                    Return all filter in a dict 
                    Example 
                    user query : "I would like to have the generall CO2 emissions data of Germany and Austria for the years 2000 to 2010"
                    response: {'Country1':'Austria','Country2':'Germany',"min_year":2000,"max_year":2010}
                    user query : "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03"
                    response: {'Country1': 'Aruba', 'min_value': 0.02, 'max_value': 0.03}
                    """
    input_prompt = PromptTemplate.from_template("""
            User Query:{query}
            """)
    input_prompt = input_prompt.format(query=query)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    response = llm.invoke(messages)

    return {"messages": [response.content]}

