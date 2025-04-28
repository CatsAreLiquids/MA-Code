from langchain.tools.retriever import create_retriever_tool
from dotenv import load_dotenv
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_postgres.vectorstores import PGVector
from typing import Annotated, Sequence
from typing_extensions import TypedDict
import os
import pprint

from langchain_core.messages import BaseMessage

from langgraph.graph.message import add_messages
from typing import Annotated, Literal, Sequence
from typing_extensions import TypedDict

from langchain import hub
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI

from pydantic import BaseModel, Field
from typing import List, Dict
from langgraph.graph import END, StateGraph, START
from langgraph.prebuilt import ToolNode

from langgraph.prebuilt import tools_condition
from node import extractKeywords,getCatalogItem,getDataProduct,extractFilter, retrieve


class AgentState(TypedDict):
    # The add_messages function defines how an update should be processed
    # Default is to replace. add_messages says "append"
    messages: Annotated[Sequence[BaseMessage], add_messages]
    file: str
    keywords: Annotated[Sequence[str], add_messages]
    base_api: str
    filter_dict: Dict


load_dotenv()

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
connection = "postgresql+psycopg://langchain:langchain@localhost:6024/langchain"  # Uses psycopg3!
collection_name = "my_docs"

vector_store = PGVector(
    embeddings=embeddings,
    collection_name=collection_name,
    connection=connection,
    use_jsonb=True,
)

retriever = vector_store.as_retriever()

retriever_tool = create_retriever_tool(
    retriever,
    "retrieve_blog_posts",
    "Searches and returns Data files about the greenhouse gas emissions of diffrent countires.",
)

tools = [retriever_tool]


### Nodes


def agent(state):
    """
    You are a helpful assistant providing users with their requested data via providing opnly the link the data can be found at.

    Args:
        state (messages): The current state

    Returns:
        dict: The updated state with the agent response appended to messages
    """
    print("---CALL AGENT---")
    messages = state["messages"]
    # model = ChatOpenAI(temperature=0, streaming=True, model="gpt-4-turbo")
    #model = llm.bind_tools(tools)
    response = llm.invoke(messages)
    # We return a list, because this will get added to the existing list
    return {"messages": [response]}


# Define a new graph
workflow = StateGraph(AgentState)

# Define the nodes we will cycle between
workflow.add_node("agent", agent)  # agent
workflow.add_node("getProduct", getDataProduct)
workflow.add_node("getCatalog", getCatalogItem)
#retrieve = ToolNode([retriever_tool])
workflow.add_node("retrieve", retrieve)
#workflow.add_node("extractKeywords", extractKeywords)


# Call agent node to decide to retrieve or not
workflow.add_edge(START, "agent")
workflow.add_edge("agent","retrieve")

workflow.add_edge("retrieve","getCatalog")
workflow.add_edge("getProduct",END)
# Compile
graph = workflow.compile()

try:
    graph.get_graph(xray=True).draw_mermaid_png(output_file_path="./graph/graph.png")
except Exception:
    # Request Timeout image will be generate on the next try
    pass

inputs = {
    "messages": [
        ("user", "Sum of Germanys C02 emissions with a 5 year period"),
    ]
}
for output in graph.stream(inputs):
    for key, value in output.items():
        pprint.pprint(f"Output from node '{key}':")
        pprint.pprint("---")
        pprint.pprint(value, indent=2, width=80, depth=None)
    pprint.pprint("\n---\n")
