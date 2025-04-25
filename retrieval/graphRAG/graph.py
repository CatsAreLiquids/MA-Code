from typing import Annotated, Sequence
from typing_extensions import TypedDict

from langchain_core.messages import BaseMessage

from langgraph.graph.message import add_messages
from langgraph.graph import END, StateGraph, START
from langgraph.prebuilt import ToolNode
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
import os
from dotenv import load_dotenv
import pprint

import tools as agent_tools

from langchain_postgres.vectorstores import PGVector
from langchain.tools.retriever import create_retriever_tool

embeddings = AzureOpenAIEmbeddings(
    model="text-embedding-ada-002",
    azure_endpoint=os.getenv("TextEmb_EndPoint")
)

load_dotenv()
default_chat = AzureChatOpenAI(
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

class AgentState(TypedDict):
    # The add_messages function defines how an update should be processed
    # Default is to replace. add_messages says "append"
    messages: Annotated[Sequence[BaseMessage], add_messages]


def agent(state):
    """
    Invokes the agent model to generate a response based on the current state. Given
    the question, it will decide to retrieve using the retriever tool, or simply end.

    Args:
        state (messages): The current state

    Returns:
        dict: The updated state with the agent response appended to messages
    """
    print("---CALL AGENT---")
    messages = state["messages"]
    model = default_chat
    tools = agent_tools.getTools()
    model = model.bind_tools(tools.append(retriever_tool))
    response = model.invoke(messages)
    # We return a list, because this will get added to the existing list
    return {"messages": [response]}



retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files about the greenhouse gas emissions of diffrent countires.",
    )

workflow = StateGraph(AgentState)
workflow.add_node("agent", agent)

tool_list = agent_tools.getTools()
#retrieve = ToolNode([agent_tools.retrieverTool])
#workflow.add_node("retrieve", retrieve)

tools_ = ToolNode([agent_tools.extractFilter,agent_tools.matchFunction,
                   agent_tools.getDataProduct,agent_tools.getCatalogItem,
                   agent_tools.extractKeywords,retriever_tool])
tools_ = ToolNode(tool_list)
workflow.add_node("tools", tools_)

workflow.add_edge(START, "agent")
#workflow.add_edge("agent", "retrieve")
workflow.add_edge("agent", "tools")
workflow.add_edge("tools","agent")
#workflow.add_edge("retrieve","agent")
workflow.add_edge("agent",END)
workflow.add_edge("tools",END)
graph = workflow.compile()

try:
    graph.get_graph(xray=True).draw_mermaid_png(output_file_path="graph.png")
except Exception:
    # Request Timeout image will be generate on the next try
    pass

inputs = {
    "messages": [
        ("user", "What are the co2 emissions of germany"),
    ]
}
for output in graph.stream(inputs):
    for key, value in output.items():
        pprint.pprint(f"Output from node '{key}':")
        pprint.pprint("---")
        pprint.pprint(value, indent=2, width=80, depth=None)
    pprint.pprint("\n---\n")

