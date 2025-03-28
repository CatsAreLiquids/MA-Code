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

load_dotenv()
default_chat = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
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
    model = model.bind_tools(agent_tools.getTools())
    response = model.invoke(messages)
    # We return a list, because this will get added to the existing list
    return {"messages": [response]}



workflow = StateGraph(AgentState)
workflow.add_node("agent", agent)

tool_list = agent_tools.getTools()
#retrieve = ToolNode([agent_tools.retrieverTool])
#workflow.add_node("retrieve", retrieve)

tools_ = ToolNode([agent_tools.extendFilter,agent_tools.extractKeywords,agent_tools.createFilter,agent_tools.currentDate,agent_tools.getDataProduct])
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

