# import RAG

from Agent.agents import processing_agent, retrieval_agent

# user = "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03 "
# user= "Emissions Data for Austrias Argiculture and building Sector for the 1990er"
#user = "Sweden, Norveigen and Finnlands per capita Co2 emissions "
user = "All females customers who paid with Credit Card and are at least 38 years old"
#user= "Germanies emisson for the 2000s"

ragent = retrieval_agent.init_agent()
pagent = processing_agent.init_agent()

#input = {"input": user}
#agent_result = ragent.invoke(input)
#print(agent_result)

input = {"input": user}
agent_result = pagent.invoke(input)
print(agent_result)



