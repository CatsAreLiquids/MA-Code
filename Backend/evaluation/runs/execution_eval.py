from Backend.Agent import Agent,execute

import time
queries = ["Name the user that commented 'thank you user93!'"
"Write the contents of comments with a score of 17."
"What is the border color of card \"Ancestor's Chosen\"?" 
"What is the language of the card with the multiverse number 149934?"
"How many schools with an average score in Math greater than 400 in the SAT test are exclusively virtual?"
"List all the withdrawals in cash transactions that the client with the id 3356 makes."
"Tell the phone number of \"Carlo Jacobs\"."
"Which player is the tallest?"
"What is his number of the driver who finished 0:01:54 in the Q3 of qualifying race No.903?"
"What are the superpowers of heroes with ID 1?"]

functions =[]

def runEval():
    agent = Agent.init_agent()
    res = {"plan":[],"time":[],"agent_error":[],"correct":[],"execution_error":[],"parsing_error":[]}

    for i in range(len(queries)):
        start = time.time()
        try:
            agent_result = agent.invoke({'input': queries[i]})['output']
            res["agent_error"].append(False)
        except:
            agent_result = ""
            res["agent_error"].append(True)
        end = time.time()
        res["plan"].append(agent_result)
        res["time"].append(end - start)

        if agent_result != "":
            try:
                agent_result = ast.literal_eval(agent_result)
                res["parsing_error"].append(False)
            except:
                agent_result = None
                res["parsing_error"].append(True)

        if agent_result is not None:
            try:
                res = execute.execute_new(agent_result)
                res["execution_error"].append(False)
                #ground_truth = functions[i]
                #if ground_truth == res:
                #    res["correct"].append(True)
                #else:
                #    res["correct"].append(False)
            except:
                res["execution_error"].append(True)



if __name__ == "__main__":
    runEval()