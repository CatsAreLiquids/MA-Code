from Backend import models
import numpy as np


def mmr(targets, contexts) -> float:
    mmr = 0
    for i in range(len(contexts)):
        if contexts[i] in targets:
            mmr += 1 / (i+1)
    return mmr/len(contexts)


def correctness_LLM(truth, response):
    sys_prompt = """Your Task is to decide wether a response allings with an expected output. Return True if the out put aligins thematically and semantically and False if not """
    input_prompt = PromptTemplate.from_template("""expected output: {truth}\n response :{response}""")
    input_prompt = input_prompt.format(truth=truth, response=response)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    llm = models.get_LLM()
    model_out = llm.invoke(messages)

    if model_out == "False":
        return False
    elif model_out == "True":
        return True

def precison(relevant,contexts):
    rel = 0
    for context in contexts:
        if context in relevant:
            rel += 1

    return rel / len(contexts)

def recall(relevant,contexts):
    rel = 0
    for context in contexts:
        if context in relevant:
            rel += 1

    return rel / len(relevant)

def F1(precision,recall):
    score = []
    for p,r in zip(precision,recall):
        try:
            score.append(  2* ((p * r)/ (p+ r)))
        except:
            score.append(0)
    return score