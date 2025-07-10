import pandas as pd
from Backend import models
from Backend.evaluation import metrics
from langchain_core.prompts import PromptTemplate
from Backend.RAG import eval_retriever
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

def generateAnswer(context,query):
    sys_prompt = """Your task is to help find the best fitting data entry. You are provided with a user query .
                    Provide the most likely fitting data entry, always provide the data entrys name and why it would fit this query
            Context: {context} 
            Answer:
        """
    sys_prompt = sys_prompt.format(context=context)

    input_prompt = PromptTemplate.from_template("""I am looking for a data entry that can solve the following problem for me :{query} """)
    input_prompt = input_prompt.format(query=query)

    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    llm = models.get_LLM()
    response = llm.invoke(messages)

    return response.content

def createGroundTruth(file):
    df = pd.read_csv(file)
    truth = []
    df["ground_truth"] = ""
    for index, row in df.iterrows():
        truth.append(generateAnswer(row["correct_context"],row["query"]))
    df["ground_truth"] = truth
    df.to_csv(file,index=False)

def scoreFunction(file):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval})
    df = df.dropna()
    correctness_LLM = []
    simple_match = []

    for index, row in tqdm(df.iterrows()):
        correctness_LLM.append(metrics.correctness_LLM(row["function"],row["response"]))
        simple_match.append(metrics.simple_match(row["response"], [row["function"]]))




    df["correctness_LLM"] = correctness_LLM
    df["simple_match"] = simple_match
    df.to_csv(file, index=False)

def score_function_no_reorder(file):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval})
    df = df.dropna()
    correctness_LLM = []
    simple_match = []
    responses = []

    for index, row in tqdm(df.iterrows()):
        response = eval_retriever.functionRetriever_eval_noreorder(row["step"],row["retrieved_docs"])
        responses.append(response)
        correctness_LLM.append(metrics.correctness_LLM(row["function"], response))
        simple_match.append(metrics.simple_match(response, [row["function"]]))


    df["correctness_LLM"] = correctness_LLM
    df["simple_match"] = simple_match
    df["response"] = responses
    df.to_csv(file, index=False)


def score_step(file):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'products': pd.eval,"correct_context":pd.eval})

    correctness_LLM = []
    simple_match = []


    for index, row in tqdm(df.iterrows()):
        correctness_LLM.append(metrics.correctness_LLM([row["product"]],row["response"]))
        simple_match.append(metrics.simple_match(row["response"], [row["product"]]))

    df["correctness_LLM"] = correctness_LLM
    df["simple_match"] = simple_match
    df.to_csv(file, index=False)

def score(file):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'products': pd.eval,"correct_context":pd.eval})

    correctness_LLM = []
    simple_match = []
    ragasFaithfullnes = []
    ragasResponseGroundedness = []
    ragasSemanticSimilarity = []


    for index, row in tqdm(df.iterrows()):
        correctness_LLM.append(metrics.correctness_LLM(row["products"],row["response"]))
        simple_match.append(metrics.simple_match(row["response"], row["products"]))
        #ragasFaithfullnes.append(metrics.ragasFaithfullnes(row["query"], row["response"],row["retrieved_docs"]))
        #ragasResponseGroundedness.append(metrics.ragasResponseGroundedness( row["response"],row["retrieved_docs"]))
        #ragasSemanticSimilarity.append(metrics.ragasSemanticSimilarity( row["response"],row["ground_truth"]))

    df["correctness_LLM"] = correctness_LLM
    df["simple_match"] = simple_match
    #df["ragasFaithfullnes"] = ragasFaithfullnes
    #df["ragasResponseGroundedness"] = ragasResponseGroundedness
    #df["ragasSemanticSimilarity"] = ragasSemanticSimilarity
    df.to_csv(file, index=False)


if __name__ == "__main__":
    #createGroundTruth(file)
    file = "runs/products_retrieval_steps_hybrid_05_2025-07-07-13-55.csv"
    #file = "bird_minidev_questions_functions_simple_eval_function_2025-06-17-17-36.csv"
    #score_function_no_reorder(file)
    #scoreFunction(file)
    score_step(file)
    df = pd.read_csv(file)
    #print(df.columns)
    #print(df["correctness_LLM"].value_counts())
    print(df[["correctness_LLM", "simple_match"]].describe())
    #print(df[["simple_match"]].describe())
    #print(df[[ "ragasFaithfullnes","ragasResponseGroundedness",
    #          "ragasSemanticSimilarity"]].describe())