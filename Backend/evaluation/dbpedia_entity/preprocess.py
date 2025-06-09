import re
import pandas as pd
from Backend.models import getVectorStore
import json
from dotenv import load_dotenv
from langchain_core.documents import Document
import uuid

load_dotenv()


def createSemanticQrels():
    df = pd.read_csv('unprocessed/qrels-v2.txt', sep='\t', header=None)
    df = df[df[0].str.contains(r"SemSearch_LS-[0-9]{1,3}")]
    df = df.drop(columns=[1])
    df = df[df[3] > 0]
    df = df.rename(columns={0: "query_id", 2: "entity", 3: "rel_score"})
    df = df.reindex()
    df.to_csv("semantic_qrels.csv", index=False)


def createSemanticQueries():
    df = pd.read_csv('unprocessed/queries-v2.txt', sep='\t', header=None)
    df = df[df[0].str.contains(r"SemSearch_LS-[0-9]{1,3}")]
    df = df.rename(columns={0: "query_id", 1: "query"})
    df["type"] = "db_pedia"
    df = df.reindex()
    df.to_csv("semantic_queries.csv", index=False)


def get_names(id):
    df = pd.read_csv("semantic_qrels.csv")
    df = df[df["query_id"] == id]
    return df["entity"].unique().tolist()


def get_correct_context(names):
    context = []
    with open('corpus.jsonl') as f:
        for line in f:
            obj = json.loads(line)
            if obj["_id"] in names:
                context.append(obj["text"])
    return context


def prep_eval():
    df = pd.read_csv("semantic_queries.csv")
    names = []
    contexts =[]
    for index, row in df.iterrows():
        name = get_names(row["query_id"])
        context = get_correct_context(name)

        names.append(names)
        contexts.append(context)
    df["products"] = names
    df["correct context"] = contexts

    df.to_csv("semantic_queries.csv", index=False)

def addtoDB():
    df = pd.read_csv('semantic_qrels.csv', )
    entities = df["entity"].unique().tolist()

    vec_store = getVectorStore("DB_PEDIA")
    i = 0
    with open('corpus.jsonl') as f:
        for line in f:
            print(i)
            obj = json.loads(line)
            if obj["_id"] in entities:
                obj = json.loads(line)
                doc = Document(page_content=obj["text"],
                               metadata={"entitiy_id": obj["_id"], 'title': obj["title"], "id": str(uuid.uuid4())})
                vec_store.add_documents([doc])
            i += 1


if __name__ == "__main__":
    # addtoDB()
    # createSemanticQrels()
    #createSemanticQueries()
    prep_eval()
