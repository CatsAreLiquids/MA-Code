from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from dotenv import load_dotenv
import json
import os
import pandas as pd
import re
from langchain_core.prompts import PromptTemplate
import glob
from pathlib import Path
import time
import ast


def manual_tagger(df):
    columns = df.columns.tolist()
    columns = [x for x in columns if not x.isdigit()]
    df_tmp = df[columns].copy()
    df_tmp = df_tmp.drop(columns=['EDGAR Country Code'])

    tags = [df[column].drop_duplicates().tolist() for column in df_tmp]
    return [tag for tag_types in tags for tag in tag_types]


def inferPeriod(frame):
    keys = frame.columns.tolist()
    keys = [int(key) for key in keys if re.match(r"[0-9]{4}", key)]
    return "{min} - {max}".format(min=min(keys), max=max(keys))


def fromatInput(frame, max_row):
    titles = ",".join(frame.columns.tolist())
    values = frame.iloc[0:max_row].values
    res = []
    for i in range(len(values)):
        row = [str(elem) for elem in values[i]]
        row = ",".join(row)
        res.append(row + "\n")

    return titles, res


def generateTags(titles, values, file_name, tagNumber):
    sys_prompt = PromptTemplate.from_template(json.load(open("../prompts.json"))['tagger_system_prompt'])
    sys_prompt = sys_prompt.format(count=tagNumber)
    input_prompt = PromptTemplate.from_template(json.load(open("../prompts.json"))['data_input_prompt'])
    input_prompt = input_prompt.format(titles=titles, values=values, file=file_name)

    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)


def generateText(titles, values, file_name):
    sys_prompt = json.load(open("../prompts.json"))['text_system_prompt']
    input_prompt = PromptTemplate.from_template(json.load(open("../prompts.json"))['data_input_prompt'])
    input_prompt = input_prompt.format(titles=titles, values=values, file=file_name)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)


def formatResult(res, files):
    tmp = zip(files, res.values())
    emptyDict = {}

    for file, result in tmp:
        text = result[0]
        tags = ast.literal_eval(result[1])
        file_name = Path(file).stem
        emptyDict[file_name] = {"description": text, "tags": tags[0], "min_year": tags[1], "max_year": tags[2]}

    return emptyDict


if __name__ == "__main__":

    load_dotenv()

    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
    )

    files = glob.glob("data/data_products/*.csv")
    res = {}

    for file in files:
        df = pd.read_csv(file, index_col=0)
        df = df.round(3)
        file_name = Path(file).stem
        titles, values = fromatInput(df, 2)

        text = generateText(titles, values, Path(file).stem).content
        tags = generateTags(titles, values, Path(file).stem, 7).content

        res[file_name] = [text, tags]

    res = formatResult(res, files)

    json.dump(res, open("../data/data_products/metadata_automatic.json", 'w'))

# TODO caching of input so we save on computing
